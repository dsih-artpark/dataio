"""Replace keeps who can see a dataset (DIO-53).

Runs the real Replace step against PostgreSQL (see postgres_schema.py), with
only the S3 file steps stubbed, and checks that user permissions, group
permissions and resource-group memberships are untouched - both when the
replace succeeds and when it fails and is rolled back.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from fastapi import HTTPException  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from dataio.api.auth import permissions as permissions_module  # noqa: E402
from dataio.api.database import functions as db_functions  # noqa: E402
from dataio.api.database.enums import (  # noqa: E402
    AccessLevel,
    ResourceType,
    SpatialResolution,
    TemporalResolution,
    VersionType,
)
from dataio.api.database.models import (  # noqa: E402
    Collection,
    DataOwner,
    Dataset,
    DatasetChangeRecord,
    DatasetRawDataset,
    RawDataset,
    ResourceGroup,
    ResourceGroupMember,
    User,
    UserGroup,
    UserPermission,
)
from dataio.api.services import dataset_change_service as change_module  # noqa: E402
from dataio.api.services.dataset_changes import DatasetChangeInput  # noqa: E402
from dataio.api.services.web_admin_service import WebAdminService  # noqa: E402
from dataio.api.tests.postgres_schema import fresh_schema_engine, requires_postgres  # noqa: E402

DS_ID = "CS0007DS0999"
RDS_ID = "CSRDS0099"
ADMIN = "admin@example.org"
DIRECT = "direct@example.org"  # DOWNLOAD on the dataset itself
MEMBER = "member@example.org"  # in a user group that has VIEW
GROUP = "group@example.org"
VIA_RG = "via-rg@example.org"  # DOWNLOAD on a resource group holding the dataset
RG_ID = "RG-LIVESTOCK"
CHANGELOG = DatasetChangeInput(type="data", breaking=False, summary="New files.", reason="Test.")


@pytest.fixture(scope="module")
def engine():
    if not os.getenv("DATAIO_TEST_DATABASE_URL"):
        pytest.skip("DATAIO_TEST_DATABASE_URL not set")
    engine = fresh_schema_engine()
    yield engine
    engine.dispose()


@pytest.fixture()
def factory(engine, monkeypatch):
    """Fresh rows for each test, and every DB layer Replace uses bound to them."""
    factory = sessionmaker(bind=engine)
    session = factory()
    for model in (
        DatasetChangeRecord, UserPermission, ResourceGroupMember, ResourceGroup,
        UserGroup, DatasetRawDataset, Dataset, RawDataset, DataOwner, Collection, User,
    ):
        session.query(model).delete()
    session.commit()

    collection = Collection(
        collection_id="CS0007", collection_name="Livestock", category_id="CS", category_name="Census"
    )
    owner = DataOwner(name="Animal Husbandry Dept")
    raw = RawDataset(rds_id=RDS_ID, title="Old raw title", source="Old source")
    session.add_all([collection, owner, raw])
    session.flush()
    dataset = Dataset(
        ds_id=DS_ID,
        title="Old title",
        collection_id=collection.id,
        data_owner_id=owner.id,
        spatial_resolution=SpatialResolution.DISTRICT,
        temporal_resolution=TemporalResolution.YEAR,
        access_level=AccessLevel.NONE,  # restricted: only explicit grants can see it
    )
    session.add(dataset)
    session.flush()
    session.add(DatasetRawDataset(dataset_id=dataset.id, raw_dataset_id=raw.id))
    session.flush()
    for email, is_group, is_admin in (
        (ADMIN, False, True), (DIRECT, False, False), (MEMBER, False, False),
        (GROUP, True, False), (VIA_RG, False, False),
    ):
        session.add(User(email=email, is_group=is_group, is_admin=is_admin, email_verified=True))
        session.flush()
    session.add(UserGroup(group_email=GROUP, user_email=MEMBER))
    session.add(ResourceGroup(resource_group_id=RG_ID, group_name="Livestock"))
    session.flush()
    session.add(
        ResourceGroupMember(resource_group_id=RG_ID, resource_id=DS_ID, resource_type=ResourceType.DATASET)
    )
    # One row per INSERT: a batched insert casts to SQLAlchemy's default
    # enum type names, which differ from schema.sql's.
    for email, rtype, rid, level in (
        (DIRECT, ResourceType.DATASET, DS_ID, AccessLevel.DOWNLOAD),
        (GROUP, ResourceType.DATASET, DS_ID, AccessLevel.VIEW),
        (VIA_RG, ResourceType.GROUP, RG_ID, AccessLevel.DOWNLOAD),
    ):
        session.add(UserPermission(user_email=email, resource_type=rtype, resource_id=rid, permission=level))
        session.flush()
    session.commit()
    session.close()

    monkeypatch.setattr(db_functions, "Session", factory)
    monkeypatch.setattr(permissions_module, "Session", factory)
    monkeypatch.setattr(change_module, "DBSession", factory)
    return factory


def _access_rows(factory) -> dict:
    session = factory()
    try:
        return {
            "permissions": sorted(
                (p.user_email, p.resource_type.value, p.resource_id, p.permission.value)
                for p in session.query(UserPermission).all()
            ),
            "group_members": sorted(
                (m.resource_group_id, m.resource_type.value, m.resource_id)
                for m in session.query(ResourceGroupMember).all()
            ),
            "user_groups": sorted((g.group_email, g.user_email) for g in session.query(UserGroup).all()),
            "access_level": session.query(Dataset).filter(Dataset.ds_id == DS_ID).one().access_level,
        }
    finally:
        session.close()


def _visible_level(email: str):
    """What the dataset list shows this user for the dataset, or None if hidden."""
    user = SimpleNamespace(email=email, is_group=False, is_admin=False)
    perms = permissions_module.determine_user_permissions(user)
    for dataset in db_functions.get_datasets(limit=1000, user_permissions=perms):
        if dataset.ds_id == DS_ID:
            return dataset.access_level
    return None


def _service(monkeypatch, *, fail_manifest=False) -> WebAdminService:
    """The real service; only the S3 file steps are stubbed."""
    service = WebAdminService()
    admin = service.admin_dataset_service
    filestore = admin.filestore_service
    monkeypatch.setattr(
        filestore, "archive_dataset_version",
        lambda ds_id, bucket_type, stamp: (f"archive/filestore/STANDARDISED/{ds_id}/{stamp}", ["old.csv"]),
    )
    monkeypatch.setattr(filestore, "delete_dataset_version", lambda *a, **kw: None)
    monkeypatch.setattr(filestore, "restore_dataset_version", lambda *a, **kw: None)
    monkeypatch.setattr(admin, "create_dataset_table", lambda *a, **kw: None)
    # Reads S3 and writes through its own session; record the call instead.
    service.cache_refreshed = []
    monkeypatch.setattr(admin, "refresh_dataset_documentation_cache", service.cache_refreshed.append)

    def upsert_manifest(*a, **kw):
        if fail_manifest:
            raise RuntimeError("manifest upload failed")

    monkeypatch.setattr(admin, "upsert_dataset_manifest", upsert_manifest)
    return service


PREVIEW = {
    "raw_dataset": {"rds_id": RDS_ID, "title": "New raw title", "source": "New source"},
    "dataset": {
        "ds_id": DS_ID,
        "title": "New title",
        "data_owner_name": "Animal Husbandry Dept",
        "description": "Reworked livestock data.",
        "spatial_resolution": "DISTRICT",
        "temporal_resolution": "YEAR",
        "tags": [],
        # The package says DOWNLOAD; Replace must ignore it and keep NONE.
        "access_level": "DOWNLOAD",
    },
    "tables": [],
    "manifest_yaml": f"datasetID: {DS_ID}\n",
}


def _replace(service):
    admin_user = SimpleNamespace(email=ADMIN, is_admin=True, is_group=False)
    return service._replace_dataset_package_locked(
        admin_user, PREVIEW, [], VersionType.STANDARDISED, CHANGELOG
    )


@requires_postgres
def test_replace_keeps_permissions_groups_and_access_level(factory, monkeypatch):
    before = _access_rows(factory)
    visible_before = {email: _visible_level(email) for email in (DIRECT, MEMBER, VIA_RG)}
    assert visible_before == {
        DIRECT: AccessLevel.DOWNLOAD, MEMBER: AccessLevel.VIEW, VIA_RG: AccessLevel.DOWNLOAD,
    }

    result = _replace(_service(monkeypatch))

    assert result["replaced"] is True
    session = factory()
    try:
        dataset = session.query(Dataset).filter(Dataset.ds_id == DS_ID).one()
        assert dataset.title == "New title"  # the replace really changed the record
    finally:
        session.close()
    assert _access_rows(factory) == before
    assert {email: _visible_level(email) for email in (DIRECT, MEMBER, VIA_RG)} == visible_before
    assert _visible_level("stranger@example.org") is None


@requires_postgres
def test_a_failed_replace_rolls_back_and_still_keeps_permissions(factory, monkeypatch):
    before = _access_rows(factory)

    service = _service(monkeypatch, fail_manifest=True)
    with pytest.raises(HTTPException):
        _replace(service)

    assert service.cache_refreshed == [DS_ID]  # the undo really ran

    session = factory()
    try:
        dataset = session.query(Dataset).filter(Dataset.ds_id == DS_ID).one()
        assert dataset.title == "Old title"  # rolled back
    finally:
        session.close()
    assert _access_rows(factory) == before
    assert _visible_level(DIRECT) == AccessLevel.DOWNLOAD
    assert _visible_level(MEMBER) == AccessLevel.VIEW
    assert _visible_level(VIA_RG) == AccessLevel.DOWNLOAD

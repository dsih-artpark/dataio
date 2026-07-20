from typing import List, Optional
import logging
import re
from sqlalchemy.orm import joinedload
import bcrypt
import secrets
import dateutil
from sqlalchemy import select
from datetime import datetime, timedelta

from dataio.api.database.config import Session
from dataio.api.database.enums import ResourceType

from dataio.api.database.models import (
    Dataset,
    AccessLevel,
    User,
    UserGroup,
    UserPermission,
    ResourceGroup,
    ResourceGroupMember,
    DataOwner,
    Collection,
    Tag,
    DatasetTag,
    RawDataset,
    DatasetRawDataset,
    ReservedDatasetID,
    Region,
    RateLimit,
)
from dataio.api.auth.permissions import determine_highest_permission
from dataio.api.models import (
    DatasetCreate,
    DatasetUpdate,
    UserCreate,
    UserReturn,
    DataOwnerCreate,
    CollectionCreate,
    DataOwnerUpdate,
    CollectionUpdate,
    RawDatasetCreate,
    RawDatasetUpdate,
    UserGroupCreate,
    ResourceGroupCreate,
    UserPermissionCreate,
    ResourceGroupMemberCreate,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_if_dataset_exists(dataset_id: str):
    session = Session()
    try:
        dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
        return dataset is not None
    except Exception as e:
        logger.error(f"Error checking if dataset exists: {str(e)}")
        raise


def get_dataset(dataset_id: str):
    session = Session()
    try:
        dataset = (
            session.query(Dataset)
            .options(
                joinedload(Dataset.collection),
                joinedload(Dataset.data_owner),
                joinedload(Dataset.spatial_coverage_region),
                joinedload(Dataset.raw_datasets),
                joinedload(Dataset.tags),
            )
            .filter(Dataset.ds_id == dataset_id)
            .first()
        )
        # Eagerly access relationships before session closes
        if dataset:
            _ = dataset.collection
            _ = dataset.data_owner
            _ = dataset.spatial_coverage_region
            _ = list(dataset.raw_datasets) if dataset.raw_datasets else []
            _ = list(dataset.tags) if dataset.tags else []
        return dataset
    except Exception as e:
        logger.error(f"Error getting dataset: {str(e)}")
        raise
    finally:
        session.close()


def get_collection_by_identifier(collection_id: str):
    session = Session()
    try:
        return (
            session.query(Collection)
            .filter(Collection.collection_id == collection_id)
            .first()
        )
    except Exception as e:
        logger.error(f"Error getting collection by identifier: {str(e)}")
        raise
    finally:
        session.close()


def get_raw_dataset_by_identifier(raw_dataset_id: str):
    session = Session()
    try:
        return (
            session.query(RawDataset)
            .filter(RawDataset.rds_id == raw_dataset_id)
            .first()
        )
    except Exception as e:
        logger.error(f"Error getting raw dataset by identifier: {str(e)}")
        raise
    finally:
        session.close()


def list_reserved_dataset_ids(search: str | None = None, limit: int = 100, offset: int = 0):
    session = Session()
    try:
        query = session.query(ReservedDatasetID)
        if search:
            query = query.filter(ReservedDatasetID.ds_id.ilike(f"%{search}%"))
        total = query.count()
        rows = query.order_by(ReservedDatasetID.created_at.desc()).offset(offset).limit(limit).all()
        return rows, total
    except Exception as e:
        logger.error(f"Error listing reserved dataset IDs: {str(e)}")
        raise
    finally:
        session.close()


def create_reserved_dataset_id(ds_id: str, collection_id: str | None, note: str | None, reserved_by: str):
    session = Session()
    try:
        if check_if_dataset_exists(ds_id):
            raise ValueError(f"Dataset with ID {ds_id} already exists")
        existing = session.query(ReservedDatasetID).filter(ReservedDatasetID.ds_id == ds_id).first()
        if existing:
            raise ValueError(f"Dataset ID {ds_id} is already reserved")
        reservation = ReservedDatasetID(
            ds_id=ds_id,
            collection_id=collection_id,
            note=note,
            reserved_by=reserved_by,
        )
        session.add(reservation)
        session.commit()
        session.refresh(reservation)
        return reservation
    except Exception as e:
        logger.error(f"Error creating reserved dataset ID: {str(e)}")
        raise
    finally:
        session.close()


def delete_reserved_dataset_id(ds_id: str):
    session = Session()
    try:
        reservation = session.query(ReservedDatasetID).filter(ReservedDatasetID.ds_id == ds_id).first()
        if not reservation:
            raise ValueError(f"Reserved dataset ID {ds_id} not found")
        session.delete(reservation)
        session.commit()
    except Exception as e:
        logger.error(f"Error deleting reserved dataset ID: {str(e)}")
        raise
    finally:
        session.close()


def update_dataset_manifest_cache(
    dataset_id: str,
    *,
    manifest_yaml: str,
    manifest_json: dict,
    updated_by: str,
):
    session = Session()
    try:
        dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
        if not dataset:
            raise ValueError(f"Dataset with ID {dataset_id} not found")

        now = datetime.utcnow()
        dataset.manifest_yaml = manifest_yaml
        dataset.manifest_json = manifest_json
        dataset.manifest_updated_at = now
        dataset.manifest_updated_by = updated_by
        dataset.documentation_synced_at = now
        session.commit()
        session.refresh(dataset)
        return dataset
    except Exception as e:
        logger.error(f"Error updating dataset manifest cache: {str(e)}")
        raise
    finally:
        session.close()


def get_datasets(
    limit: int = 100, offset: int = 0, user_permissions: List[UserPermission] = None
) -> List[Dataset]:
    """
    Fetch datasets from the database with pagination.

    Args:
        limit (int): Maximum number of datasets to return
        offset (int): Number of datasets to skip

    Returns:
        List[Dataset]: List of Dataset objects with their related data
    """
    if user_permissions is None:
        raise ValueError("User permissions are required")
    session = Session()
    try:
        datasets = (
            session.query(Dataset)
            .options(
                joinedload(Dataset.collection),
                joinedload(Dataset.data_owner),
                joinedload(Dataset.spatial_coverage_region),
                joinedload(Dataset.raw_datasets),
                joinedload(Dataset.tags),
            )
            .limit(limit)
            .offset(offset)
            .all()
        )

        print(datasets)

        dataset_user_permissions = [
            user_permission
            for user_permission in user_permissions
            if user_permission.resource_type == "DATASET"
            or user_permission.resource_type == "*"
        ]

        for dataset in datasets:
            possible_permissions = [
                user_permission.permission
                for user_permission in dataset_user_permissions
                if (user_permission.resource_id == dataset.ds_id)
                or (user_permission.resource_id == "*")
            ]
            possible_permissions.append(dataset.access_level)
            dataset.access_level = determine_highest_permission(possible_permissions)

        datasets_filtered = [
            dataset for dataset in datasets if dataset.access_level != AccessLevel.NONE
        ]
        print(datasets_filtered)
        return datasets_filtered
    except Exception as e:
        logger.error(f"Error fetching datasets: {str(e)}")
        raise
    finally:
        session.close()


def parse_date(date_string: str):
    if date_string is None:
        return None
    if len(date_string) == 4 and date_string.isdigit():
        return dateutil.parser.parse(f"{date_string}-01-01")
    else:
        return dateutil.parser.parse(date_string)


def create_dataset(dataset_create: DatasetCreate):
    session = Session()
    try:
        if check_if_dataset_exists(dataset_create.ds_id):
            raise ValueError(f"Dataset with ID {dataset_create.ds_id} already exists")

        collection = (
            session.query(Collection)
            .filter(Collection.collection_id == dataset_create.collection_id)
            .first()
        )
        if not collection:
            raise ValueError(
                f"Collection with ID {dataset_create.collection_id} not found"
            )

        data_owner = (
            session.query(DataOwner)
            .filter(DataOwner.name == dataset_create.data_owner_name)
            .first()
        )
        if not data_owner:
            raise ValueError(
                f"Data owner with name {dataset_create.data_owner_name} not found"
            )

        tc_start_date = parse_date(dataset_create.temporal_coverage_start_date)
        tc_end_date = parse_date(dataset_create.temporal_coverage_end_date)

        dataset = Dataset(
            ds_id=dataset_create.ds_id,
            title=dataset_create.title,
            collection_id=collection.id,
            data_owner_id=data_owner.id,
            description=dataset_create.description,
            spatial_coverage_region_id=dataset_create.spatial_coverage_region_id,
            spatial_resolution=dataset_create.spatial_resolution,
            temporal_coverage_start_date=tc_start_date,
            temporal_coverage_end_date=tc_end_date,
            temporal_resolution=dataset_create.temporal_resolution,
            access_level=dataset_create.access_level,
            additional_metadata=dataset_create.additional_metadata,
        )

        session.add(dataset)

        if dataset_create.tags:
            for tag in dataset_create.tags:
                # check if tag exists
                existing_tag = session.query(Tag).filter(Tag.tag_name == tag).first()
                if not existing_tag:
                    existing_tag = Tag(tag_name=tag)
                    session.add(existing_tag)
                    # flush gets the existing tag id, without committing the transaction
                    session.flush()
                dataset_tag = DatasetTag(dataset_id=dataset.id, tag_id=existing_tag.id)
                session.add(dataset_tag)

        for raw_dataset_id in dataset_create.raw_dataset_ids:
            raw_dataset = (
                session.query(RawDataset)
                .filter(RawDataset.rds_id == raw_dataset_id)
                .first()
            )
            if not raw_dataset:
                raise ValueError(f"Raw dataset with ID {raw_dataset_id} not found")
            dataset_raw_dataset = DatasetRawDataset(
                dataset_id=dataset.id, raw_dataset_id=raw_dataset.id
            )
            session.add(dataset_raw_dataset)

        reserved = session.query(ReservedDatasetID).filter(ReservedDatasetID.ds_id == dataset_create.ds_id).first()
        if reserved:
            session.delete(reserved)

        session.commit()
        session.refresh(dataset)
        return dataset
    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        raise
    finally:
        session.close()


def update_dataset(dataset_id: str, dataset_update: DatasetUpdate):
    session = Session()
    try:
        dataset = (
            session.query(Dataset)
            .options(
                joinedload(Dataset.collection),
                joinedload(Dataset.raw_datasets),
                joinedload(Dataset.tags),
            )
            .filter(Dataset.ds_id == dataset_id)
            .first()
        )
        if not dataset:
            raise ValueError(f"Dataset with ID {dataset_id} not found")

        next_dataset_id = dataset_update.ds_id or dataset.ds_id
        if next_dataset_id != dataset.ds_id and check_if_dataset_exists(next_dataset_id):
            raise ValueError(f"Dataset with ID {next_dataset_id} already exists")

        if dataset_update.collection_id is not None:
            collection = (
                session.query(Collection)
                .filter(Collection.collection_id == dataset_update.collection_id)
                .first()
            )
            if not collection:
                raise ValueError(
                    f"Collection with ID {dataset_update.collection_id} not found"
                )
            dataset.collection_id = collection.id

        if dataset_update.ds_id is not None:
            previous_dataset_id = dataset.ds_id
            dataset.ds_id = dataset_update.ds_id
            session.query(UserPermission).filter(
                UserPermission.resource_type == ResourceType.DATASET,
                UserPermission.resource_id == previous_dataset_id,
            ).update({"resource_id": dataset_update.ds_id}, synchronize_session=False)
            session.query(ResourceGroupMember).filter(
                ResourceGroupMember.resource_type == ResourceType.DATASET,
                ResourceGroupMember.resource_id == previous_dataset_id,
            ).update({"resource_id": dataset_update.ds_id}, synchronize_session=False)
            reserved = session.query(ReservedDatasetID).filter(ReservedDatasetID.ds_id == dataset_update.ds_id).first()
            if reserved:
                session.delete(reserved)

        if dataset_update.data_owner_name is not None:
            data_owner = (
                session.query(DataOwner)
                .filter(DataOwner.name == dataset_update.data_owner_name)
                .first()
            )
            if not data_owner:
                raise ValueError(
                    f"Data owner with name {dataset_update.data_owner_name} not found"
                )
            dataset.data_owner_id = data_owner.id

        if "title" in dataset_update.model_fields_set:
            dataset.title = dataset_update.title
        if "description" in dataset_update.model_fields_set:
            dataset.description = dataset_update.description
        if "spatial_coverage_region_id" in dataset_update.model_fields_set:
            dataset.spatial_coverage_region_id = dataset_update.spatial_coverage_region_id
        if "spatial_resolution" in dataset_update.model_fields_set:
            dataset.spatial_resolution = dataset_update.spatial_resolution
        if "temporal_resolution" in dataset_update.model_fields_set:
            dataset.temporal_resolution = dataset_update.temporal_resolution
        if "access_level" in dataset_update.model_fields_set:
            dataset.access_level = dataset_update.access_level
        if "additional_metadata" in dataset_update.model_fields_set:
            dataset.additional_metadata = dataset_update.additional_metadata
        if "temporal_coverage_start_date" in dataset_update.model_fields_set:
            dataset.temporal_coverage_start_date = parse_date(
                dataset_update.temporal_coverage_start_date
            )
        if "temporal_coverage_end_date" in dataset_update.model_fields_set:
            dataset.temporal_coverage_end_date = parse_date(
                dataset_update.temporal_coverage_end_date
            )

        if dataset_update.tags is not None:
            dataset.tags.clear()
            for tag_name in dataset_update.tags:
                existing_tag = session.query(Tag).filter(Tag.tag_name == tag_name).first()
                if not existing_tag:
                    existing_tag = Tag(tag_name=tag_name)
                    session.add(existing_tag)
                    session.flush()
                dataset.tags.append(existing_tag)

        if dataset_update.raw_dataset_ids is not None:
            dataset.raw_datasets.clear()
            for raw_dataset_id in dataset_update.raw_dataset_ids:
                raw_dataset = (
                    session.query(RawDataset)
                    .filter(RawDataset.rds_id == raw_dataset_id)
                    .first()
                )
                if not raw_dataset:
                    raise ValueError(f"Raw dataset with ID {raw_dataset_id} not found")
                dataset.raw_datasets.append(raw_dataset)

        session.commit()
        session.refresh(dataset)
        return dataset
    except Exception as e:
        logger.error(f"Error updating dataset: {str(e)}")
        raise
    finally:
        session.close()


def delete_dataset(dataset_id: str):
    session = Session()
    try:
        dataset = (
            session.query(Dataset)
            .options(joinedload(Dataset.raw_datasets), joinedload(Dataset.tags))
            .filter(Dataset.ds_id == dataset_id)
            .first()
        )
        if not dataset:
            raise ValueError(f"Dataset with ID {dataset_id} not found")

        session.query(UserPermission).filter(
            UserPermission.resource_type == ResourceType.DATASET,
            UserPermission.resource_id == dataset_id,
        ).delete(synchronize_session=False)
        session.query(ResourceGroupMember).filter(
            ResourceGroupMember.resource_type == ResourceType.DATASET,
            ResourceGroupMember.resource_id == dataset_id,
        ).delete(synchronize_session=False)

        dataset.raw_datasets.clear()
        dataset.tags.clear()
        session.flush()
        session.delete(dataset)
        session.commit()
    except Exception as e:
        logger.error(f"Error deleting dataset: {str(e)}")
        raise
    finally:
        session.close()


def get_next_dataset_serial_number(session=None) -> int:
    """The next number in the catalogue-wide dataset ID counter (see
    suggest_next_dataset_id) - independent of collection, since this counter
    is global. Accepts an optional existing session so callers already
    holding one (e.g. suggest_next_dataset_id) don't open a second.
    """
    owns_session = session is None
    session = session or Session()
    try:
        existing_ids = session.query(Dataset.ds_id).all()
        reserved_ids = session.query(ReservedDatasetID.ds_id).all()
        max_suffix = 0
        suffix_pattern = re.compile(r"DS(\d{4})$")
        for (ds_id,) in [*existing_ids, *reserved_ids]:
            match = suffix_pattern.search(ds_id or "")
            if match:
                max_suffix = max(max_suffix, int(match.group(1)))
        return max_suffix + 1
    finally:
        if owns_session:
            session.close()


def suggest_next_dataset_id(collection_id: str) -> str:
    """Suggest the next dataset ID, matching the master catalogue's numbering:
    the numeric suffix is a single counter shared across every dataset in the
    catalogue (not scoped to one collection), so it always keeps pace with
    whatever the catalogue would assign next, regardless of which collection
    the new dataset belongs to.
    """
    session = Session()
    try:
        next_number = get_next_dataset_serial_number(session)
        prefix = f"{collection_id}DS"
        return f"{prefix}{next_number:04d}"
    except Exception as e:
        logger.error(f"Error suggesting dataset id: {str(e)}")
        raise
    finally:
        session.close()


def suggest_next_raw_dataset_id_for_category(category_id: str, session=None) -> str:
    """The next raw dataset ID for a category (e.g. "CS"), matching the
    master catalogue's numbering: the counter is shared across every
    collection in that category (all of CS0001, CS0007, CS0026... share one
    "CS" counter), using the catalogue's own unpadded "{category}RDS{n}"
    format (e.g. CSRDS16).

    Also folds in rds_ids still stored in the older per-collection format
    (e.g. CS0002RDS0003) so a category that already has raw datasets under
    the old scheme doesn't restart its counter from 1 and collide with them.
    """
    owns_session = session is None
    session = session or Session()
    try:
        existing_ids = session.query(RawDataset.rds_id).all()
        max_suffix = 0
        new_format_pattern = re.compile(rf"^{re.escape(category_id)}RDS(\d+)$")
        legacy_format_pattern = re.compile(rf"^{re.escape(category_id)}\d{{4}}RDS(\d{{4}})$")
        for (rds_id,) in existing_ids:
            rds_id = rds_id or ""
            match = new_format_pattern.match(rds_id) or legacy_format_pattern.match(rds_id)
            if match:
                max_suffix = max(max_suffix, int(match.group(1)))
        return f"{category_id}RDS{max_suffix + 1}"
    finally:
        if owns_session:
            session.close()


def suggest_next_raw_dataset_id(collection_id: str) -> str:
    """Suggest the next raw dataset ID for the category that collection_id
    belongs to - see suggest_next_raw_dataset_id_for_category.
    """
    session = Session()
    try:
        collection = (
            session.query(Collection)
            .filter(Collection.collection_id == collection_id)
            .first()
        )
        category_id = collection.category_id if collection else re.sub(r"\d+$", "", collection_id)
        return suggest_next_raw_dataset_id_for_category(category_id, session)
    except Exception as e:
        logger.error(f"Error suggesting raw dataset id: {str(e)}")
        raise
    finally:
        session.close()


def list_raw_datasets(search: str | None = None, limit: int = 100, offset: int = 0):
    session = Session()
    try:
        query = session.query(RawDataset)
        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                (RawDataset.rds_id.ilike(search_pattern))
                | (RawDataset.title.ilike(search_pattern))
                | (RawDataset.source.ilike(search_pattern))
            )

        total = query.count()
        raw_datasets = query.order_by(RawDataset.rds_id).offset(offset).limit(limit).all()
        return raw_datasets, total
    except Exception as e:
        logger.error(f"Error listing raw datasets: {str(e)}")
        raise
    finally:
        session.close()


def update_raw_dataset(raw_dataset_id: str, raw_dataset_update: RawDatasetUpdate):
    session = Session()
    try:
        raw_dataset = (
            session.query(RawDataset)
            .filter(RawDataset.rds_id == raw_dataset_id)
            .first()
        )
        if not raw_dataset:
            raise ValueError(f"Raw dataset with ID {raw_dataset_id} not found")

        for key, value in raw_dataset_update.model_dump().items():
            if value is not None:
                setattr(raw_dataset, key, value)

        session.commit()
        session.refresh(raw_dataset)
        return raw_dataset
    except Exception as e:
        logger.error(f"Error updating raw dataset: {str(e)}")
        raise
    finally:
        session.close()


def get_resource_group_members(resource_group_id: str):
    session = Session()
    try:
        resource_group_members = (
            session.query(ResourceGroupMember)
            .filter(ResourceGroupMember.resource_group_id == resource_group_id)
            .all()
        )
        return resource_group_members
    except Exception as e:
        logger.error(f"Error getting resource group members: {str(e)}")
        raise
    finally:
        session.close()


def create_user(user_create: UserCreate):
    try:
        if user_create.is_group:
            # dont generate key for group
            user = User(email=user_create.email, is_group=user_create.is_group)
            user_return = UserReturn(email=user.email, is_group=user.is_group, key=None)
            session = Session()
            session.add(user)
            session.commit()
            return user_return
        else:
            key = secrets.token_urlsafe()
            bytes = key.encode("utf-8")
            salt = bcrypt.gensalt()
            hash = bcrypt.hashpw(bytes, salt)
            user = User(
                email=user_create.email, is_group=user_create.is_group, key=hash
            )
            user_return = UserReturn(
                email=user.email,
                is_group=user.is_group,
                key=key,
                message="Please note down the key. It cannot be seen or retrieved again. You will have to regenerate a new key if you forget it.",
            )
            session = Session()
            session.add(user)
            session.commit()
            return user_return
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise
    finally:
        session.close()


def create_user_group(user_group: UserGroupCreate):
    session = Session()
    try:
        user_group = UserGroup(**user_group.model_dump())
        session.add(user_group)
        session.commit()
        session.refresh(user_group)
    except Exception as e:
        logger.error(f"Error creating user group: {str(e)}")
        raise
    finally:
        session.close()


def create_resource_group(resource_group: ResourceGroupCreate):
    session = Session()
    try:
        resource_group = ResourceGroup(**resource_group.model_dump())
        session.add(resource_group)
        session.commit()
        session.refresh(resource_group)
    except Exception as e:
        logger.error(f"Error creating resource group: {str(e)}")
        raise
    finally:
        session.close()


def create_resource_group_member(resource_group_member: ResourceGroupMemberCreate):
    session = Session()
    try:
        resource_group_member = ResourceGroupMember(
            **resource_group_member.model_dump()
        )
        session.add(resource_group_member)
        session.commit()
        session.refresh(resource_group_member)
    except Exception as e:
        logger.error(f"Error creating resource group member: {str(e)}")
        raise
    finally:
        session.close()


def create_user_permission(user_permission: UserPermissionCreate):
    session = Session()
    try:
        user_permission = UserPermission(**user_permission.model_dump())
        session.add(user_permission)
        session.commit()
        session.refresh(user_permission)
    except Exception as e:
        logger.error(f"Error creating user permission: {str(e)}")
        raise
    finally:
        session.close()


def create_data_owner(data_owner: DataOwnerCreate):
    session = Session()
    try:
        data_owner = DataOwner(**data_owner.model_dump())
        session.add(data_owner)
        session.commit()
        session.refresh(data_owner)
        return data_owner
    except Exception as e:
        logger.error(f"Error creating data owner: {str(e)}")
        raise
    finally:
        session.close()


def create_collection(collection: CollectionCreate):
    session = Session()
    try:
        collection = Collection(**collection.model_dump())
        session.add(collection)
        session.commit()
        session.refresh(collection)
        return collection
    except Exception as e:
        logger.error(f"Error creating collection: {str(e)}")
        raise
    finally:
        session.close()


def get_data_owners():
    session = Session()
    try:
        data_owners = session.query(DataOwner).all()
        return data_owners
    except Exception as e:
        logger.error(f"Error getting data owners: {str(e)}")
        raise
    finally:
        session.close()


def get_users():
    session = Session()
    try:
        results = session.execute(select(User.email, User.is_group, User.is_admin))
        users = []
        for result in results:
            users.append(
                {
                    "email": result.email,
                    "is_group": result.is_group,
                    "is_admin": result.is_admin,
                }
            )
        return users
    except Exception as e:
        logger.error(f"Error getting users: {str(e)}")


def get_collections():
    session = Session()
    try:
        collections = session.query(Collection).all()
        return collections
    except Exception as e:
        logger.error(f"Error getting collections: {str(e)}")
        raise
    finally:
        session.close()


def update_data_owner(data_owner_id: int, data_owner_update: DataOwnerUpdate):
    session = Session()
    try:
        data_owner = (
            session.query(DataOwner).filter(DataOwner.id == data_owner_id).first()
        )
        if not data_owner:
            raise ValueError(f"Data owner with ID {data_owner_id} not found")
        for key, value in data_owner_update.model_dump().items():
            if value is not None:
                setattr(data_owner, key, value)
        session.commit()
        session.refresh(data_owner)
        return data_owner
    except Exception as e:
        logger.error(f"Error updating data owner: {str(e)}")
        raise
    finally:
        session.close()


def update_collection(collection_id: int, collection_update: CollectionUpdate):
    session = Session()
    try:
        collection = (
            session.query(Collection).filter(Collection.id == collection_id).first()
        )
        if not collection:
            raise ValueError(f"Collection with ID {collection_id} not found")
        for key, value in collection_update.model_dump().items():
            if value is not None:
                setattr(collection, key, value)
        session.commit()
        session.refresh(collection)
        return collection
    except Exception as e:
        logger.error(f"Error updating collection: {str(e)}")
        raise
    finally:
        session.close()


def create_raw_dataset(raw_dataset: RawDatasetCreate):
    session = Session()
    try:
        raw_dataset = RawDataset(**raw_dataset.model_dump())
        session.add(raw_dataset)
        session.commit()
        session.refresh(raw_dataset)
        return raw_dataset
    except Exception as e:
        logger.error(f"Error creating raw dataset: {str(e)}")
        raise
    finally:
        session.close()


def get_parentID_of_region(region_id: str):
    session = Session()
    try:
        region = session.query(Region).filter(Region.region_id == region_id).first()
        if region is None:
            return None
        parent_id = region.parent_region_id
        return parent_id
    except Exception as e:
        logger.error(f"Error fetching parent id from DB: {str(e)}")
        raise
    finally:
        session.close()


def get_regions_by_ids(region_ids: List[str]):
    """
    Get region names by their region_ids.

    Args:
        region_ids: List of region_id strings

    Returns:
        Dict mapping region_id to region_name
    """
    session = Session()
    try:
        regions = session.query(Region).filter(Region.region_id.in_(region_ids)).all()
        return {region.region_id: region.region_name for region in regions}
    except Exception as e:
        logger.error(f"Error fetching regions from DB: {str(e)}")
        raise
    finally:
        session.close()


def check_rate_limit_exceeded(user_email: str, access_point: str):
    session = Session()
    try:
        rate_limit = (
            session.query(RateLimit)
            .filter(
                RateLimit.user_email == user_email,
                RateLimit.access_point == access_point,
            )
            .first()
        )
        if not rate_limit:
            print("Rate limit not found")
            return False
        if rate_limit.last_access_timestamp < datetime.now() - timedelta(minutes=1):
            rate_limit.number_of_attempts = 0
            session.commit()
            session.refresh(rate_limit)
        return rate_limit.number_of_attempts >= rate_limit.max_limit_per_minute
    except Exception as e:
        logger.error(f"Error checking rate limit exceeded: {str(e)}")
        raise


def get_children_regions(parent_region_id: str):
    """
    Get all direct children regions for a given parent region_id.

    Args:
        parent_region_id: The region_id of the parent region

    Returns:
        List of Region objects that have the specified parent_region_id
    """
    session = Session()
    try:
        children = session.query(Region).filter(Region.parent_region_id == parent_region_id).all()
        return children
    except Exception as e:
        logger.error(f"Error fetching children regions from DB: {str(e)}")
        raise
    finally:
        session.close()


def update_shapefile_rate_limit(user_email: str):
    session = Session()
    try:
        rate_limit = (
            session.query(RateLimit)
            .filter(
                RateLimit.user_email == user_email
                and RateLimit.access_point == "shapefile"
            )
            .first()
        )
        if not rate_limit:
            rate_limit = RateLimit(
                user_email=user_email, access_point="shapefile", number_of_attempts=1
            )
            session.add(rate_limit)
            session.commit()
            session.refresh(rate_limit)
        else:
            rate_limit.number_of_attempts += 1
            rate_limit.last_access_timestamp = datetime.now()
            session.commit()
            session.refresh(rate_limit)
    except Exception as e:
        logger.error(f"Error updating shapefile download count: {str(e)}")
        raise
    finally:
        session.close()

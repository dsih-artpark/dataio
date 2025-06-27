from typing import List, Optional
import logging
from sqlalchemy.orm import joinedload
import bcrypt
import secrets
import dateutil

from dataio.api.database.config import Session
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
)
from dataio.api.api.models import (
    DatasetCreate,
    UserCreate,
    UserReturn,
    DataOwnerCreate,
    CollectionCreate,
    DataOwnerUpdate,
    CollectionUpdate,
    RawDatasetCreate,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        ]

        for dataset in datasets:
            possible_permissions = [
                user_permission.permission
                for user_permission in dataset_user_permissions
                if user_permission.resource_id == dataset.ds_id
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


def create_dataset(dataset_create: DatasetCreate):
    session = Session()
    try:
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

        dataset = Dataset(
            ds_id=dataset_create.ds_id,
            title=dataset_create.title,
            collection_id=collection.id,
            data_owner_id=data_owner.id,
            description=dataset_create.description,
            spatial_coverage_region_id=dataset_create.spatial_coverage_region_id,
            spatial_resolution=dataset_create.spatial_resolution,
            temporal_coverage_start_date=dateutil.parser.parse(
                dataset_create.temporal_coverage_start_date
            ),
            temporal_coverage_end_date=dateutil.parser.parse(
                dataset_create.temporal_coverage_end_date
            ),
            temporal_resolution=dataset_create.temporal_resolution,
            access_level=dataset_create.access_level,
            additional_metadata=dataset_create.additional_metadata,
        )

        session.add(dataset)
        session.commit()
        session.refresh(dataset)

        for tag in dataset_create.tags:
            # check if tag exists
            existing_tag = session.query(Tag).filter(Tag.tag_name == tag).first()
            if not existing_tag:
                existing_tag = Tag(tag_name=tag)
                session.add(existing_tag)
                session.commit()
                session.refresh(existing_tag)
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

        session.commit()
        session.refresh(dataset)
        return dataset
    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        raise
    finally:
        session.close()


# def update_dataset(dataset_id: str, new_dataset: DatasetCreate):
#     session = Session()
#     try:
#         dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
#         if not dataset:
#             raise ValueError(f"Dataset with ID {dataset_id} not found")
#         for key, value in new_dataset.model_dump().items():


#         session.commit()
#         session.refresh(dataset)
#         return dataset
#     except Exception as e:
#         logger.error(f"Error updating dataset: {str(e)}")
#         raise


def check_if_admin(user: User):
    if user.is_group:
        raise ValueError("User is a group")
    return user.email == "admin@artpark.in"


def determine_highest_permission(permissions: List[AccessLevel]):
    if AccessLevel.DOWNLOAD in permissions:
        return AccessLevel.DOWNLOAD
    elif AccessLevel.VIEW in permissions:
        return AccessLevel.VIEW
    else:
        return AccessLevel.NONE


def determine_user_permissions(user: User):
    if user.is_group:
        raise ValueError("User is a group")

    session = Session()
    try:
        user_permissions = []
        if user.is_admin is True:
            user_permissions.append(
                UserPermission(
                    user_email=user.email,
                    resource_type="*",
                    resource_id="*",
                    permission="DOWNLOAD",
                )
            )

        user_groups = (
            session.query(UserGroup).filter(UserGroup.user_email == user.email).all()
        )

        for user_group in user_groups:
            group_permissions = determine_user_group_permissions(user_group.group_email)

        user_permissions.extend(
            session.query(UserPermission)
            .filter(UserPermission.user_email == user.email)
            .all()
        )
        user_permissions.extend(group_permissions)

        for user_permission in user_permissions:
            if user_permission.resource_type == "GROUP":
                group_members = get_resource_group_members(user_permission.resource_id)
                for group_member in group_members:
                    user_permissions.append(
                        UserPermission(
                            user_email=user_permission.user_email,
                            resource_type=group_member.resource_type,
                            resource_id=group_member.resource_id,
                            permission=user_permission.permission,
                        )
                    )
                user_permissions.remove(user_permission)
        return user_permissions
    except Exception as e:
        logger.error(f"Error determining user permissions: {str(e)}")
        raise
    finally:
        session.close()


def determine_user_group_permissions(group_email: str):
    session = Session()
    try:
        user_permissions = (
            session.query(UserPermission)
            .filter(UserPermission.user_email == group_email)
            .all()
        )
        return user_permissions
    except Exception as e:
        logger.error(f"Error determining user group permissions: {str(e)}")
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


def check_api_key(api_key: str) -> bool:
    try:
        users = Session().query(User).all()
        for user in users:
            if user.key:
                if bcrypt.checkpw(api_key.encode("utf-8"), user.key):
                    print("user found - key verified")
                    return user
        return None
    except Exception as e:
        logger.error(f"Error checking API key: {str(e)}")


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

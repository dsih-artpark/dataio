from typing import List, Optional
import logging
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
    Region,
    RateLimit,
)
from dataio.api.auth.permissions import determine_highest_permission
from dataio.api.models import (
    DatasetCreate,
    UserCreate,
    UserReturn,
    DataOwnerCreate,
    CollectionCreate,
    DataOwnerUpdate,
    CollectionUpdate,
    RawDatasetCreate,
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
        dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
        return dataset
    except Exception as e:
        logger.error(f"Error getting dataset: {str(e)}")
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

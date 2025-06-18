from typing import List, Optional
import logging
from sqlalchemy.orm import joinedload
import bcrypt
import secrets
import psycopg2.errors

from dataio.api.database.config import Session
from dataio.api.database.models import Dataset, AccessLevel, User, UserGroup, UserPermission, ResourceGroup, ResourceGroupMember, DatasetVersion
from dataio.api.api.models import DatasetCreate, DatasetVersionCreate, UserCreate, UserReturn, DatasetUpdate


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_datasets(limit: int = 100, offset: int = 0, user_permissions: List[UserPermission] = None) -> List[Dataset]:
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

        dataset_user_permissions = [user_permission for user_permission in user_permissions if user_permission.resource_type == 'DATASET']

        for dataset in datasets:
            possible_permissions = [user_permission.permission for user_permission in dataset_user_permissions if user_permission.resource_id == dataset.ds_id]
            possible_permissions.append(dataset.access_level)
            dataset.access_level = determine_highest_permission(possible_permissions)

        datasets_filtered = [dataset for dataset in datasets if dataset.access_level != AccessLevel.NONE]
        print(datasets_filtered)
        return datasets_filtered
    except Exception as e:
        logger.error(f"Error fetching datasets: {str(e)}")
        raise
    finally:
        session.close()

def create_dataset(dataset: DatasetCreate):
    session = Session()
    try:
        dataset = Dataset(**dataset.model_dump())
        session.add(dataset)
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
        dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
        if not dataset:
            raise ValueError(f"Dataset with ID {dataset_id} not found")
        for key, value in dataset_update.model_dump().items():
            if value is not None:
                setattr(dataset, key, value)
        session.commit()
        session.refresh(dataset)
        return dataset
    except Exception as e:
        logger.error(f"Error updating dataset: {str(e)}")
        raise

def create_dataset_version(dataset_id: str, dataset_version_create: DatasetVersionCreate):

    session = Session()
    try:
        # replace ds_id with id
        dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
        if not dataset:
            raise ValueError(f"Dataset with ID {dataset_id} not found")
        
        dataset_version = DatasetVersion(
            dataset_id=dataset.id,
            version_id=dataset_version_create.version_id,
            version_title=dataset_version_create.version_title,
            type=dataset_version_create.type,
            last_modified_date=dataset_version_create.last_modified_date,
            updation_frequency=dataset_version_create.updation_frequency,
            access_level=dataset_version_create.access_level,
        )
        
        session.add(dataset_version)
        session.commit()
        session.refresh(dataset_version)
        return dataset_version
    except Exception as e:
        logger.error(f"Error creating dataset version: {str(e)}")
        raise
    finally:
        session.close()

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
        user_groups = session.query(UserGroup).filter(UserGroup.user_email == user.email).all()

        for user_group in user_groups:
            group_permissions = determine_user_group_permissions(user_group.group_email)

        user_permissions = session.query(UserPermission).filter(UserPermission.user_email == user.email).all()
        user_permissions.extend(group_permissions)

        for user_permission in user_permissions:
            if user_permission.resource_type == 'GROUP': 
                group_members = get_resource_group_members(user_permission.resource_id)
                for group_member in group_members:
                    user_permissions.append(UserPermission(user_email=user_permission.user_email, resource_type=group_member.resource_type, resource_id=group_member.resource_id, permission=user_permission.permission))
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
        user_permissions = session.query(UserPermission).filter(UserPermission.user_email == group_email).all()
        return user_permissions
    except Exception as e:
        logger.error(f"Error determining user group permissions: {str(e)}")
        raise
    finally:
        session.close()

def get_resource_group_members(resource_group_id: str):
    session = Session()
    try:
        resource_group_members = session.query(ResourceGroupMember).filter(ResourceGroupMember.resource_group_id == resource_group_id).all()
        return resource_group_members
    except Exception as e:
        logger.error(f"Error getting resource group members: {str(e)}")
        raise
    finally:
        session.close()

# def get_dataset_by_id(dataset_id: int) -> Optional[Dataset]:
#     """
#     Fetch a single dataset by its ID.
    
#     Args:
#         dataset_id (int): The ID of the dataset to fetch
        
#     Returns:
#         Optional[Dataset]: The Dataset object if found, None otherwise
#     """
#     session = Session()
#     try:
#         dataset = (
#             session.query(Dataset)
#             .options(
#                 joinedload(Dataset.collection),
#                 joinedload(Dataset.data_owner),
#                 joinedload(Dataset.spatial_coverage_region),
#             )
#             .filter(Dataset.id == dataset_id)
#             .first()
#         )
#         return dataset
#     except Exception as e:
#         logger.error(f"Error fetching dataset {dataset_id}: {str(e)}")
#         raise
#     finally:
#         session.close()

def check_api_key(api_key: str) -> bool:
    try:
        users = Session().query(User).all()
        for user in users:
            if user.key:    
                if bcrypt.checkpw(api_key.encode('utf-8'), user.key):
                    print('user found - key verified')
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
            bytes = key.encode('utf-8')
            salt = bcrypt.gensalt()
            hash = bcrypt.hashpw(bytes, salt)
            user = User(email=user_create.email, is_group=user_create.is_group, key=hash)
            user_return = UserReturn(email=user.email, is_group=user.is_group, key=key, message="Please note down the key. It cannot be seen or retrieved again. You will have to regenerate a new key if you forget it.")
            session = Session()
            session.add(user)
            session.commit()
            return user_return
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise
    finally:
        session.close()
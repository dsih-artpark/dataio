from typing import List, Optional
import logging
from sqlalchemy.orm import joinedload
import bcrypt

from .config import Session
from .models import Dataset, AccessLevel, User, UserGroup, UserPermission, ResourceGroup, ResourceGroupMember

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

        dataset_user_permissions = [user_permission for user_permission in user_permissions if user_permission.resource_type == 'DATASET']

        for dataset in datasets:
            possible_permissions = [user_permission.permission for user_permission in dataset_user_permissions if user_permission.resource_id == dataset.ds_id]
            possible_permissions.append(dataset.access_level)
            dataset.access_level = determine_highest_permission(possible_permissions)

        datasets_filtered = [dataset for dataset in datasets if dataset.access_level != AccessLevel.NONE]

        return datasets_filtered
    except Exception as e:
        logger.error(f"Error fetching datasets: {str(e)}")
        raise
    finally:
        session.close()

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

# def create_dataset(
#     raw_dataset_ids: List[int],
#     ds_id: str,
#     title: str,
#     collection_id: int,
#     data_owner_id: int,
#     concept_id: int,
#     description: Optional[str] = None,
#     tag_ids: Optional[List[int]] = None,
#     spatial_coverage: Optional[str] = None,
#     spatial_resolution: Optional[str] = None,
#     temporal_coverage: Optional[str] = None,
#     temporal_resolution: Optional[str] = None,
#     public_access_level: AccessLevel = AccessLevel.NONE,
#     notes: Optional[str] = None,
#     supplementary_documents: Optional[str] = None
# ) -> Dataset:
#     """
#     Create a new dataset in the database.
    
#     Args:
#         raw_dataset_ids (List[int]): List of raw dataset IDs
#         ds_id (str): Dataset identifier
#         title (str): Dataset title
#         collection_id (int): ID of the collection this dataset belongs to
#         data_owner_id (int): ID of the data owner
#         concept_id (int): ID of the concept this dataset represents
#         description (Optional[str]): Dataset description
#         tag_ids (Optional[List[int]]): List of tag IDs
#         spatial_coverage (Optional[str]): Spatial coverage information
#         spatial_resolution (Optional[str]): Spatial resolution information
#         temporal_coverage (Optional[str]): Temporal coverage information
#         temporal_resolution (Optional[str]): Temporal resolution information
#         public_access_level (AccessLevel): Public access level for the dataset
        
#     Returns:
#         Dataset: The created dataset object
        
#     Raises:
#         ValueError: If required fields are missing or invalid
#         Exception: For database errors
#     """
#     session = Session()
#     try:
#         # Create new dataset
#         dataset = Dataset(
#             raw_dataset_ids=raw_dataset_ids,
#             ds_id=ds_id,
#             title=title,
#             collection_id=collection_id,
#             data_owner_id=data_owner_id,
#             concept_id=concept_id,
#             description=description,
#             tag_ids=tag_ids or [],
#             spatial_coverage=spatial_coverage,
#             spatial_resolution=spatial_resolution,
#             temporal_coverage=temporal_coverage,
#             temporal_resolution=temporal_resolution,
#             public_access_level=public_access_level,
#             notes=notes,
#             supplementary_documents=supplementary_documents
#         )
        
#         # Add to session and commit
#         session.add(dataset)
#         session.commit()
        
#         # Refresh to get the created ID and relationships
#         session.refresh(dataset)
        
#         return dataset
#     except Exception as e:
#         session.rollback()
#         logger.error(f"Error creating dataset: {str(e)}")
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
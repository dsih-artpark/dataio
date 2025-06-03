from typing import List, Optional
import logging
from sqlalchemy.orm import joinedload
import bcrypt

from .config import Session
from .models import Dataset, AccessLevel, User

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_datasets(limit: int = 100, offset: int = 0) -> List[Dataset]:
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
        return datasets
    except Exception as e:
        logger.error(f"Error fetching datasets: {str(e)}")
        raise
    finally:
        session.close()

def get_dataset_by_id(dataset_id: int) -> Optional[Dataset]:
    """
    Fetch a single dataset by its ID.
    
    Args:
        dataset_id (int): The ID of the dataset to fetch
        
    Returns:
        Optional[Dataset]: The Dataset object if found, None otherwise
    """
    session = Session()
    try:
        dataset = (
            session.query(Dataset)
            .options(
                joinedload(Dataset.collection),
                joinedload(Dataset.data_owner),
                joinedload(Dataset.spatial_coverage_region),
            )
            .filter(Dataset.id == dataset_id)
            .first()
        )
        return dataset
    except Exception as e:
        logger.error(f"Error fetching dataset {dataset_id}: {str(e)}")
        raise
    finally:
        session.close()

def create_dataset(
    raw_dataset_ids: List[int],
    ds_id: str,
    title: str,
    collection_id: int,
    data_owner_id: int,
    concept_id: int,
    description: Optional[str] = None,
    tag_ids: Optional[List[int]] = None,
    spatial_coverage: Optional[str] = None,
    spatial_resolution: Optional[str] = None,
    temporal_coverage: Optional[str] = None,
    temporal_resolution: Optional[str] = None,
    public_access_level: AccessLevel = AccessLevel.NONE,
    notes: Optional[str] = None,
    supplementary_documents: Optional[str] = None
) -> Dataset:
    """
    Create a new dataset in the database.
    
    Args:
        raw_dataset_ids (List[int]): List of raw dataset IDs
        ds_id (str): Dataset identifier
        title (str): Dataset title
        collection_id (int): ID of the collection this dataset belongs to
        data_owner_id (int): ID of the data owner
        concept_id (int): ID of the concept this dataset represents
        description (Optional[str]): Dataset description
        tag_ids (Optional[List[int]]): List of tag IDs
        spatial_coverage (Optional[str]): Spatial coverage information
        spatial_resolution (Optional[str]): Spatial resolution information
        temporal_coverage (Optional[str]): Temporal coverage information
        temporal_resolution (Optional[str]): Temporal resolution information
        public_access_level (AccessLevel): Public access level for the dataset
        
    Returns:
        Dataset: The created dataset object
        
    Raises:
        ValueError: If required fields are missing or invalid
        Exception: For database errors
    """
    session = Session()
    try:
        # Create new dataset
        dataset = Dataset(
            raw_dataset_ids=raw_dataset_ids,
            ds_id=ds_id,
            title=title,
            collection_id=collection_id,
            data_owner_id=data_owner_id,
            concept_id=concept_id,
            description=description,
            tag_ids=tag_ids or [],
            spatial_coverage=spatial_coverage,
            spatial_resolution=spatial_resolution,
            temporal_coverage=temporal_coverage,
            temporal_resolution=temporal_resolution,
            public_access_level=public_access_level,
            notes=notes,
            supplementary_documents=supplementary_documents
        )
        
        # Add to session and commit
        session.add(dataset)
        session.commit()
        
        # Refresh to get the created ID and relationships
        session.refresh(dataset)
        
        return dataset
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating dataset: {str(e)}")
        raise
    finally:
        session.close() 

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
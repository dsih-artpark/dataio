from sqlalchemy import Column, Integer, String, Enum as SQLEnum, ForeignKey, Text, Date, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, declarative_base
import enum

Base = declarative_base()

class AccessLevel(str, enum.Enum):
    NONE = "NONE"
    VIEW = "VIEW"
    DOWNLOAD = "DOWNLOAD"

class SpatialResolution(str, enum.Enum):
    COUNTRY = "COUNTRY"
    STATE = "STATE"
    UT = "UT"
    DISTRICT = "DISTRICT"
    SUBDISTRICT = "SUBDISTRICT"
    MUNICIPALITY = "MUNICIPALITY"
    VILLAGE = "VILLAGE"
    WARD = "WARD"
    PRABHAG = "PRABHAG"
    ULB = "ULB"
    LAT_LONG = "LAT/LONG"
    OTHER = "OTHER"

class TemporalResolution(str, enum.Enum):
    YEAR = "YEAR"
    MONTH = "MONTH"
    WEEK = "WEEK"
    DATE = "DATE"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"

class VersionType(str, enum.Enum):
    PREPROCESSED = "PREPROCESSED"
    STANDARDISED = "STANDARDISED"

class UpdationFrequency(str, enum.Enum):
    ONE_TIME = "ONE_TIME"
    YEARLY = "YEARLY"
    MONTHLY = "MONTHLY"
    WEEKLY = "WEEKLY"
    DAILY = "DAILY"
    HOURLY = "HOURLY"
    REAL_TIME = "REAL_TIME"
    ADHOC = "ADHOC"

class ResourceType(str, enum.Enum):
    DATASET = "DATASET"
    GROUP = "GROUP"

class Collection(Base):
    __tablename__ = 'collections'

    id = Column(Integer, primary_key=True)
    collection_id = Column(Text, nullable=False)
    collection_name = Column(Text, nullable=False, unique=True)
    category_id = Column(Text, nullable=False)
    category_name = Column(Text, nullable=False)

class DataOwner(Base):
    __tablename__ = 'data_owners'

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False, unique=True)
    contact_person = Column(Text)
    contact_person_email = Column(Text)

class RawDataset(Base):
    __tablename__ = 'raw_datasets'

    id = Column(Integer, primary_key=True)
    rds_id = Column(String(50), nullable=False)
    title = Column(Text, nullable=False)
    source = Column(Text, nullable=False)
    data_owner_id = Column(Integer, ForeignKey('data_owners.id'), nullable=False)

    data_owner = relationship("DataOwner")

class Tag(Base):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    tag_name = Column(Text, nullable=False)

class Region(Base):
    __tablename__ = 'regions'

    id = Column(Integer, primary_key=True)
    region_id = Column(Text, nullable=False)
    region_name = Column(Text, nullable=False)
    region_type = Column(SQLEnum(SpatialResolution), nullable=False)

class Dataset(Base):
    __tablename__ = 'datasets'

    id = Column(Integer, primary_key=True)
    ds_id = Column(String(50), nullable=False)
    title = Column(Text, nullable=False)
    collection_id = Column(Integer, ForeignKey('collections.id'), nullable=False)
    data_owner_id = Column(Integer, ForeignKey('data_owners.id'), nullable=False)
    description = Column(Text)
    spatial_coverage_region_id = Column(Text, ForeignKey('regions.region_id'))
    spatial_resolution = Column(SQLEnum(SpatialResolution), nullable=False)
    temporal_coverage_start_date = Column(Date)
    temporal_coverage_end_date = Column(Date)
    temporal_resolution = Column(SQLEnum(TemporalResolution), nullable=False)
    public_access_level = Column(SQLEnum(AccessLevel), nullable=False)
    additional_metadata = Column(JSONB)

    # Relationships
    collection = relationship("Collection")
    data_owner = relationship("DataOwner")
    spatial_coverage_region = relationship("Region")
    raw_datasets = relationship("RawDataset", secondary="dataset_raw_datasets")
    tags = relationship("Tag", secondary="dataset_tags")

class DatasetRawDataset(Base):
    __tablename__ = 'dataset_raw_datasets'

    dataset_id = Column(Integer, ForeignKey('datasets.id'), primary_key=True)
    raw_dataset_id = Column(Integer, ForeignKey('raw_datasets.id'), primary_key=True)

class DatasetTag(Base):
    __tablename__ = 'dataset_tags'
    
    dataset_id = Column(Integer, ForeignKey('datasets.id'), primary_key=True)
    tag_id = Column(Integer, ForeignKey('tags.id'), primary_key=True)

class DatasetVersion(Base):
    __tablename__ = 'dataset_versions'

    dataset_id = Column(Integer, ForeignKey('datasets.id'), primary_key=True)
    version_id = Column(Text, primary_key=True)
    version_title = Column(Text, nullable=False)
    type = Column(SQLEnum(VersionType), nullable=False)
    last_modified_date = Column(Date, nullable=False)
    updation_frequency = Column(SQLEnum(UpdationFrequency), nullable=False)
    public_access_level = Column(SQLEnum(AccessLevel), nullable=False)

    dataset = relationship("Dataset")

class User(Base):
    __tablename__ = 'users'

    email = Column(Text, primary_key=True)
    key = Column(Text, nullable=True)
    is_group = Column(Boolean, nullable=False, default=False)

class UserGroup(Base):
    __tablename__ = 'user_groups'

    group_email = Column(Text, ForeignKey('users.email'), primary_key=True)
    user_email = Column(Text, ForeignKey('users.email'), primary_key=True)


class UserPermission(Base):
    __tablename__ = 'user_permissions'

    user_email = Column(Text, ForeignKey('users.email'), primary_key=True)
    resource_type = Column(SQLEnum(ResourceType), nullable=False, primary_key=True)
    resource_id = Column(Text, nullable=False, primary_key=True)
    permission = Column(SQLEnum(AccessLevel), nullable=False)

class ResourceGroup(Base):
    __tablename__ = 'resource_groups'

    id = Column(Integer, primary_key=True)
    resource_group_id = Column(Text, nullable=False, unique=True)
    group_name = Column(Text, nullable=False, unique=True)

class ResourceGroupMember(Base):
    __tablename__ = 'resource_group_members'

    resource_group_id = Column(Text, ForeignKey('resource_groups.resource_group_id'), primary_key=True)
    resource_id = Column(Text, nullable=False, primary_key=True)
    resource_type = Column(SQLEnum(ResourceType), nullable=False)
    resource_json = Column(JSONB, nullable=False)
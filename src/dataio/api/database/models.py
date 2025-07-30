from sqlalchemy import (
    Column,
    Integer,
    String,
    Enum as SQLEnum,
    ForeignKey,
    Text,
    Date,
    Boolean,
    DateTime,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
from dataio.api.database.enums import (
    AccessLevel,
    SpatialResolution,
    TemporalResolution,
    ResourceType,
)

Base = declarative_base()


class Collection(Base):
    __tablename__ = "collections"

    id = Column(Integer, primary_key=True)
    collection_id = Column(Text, nullable=False)
    collection_name = Column(Text, nullable=False, unique=True)
    category_id = Column(Text, nullable=False)
    category_name = Column(Text, nullable=False)


class DataOwner(Base):
    __tablename__ = "data_owners"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False, unique=True)
    contact_person = Column(Text)
    contact_person_email = Column(Text)


class RawDataset(Base):
    __tablename__ = "raw_datasets"

    id = Column(Integer, primary_key=True)
    rds_id = Column(String(50), nullable=False)
    title = Column(Text, nullable=False)
    source = Column(Text, nullable=False)


class Tag(Base):
    __tablename__ = "tags"

    id = Column(Integer, primary_key=True)
    tag_name = Column(Text, nullable=False)


class Region(Base):
    __tablename__ = "regions"

    id = Column(Integer, primary_key=True)
    region_id = Column(Text, nullable=False)
    region_name = Column(Text, nullable=False)
    parent_region_id = Column(Text)


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    ds_id = Column(String(50), nullable=False)
    title = Column(Text, nullable=False)
    collection_id = Column(Integer, ForeignKey("collections.id"), nullable=False)
    data_owner_id = Column(Integer, ForeignKey("data_owners.id"), nullable=False)
    description = Column(Text)
    spatial_coverage_region_id = Column(Text, ForeignKey("regions.region_id"))
    spatial_resolution = Column(SQLEnum(SpatialResolution), nullable=False)
    temporal_coverage_start_date = Column(Date)
    temporal_coverage_end_date = Column(Date)
    temporal_resolution = Column(SQLEnum(TemporalResolution), nullable=False)
    access_level = Column(SQLEnum(AccessLevel), nullable=False)
    additional_metadata = Column(JSONB)

    # Relationships
    collection = relationship("Collection")
    data_owner = relationship("DataOwner")
    spatial_coverage_region = relationship("Region")
    raw_datasets = relationship("RawDataset", secondary="dataset_raw_datasets")
    tags = relationship("Tag", secondary="dataset_tags")


class DatasetRawDataset(Base):
    __tablename__ = "dataset_raw_datasets"

    dataset_id = Column(Integer, ForeignKey("datasets.id"), primary_key=True)
    raw_dataset_id = Column(Integer, ForeignKey("raw_datasets.id"), primary_key=True)


class DatasetTag(Base):
    __tablename__ = "dataset_tags"

    dataset_id = Column(Integer, ForeignKey("datasets.id"), primary_key=True)
    tag_id = Column(Integer, ForeignKey("tags.id"), primary_key=True)


class User(Base):
    __tablename__ = "users"

    email = Column(Text, primary_key=True)
    key = Column(Text, nullable=True)
    is_group = Column(Boolean, nullable=False, default=False)
    is_admin = Column(Boolean, nullable=False, default=False)


class UserGroup(Base):
    __tablename__ = "user_groups"

    group_email = Column(Text, ForeignKey("users.email"), primary_key=True)
    user_email = Column(Text, ForeignKey("users.email"), primary_key=True)


class UserPermission(Base):
    __tablename__ = "user_permissions"

    user_email = Column(Text, ForeignKey("users.email"), primary_key=True)
    resource_type = Column(SQLEnum(ResourceType), nullable=False, primary_key=True)
    resource_id = Column(Text, nullable=False, primary_key=True)
    permission = Column(SQLEnum(AccessLevel), nullable=False)


class ResourceGroup(Base):
    __tablename__ = "resource_groups"

    id = Column(Integer, primary_key=True)
    resource_group_id = Column(Text, nullable=False, unique=True)
    group_name = Column(Text, nullable=False, unique=True)


class ResourceGroupMember(Base):
    __tablename__ = "resource_group_members"

    resource_group_id = Column(
        Text, ForeignKey("resource_groups.resource_group_id"), primary_key=True
    )
    resource_id = Column(Text, nullable=False, primary_key=True)
    resource_type = Column(SQLEnum(ResourceType), nullable=False)
    resource_json = Column(JSONB, nullable=True)


class RateLimit(Base):
    __tablename__ = "rate_limit"

    user_email = Column(Text, primary_key=True)
    number_of_attempts = Column(Integer, nullable=False, default=0)
    max_limit_per_minute = Column(Integer, nullable=False, default=5)
    last_access_timestamp = Column(DateTime, nullable=False, default=datetime.now)
    access_point = Column(Text, nullable=False)

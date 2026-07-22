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
    LargeBinary,
    ARRAY,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
import uuid
from dataio.api.database.enums import (
    AccessLevel,
    SpatialResolution,
    TemporalResolution,
    ResourceType,
    DatasetManifestDraftStatus,
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

    # Documentation caching fields (synced from file server)
    readme_md = Column(Text, nullable=True)
    data_dictionary_json = Column(Text, nullable=True)
    manifest_yaml = Column(Text, nullable=True)
    manifest_json = Column(JSONB, nullable=True)
    manifest_updated_at = Column(DateTime, nullable=True)
    manifest_updated_by = Column(Text, nullable=True)
    documentation_synced_at = Column(DateTime, nullable=True)

    # Relationships
    collection = relationship("Collection")
    data_owner = relationship("DataOwner")
    spatial_coverage_region = relationship("Region")
    raw_datasets = relationship("RawDataset", secondary="dataset_raw_datasets")
    tags = relationship("Tag", secondary="dataset_tags")


class ReservedDatasetID(Base):
    __tablename__ = "reserved_dataset_ids"

    id = Column(Integer, primary_key=True)
    ds_id = Column(Text, nullable=False, unique=True)
    collection_id = Column(Text, nullable=True)
    note = Column(Text, nullable=True)
    reserved_by = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ReservedRawDatasetID(Base):
    __tablename__ = "reserved_raw_dataset_ids"

    id = Column(Integer, primary_key=True)
    rds_id = Column(Text, nullable=False, unique=True)
    category_id = Column(Text, nullable=True)
    note = Column(Text, nullable=True)
    reserved_by = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


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
    email_verified = Column(Boolean, nullable=False, default=False)
    last_login = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    display_name = Column(Text, nullable=True)
    # Registration and verification fields
    verification_status = Column(Text, nullable=False, default="verified")  # 'verified', 'pending', 'rejected'
    registered_at = Column(DateTime, nullable=True)
    verified_at = Column(DateTime, nullable=True)
    verified_by = Column(Text, nullable=True)  # Admin email who verified
    suspended_at = Column(DateTime, nullable=True)  # User suspended timestamp
    suspended_by = Column(Text, nullable=True)  # Admin email who suspended


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


# =============================================================================
# Web Authentication Models
# =============================================================================


class Session(Base):
    """JWT session tracking for web authentication."""

    __tablename__ = "sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_email = Column(Text, ForeignKey("users.email", ondelete="CASCADE"), nullable=False)
    refresh_token = Column(Text, nullable=True, unique=True)
    refresh_token_jti_hash = Column(Text, nullable=True, unique=True)
    user_agent = Column(Text, nullable=True)
    ip_address = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)
    revoked_at = Column(DateTime, nullable=True)

    # Relationship
    user = relationship("User", backref="sessions")


class AuthRateLimit(Base):
    """Rate-limit counters for authentication and security-sensitive actions."""

    __tablename__ = "auth_rate_limits"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    action = Column(Text, nullable=False)
    subject = Column(Text, nullable=False)
    attempt_count = Column(Integer, nullable=False, default=0)
    window_started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    blocked_until = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AuthAuditLog(Base):
    """Append-only audit log for authentication and authorization events."""

    __tablename__ = "auth_audit_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(Text, nullable=False)
    outcome = Column(Text, nullable=False)
    actor_email = Column(Text, nullable=True)
    target_email = Column(Text, nullable=True)
    ip_address = Column(Text, nullable=True)
    user_agent = Column(Text, nullable=True)
    details = Column(JSONB, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class OAuthIdentity(Base):
    """OAuth identities linked to DataIO users."""

    __tablename__ = "oauth_identities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    provider = Column(Text, nullable=False)
    provider_user_id = Column(Text, nullable=False)
    user_email = Column(Text, ForeignKey("users.email", ondelete="CASCADE"), nullable=False)
    provider_email = Column(Text, nullable=True)
    provider_email_verified = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_login_at = Column(DateTime, nullable=True)

    user = relationship("User", backref="oauth_identities")


class OTPToken(Base):
    """One-time password tokens for email verification and login."""

    __tablename__ = "otp_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(Text, nullable=False)
    code = Column(Text, nullable=False)
    purpose = Column(Text, nullable=False)  # 'login', 'verify_email', 'invite'
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)
    attempts = Column(Integer, nullable=False, default=0)


class WebAuthnCredential(Base):
    """WebAuthn/Passkey credentials for passwordless authentication."""

    __tablename__ = "webauthn_credentials"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_email = Column(Text, ForeignKey("users.email", ondelete="CASCADE"), nullable=False)
    credential_id = Column(Text, nullable=False, unique=True)
    public_key = Column(LargeBinary, nullable=False)
    sign_count = Column(Integer, nullable=False, default=0)
    device_name = Column(Text, nullable=True)
    transports = Column(ARRAY(Text), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_used_at = Column(DateTime, nullable=True)

    # Relationship
    user = relationship("User", backref="webauthn_credentials")


class WebAuthnChallenge(Base):
    """Temporary storage for WebAuthn challenges during registration/authentication."""

    __tablename__ = "webauthn_challenges"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_email = Column(Text, nullable=False)
    challenge = Column(Text, nullable=False)
    purpose = Column(Text, nullable=False)  # 'registration', 'authentication'
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)


class UserAPIKey(Base):
    """User-managed API keys for self-service key management."""

    __tablename__ = "user_api_keys"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_email = Column(Text, ForeignKey("users.email", ondelete="CASCADE"), nullable=False)
    key_hash = Column(Text, nullable=False)
    key_prefix = Column(Text, nullable=False)  # First 8 chars for identification
    name = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_used_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True)

    # Relationship
    user = relationship("User", backref="api_keys")


class MagicLinkToken(Base):
    """Magic link tokens for registration, invitation, and account deletion verification."""

    __tablename__ = "magic_link_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(Text, nullable=False)
    token = Column(Text, nullable=False, unique=True)
    purpose = Column(Text, nullable=False)  # 'registration', 'account_deletion', 'invitation'
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)
    invited_by = Column(Text, nullable=True)  # Admin email who sent the invitation


# =============================================================================
# Chat Models (AI Assistant)
# =============================================================================


class ChatSession(Base):
    """Chat sessions for AI assistant conversations."""

    __tablename__ = "chat_sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_email = Column(Text, ForeignKey("users.email", ondelete="CASCADE"), nullable=False)
    title = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

    # Relationships
    user = relationship("User", backref="chat_sessions")
    messages = relationship("ChatMessage", back_populates="session", cascade="all, delete-orphan")


class ChatMessage(Base):
    """Individual messages in a chat session."""

    __tablename__ = "chat_messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), ForeignKey("chat_sessions.id", ondelete="CASCADE"), nullable=False)
    role = Column(Text, nullable=False)  # 'user', 'assistant', 'system'
    content = Column(Text, nullable=False)
    tool_calls = Column(JSONB, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationship
    session = relationship("ChatSession", back_populates="messages")


class DatasetManifestDraft(Base):
    """LLM-drafted metadata.yaml, pending curator review/approval.

    Staging area only. As of now, approving a draft (DraftReviewService.
    approve_draft) only flips its status - it does not write anything to
    filestore/Postgres itself. A curator downloads the approved draft_yaml
    and re-imports it through the existing manifest-upload path (see
    AdminDatasetService._validate_and_persist_manifest), which is what
    actually validates and persists it. This table never becomes a second
    source of truth for the live manifest, but "approve" alone is not
    sufficient to make a draft live.
    """

    __tablename__ = "dataset_manifest_drafts"

    draft_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(Text, nullable=True)
    collection_id = Column(Text, nullable=False)
    category_id = Column(Text, nullable=False)
    source_csv_path = Column(Text, nullable=False)
    digitization_log_path = Column(Text, nullable=True)
    # The resolved raw_dataset rds_id, tracked here rather than inside
    # draft_json/draft_yaml - real metadata.yaml files never contain a
    # raw_dataset/rds_id field, that belongs only in info.yml.
    raw_dataset_id = Column(Text, nullable=True)
    # values_callable is required here (unlike the other SQLEnum columns in
    # this file): DatasetManifestDraftStatus's member names are uppercase
    # but its values are lowercase to match the Postgres enum type
    # (dataset_manifest_draft_status), and SQLAlchemy's Enum binds by
    # .name, not .value, unless told otherwise.
    status = Column(
        SQLEnum(DatasetManifestDraftStatus, values_callable=lambda enum_cls: [e.value for e in enum_cls]),
        nullable=False,
        default=DatasetManifestDraftStatus.PENDING,
    )
    draft_yaml = Column(Text, nullable=False)
    draft_json = Column(JSONB, nullable=False)
    flagged_fields = Column(JSONB, nullable=False, default=list)
    reviewer_notes = Column(JSONB, nullable=False, default=list)
    validation_result = Column(JSONB, nullable=True)
    llm_model_id = Column(Text, nullable=False)
    llm_prompt_tokens = Column(Integer, nullable=True)
    llm_completion_tokens = Column(Integer, nullable=True)
    created_by = Column(Text, ForeignKey("users.email"), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    reviewed_by = Column(Text, ForeignKey("users.email"), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    superseded_by_draft_id = Column(UUID(as_uuid=True), ForeignKey("dataset_manifest_drafts.draft_id"), nullable=True)

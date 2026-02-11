"""
Web router for browser-based authentication and user interface.

Provides endpoints for:
- Email OTP and Passkey authentication
- User profile and API key management
- Dataset browsing and downloading
- Admin user and group management
"""

import logging
from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, EmailStr

from dataio.api.database.models import User
from dataio.api.auth.jwt import get_current_web_user
from dataio.api.services.web_auth_service import WebAuthService
from dataio.api.services.web_user_service import WebUserService
from dataio.api.services.web_admin_service import WebAdminService

logger = logging.getLogger(__name__)

web_router = APIRouter(prefix="/api/v1/web")


# =============================================================================
# Request/Response Models
# =============================================================================


class LoginInitiateRequest(BaseModel):
    email: EmailStr


class LoginVerifyRequest(BaseModel):
    email: EmailStr
    code: str


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    refresh_token: str


class PasskeyRegisterRequest(BaseModel):
    credential: dict
    device_name: Optional[str] = None


class PasskeyAuthRequest(BaseModel):
    email: EmailStr
    credential: dict


class UpdateProfileRequest(BaseModel):
    display_name: Optional[str] = None


class CreateAPIKeyRequest(BaseModel):
    name: str
    expires_at: Optional[datetime] = None


class InviteUserRequest(BaseModel):
    email: EmailStr
    display_name: Optional[str] = None
    is_admin: bool = False
    groups: Optional[List[str]] = None


class UpdateUserRequest(BaseModel):
    display_name: Optional[str] = None
    is_admin: Optional[bool] = None


class CreateGroupRequest(BaseModel):
    email: EmailStr
    display_name: Optional[str] = None


class AddGroupMemberRequest(BaseModel):
    user_email: EmailStr


class RegisterInitiateRequest(BaseModel):
    email: EmailStr


class RegisterVerifyRequest(BaseModel):
    email: EmailStr
    code: Optional[str] = None
    magic_token: Optional[str] = None


class AccountDeleteVerifyRequest(BaseModel):
    code: str


class AcceptInvitationRequest(BaseModel):
    token: str


# =============================================================================
# Helper Functions
# =============================================================================


def get_client_info(request: Request) -> tuple[Optional[str], Optional[str]]:
    """Extract user agent and IP address from request."""
    user_agent = request.headers.get("user-agent")
    ip_address = request.client.host if request.client else None
    return user_agent, ip_address


# =============================================================================
# Authentication Endpoints
# =============================================================================


@web_router.post("/auth/login/initiate", tags=["auth"])
async def initiate_login(
    body: LoginInitiateRequest,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Initiate login by sending an OTP code to the user's email.

    No authentication required.
    """
    return auth_service.initiate_login(body.email)


@web_router.post("/auth/login/verify", tags=["auth"])
async def verify_login(
    body: LoginVerifyRequest,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Verify OTP code and create session.

    Returns access token, refresh token, and user info.
    Also indicates if passkey setup is needed.
    """
    user_agent, ip_address = get_client_info(request)
    return auth_service.verify_login(
        email=body.email,
        code=body.code,
        user_agent=user_agent,
        ip_address=ip_address,
    )


@web_router.post("/auth/refresh", tags=["auth"])
async def refresh_tokens(
    body: RefreshRequest,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Refresh access token using refresh token.

    Returns new access token and refresh token.
    """
    user_agent, ip_address = get_client_info(request)
    return auth_service.refresh_tokens(
        refresh_token=body.refresh_token,
        user_agent=user_agent,
        ip_address=ip_address,
    )


@web_router.post("/auth/logout", tags=["auth"])
async def logout(
    body: LogoutRequest,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Logout by revoking the current session.
    """
    return auth_service.logout(body.refresh_token)


@web_router.post("/auth/logout-all", tags=["auth"])
async def logout_all(
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Logout from all sessions.

    Requires authentication.
    """
    return auth_service.logout_all_sessions(user.email)


# =============================================================================
# Registration Endpoints
# =============================================================================


@web_router.post("/auth/register/initiate", tags=["auth"])
async def initiate_registration(
    body: RegisterInitiateRequest,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Initiate registration by sending a verification email.

    Validates email domain against allowed institutional patterns.
    Sends both OTP code and magic link for verification.

    No authentication required.
    """
    return auth_service.initiate_registration(body.email)


@web_router.post("/auth/register/verify", tags=["auth"])
async def verify_registration(
    body: RegisterVerifyRequest,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Complete registration by verifying OTP code or magic link token.

    Creates user account and returns session tokens.

    No authentication required.
    """
    user_agent, ip_address = get_client_info(request)
    return auth_service.verify_registration(
        email=body.email,
        code=body.code,
        magic_token=body.magic_token,
        user_agent=user_agent,
        ip_address=ip_address,
    )


# =============================================================================
# Invitation Endpoints
# =============================================================================


@web_router.post("/auth/accept-invite", tags=["auth"])
async def accept_invitation(
    body: AcceptInvitationRequest,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Accept an invitation by verifying the magic link token.

    Creates a session and returns tokens for the invited user.
    Invitation links expire after 48 hours.

    No authentication required.
    """
    user_agent, ip_address = get_client_info(request)
    return auth_service.accept_invitation(
        token=body.token,
        user_agent=user_agent,
        ip_address=ip_address,
    )


# =============================================================================
# Account Deletion Endpoints
# =============================================================================


@web_router.post("/account/delete/initiate", tags=["account"])
async def initiate_account_deletion(
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Initiate account deletion by sending a verification code.

    Requires authentication.
    """
    return auth_service.initiate_account_deletion(user.email)


@web_router.post("/account/delete/verify", tags=["account"])
async def verify_account_deletion(
    body: AccountDeleteVerifyRequest,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Complete account deletion after OTP verification.

    This action is permanent and cannot be undone.
    Deletes user account, API keys, passkeys, and all sessions.

    Requires authentication.
    """
    return auth_service.verify_account_deletion(user.email, body.code)


# =============================================================================
# Passkey Endpoints
# =============================================================================


@web_router.post("/auth/passkey/register/options", tags=["passkey"])
async def get_passkey_registration_options(
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Get WebAuthn registration options for passkey setup.

    Requires authentication.
    """
    return auth_service.get_passkey_registration_options(user.email)


@web_router.post("/auth/passkey/register/verify", tags=["passkey"])
async def verify_passkey_registration(
    body: PasskeyRegisterRequest,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Verify and store a new passkey.

    Requires authentication.
    """
    return auth_service.verify_passkey_registration(
        user_email=user.email,
        credential=body.credential,
        device_name=body.device_name,
    )


@web_router.post("/auth/passkey/login/options", tags=["passkey"])
async def get_passkey_login_options(
    body: LoginInitiateRequest,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Get WebAuthn authentication options for passkey login.

    No authentication required.
    """
    return auth_service.get_passkey_authentication_options(body.email)


@web_router.post("/auth/passkey/login/verify", tags=["passkey"])
async def verify_passkey_login(
    body: PasskeyAuthRequest,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Verify passkey authentication and create session.

    Returns access token, refresh token, and user info.
    """
    user_agent, ip_address = get_client_info(request)
    return auth_service.verify_passkey_authentication(
        email=body.email,
        credential=body.credential,
        user_agent=user_agent,
        ip_address=ip_address,
    )


@web_router.get("/passkeys", tags=["passkey"])
async def list_passkeys(
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    List all passkeys for the current user.

    Requires authentication.
    """
    return auth_service.list_user_passkeys(user.email)


@web_router.delete("/passkeys/{passkey_id}", tags=["passkey"])
async def delete_passkey(
    passkey_id: str,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Delete a passkey.

    Requires authentication.
    """
    return auth_service.remove_passkey(user.email, passkey_id)


# =============================================================================
# User Profile Endpoints
# =============================================================================


@web_router.get("/me", tags=["profile"])
async def get_current_user(
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get the current user's profile.

    Requires authentication.
    """
    return user_service.get_current_user_profile(user)


@web_router.put("/me", tags=["profile"])
async def update_current_user(
    body: UpdateProfileRequest,
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Update the current user's profile.

    Requires authentication.
    """
    return user_service.update_user_profile(user, display_name=body.display_name)


# =============================================================================
# API Key Endpoints
# =============================================================================


@web_router.get("/api-keys", tags=["api-keys"])
async def list_api_keys(
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    List all API keys for the current user.

    Requires authentication.
    """
    return user_service.list_api_keys(user)


@web_router.post("/api-keys", tags=["api-keys"])
async def create_api_key(
    body: CreateAPIKeyRequest,
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Create a new API key.

    The full key is only returned once. Store it securely.

    Requires authentication.
    """
    return user_service.create_api_key(
        user, name=body.name, expires_at=body.expires_at
    )


@web_router.delete("/api-keys/{key_id}", tags=["api-keys"])
async def revoke_api_key(
    key_id: str,
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Revoke an API key.

    Requires authentication.
    """
    return user_service.revoke_api_key(user, key_id)


# =============================================================================
# Public Dataset Endpoints (No Authentication Required)
# =============================================================================


@web_router.get("/public/datasets", tags=["public"])
async def get_public_datasets(
    search: Optional[str] = None,
    collection_id: Optional[int] = None,
    data_owner_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get publicly accessible datasets.

    Returns datasets with VIEW or DOWNLOAD access level.
    No authentication required.

    Note: To download datasets, users must register and authenticate.
    """
    return user_service.get_public_datasets(
        search=search,
        collection_id=collection_id,
        data_owner_id=data_owner_id,
        limit=limit,
        offset=offset,
    )


@web_router.get("/public/datasets/{dataset_id}", tags=["public"])
async def get_public_dataset(
    dataset_id: str,
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get details for a specific public dataset.

    Only returns datasets with VIEW or DOWNLOAD access level.
    No authentication required.

    Note: Download links are not included. Users must register and
    authenticate to download datasets.
    """
    return user_service.get_public_dataset(dataset_id)


@web_router.get("/public/collections", tags=["public"])
async def get_public_collections(
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get all collections for filtering public datasets.

    No authentication required.
    """
    return user_service.get_collections()


@web_router.get("/public/data-owners", tags=["public"])
async def get_public_data_owners(
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get all data owners for filtering public datasets.

    No authentication required.
    """
    return user_service.get_data_owners()


# =============================================================================
# Dataset Endpoints (Authenticated)
# =============================================================================


@web_router.get("/datasets", tags=["datasets"])
async def get_datasets(
    search: Optional[str] = None,
    collection_id: Optional[int] = None,
    data_owner_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get datasets accessible to the current user.

    Supports filtering by search term, collection, and data owner.

    Requires authentication.
    """
    return user_service.get_datasets(
        user,
        search=search,
        collection_id=collection_id,
        data_owner_id=data_owner_id,
        limit=limit,
        offset=offset,
    )


@web_router.get("/datasets/{dataset_id}", tags=["datasets"])
async def get_dataset(
    dataset_id: str,
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get details for a specific dataset.

    Requires authentication.
    """
    return user_service.get_dataset(user, dataset_id)


@web_router.get("/datasets/{dataset_id}/download-urls", tags=["datasets"])
async def get_dataset_download_urls(
    dataset_id: str,
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get presigned download URLs for all tables in a dataset.

    Returns URLs for tables and metadata files that can be used
    to create a zip file client-side.

    Requires authentication and download permission.
    """
    return user_service.get_dataset_download_urls(user, dataset_id)


@web_router.get("/collections", tags=["datasets"])
async def get_collections(
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get all collections for filtering.

    Requires authentication.
    """
    return user_service.get_collections()


@web_router.get("/data-owners", tags=["datasets"])
async def get_data_owners(
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get all data owners for filtering.

    Requires authentication.
    """
    return user_service.get_data_owners()


# =============================================================================
# Admin User Endpoints
# =============================================================================


@web_router.get("/admin/users", tags=["web-admin/users"])
async def admin_list_users(
    search: Optional[str] = None,
    include_groups: bool = False,
    limit: int = 100,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    List all users.

    Requires admin privileges.
    """
    return admin_service.list_users(
        user,
        search=search,
        include_groups=include_groups,
        limit=limit,
        offset=offset,
    )


# IMPORTANT: This route must come BEFORE /admin/users/{email} to avoid being matched as email="pending"
@web_router.get("/admin/users/pending", tags=["web-admin/users"])
async def admin_list_pending_users(
    limit: int = 50,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    List users pending verification.

    Requires admin privileges.
    """
    # Use getattr to safely access is_admin in case of session issues
    if not getattr(user, 'is_admin', False):
        raise HTTPException(status_code=403, detail="Admin access required")
    return auth_service.get_pending_users(limit=limit, offset=offset)


@web_router.get("/admin/users/{email}", tags=["web-admin/users"])
async def admin_get_user(
    email: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Get detailed information about a user.

    Requires admin privileges.
    """
    return admin_service.get_user(user, email)


@web_router.post("/admin/users", tags=["web-admin/users"])
async def admin_invite_user(
    body: InviteUserRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Invite a new user by sending them an invitation email.

    Requires admin privileges.
    """
    return admin_service.invite_user(
        user,
        email=body.email,
        display_name=body.display_name,
        is_admin=body.is_admin,
        groups=body.groups,
    )


@web_router.put("/admin/users/{email}", tags=["web-admin/users"])
async def admin_update_user(
    email: str,
    body: UpdateUserRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Update a user's profile.

    Requires admin privileges.
    """
    return admin_service.update_user(
        user,
        email=email,
        display_name=body.display_name,
        is_admin=body.is_admin,
    )


# =============================================================================
# Admin Group Endpoints
# =============================================================================


@web_router.get("/admin/groups", tags=["web-admin/groups"])
async def admin_list_groups(
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    List all groups.

    Requires admin privileges.
    """
    return admin_service.list_groups(user, search=search, limit=limit, offset=offset)


@web_router.get("/admin/groups/{group_email}", tags=["web-admin/groups"])
async def admin_get_group(
    group_email: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Get detailed information about a group.

    Requires admin privileges.
    """
    return admin_service.get_group(user, group_email)


@web_router.post("/admin/groups", tags=["web-admin/groups"])
async def admin_create_group(
    body: CreateGroupRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Create a new group.

    Requires admin privileges.
    """
    return admin_service.create_group(
        user, email=body.email, display_name=body.display_name
    )


@web_router.post("/admin/groups/{group_email}/members", tags=["web-admin/groups"])
async def admin_add_group_member(
    group_email: str,
    body: AddGroupMemberRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Add a user to a group.

    Requires admin privileges.
    """
    return admin_service.add_user_to_group(user, group_email, body.user_email)


@web_router.delete("/admin/groups/{group_email}/members/{user_email}", tags=["web-admin/groups"])
async def admin_remove_group_member(
    group_email: str,
    user_email: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Remove a user from a group.

    Requires admin privileges.
    """
    return admin_service.remove_user_from_group(user, group_email, user_email)


# =============================================================================
# Admin User Verification Endpoints
# =============================================================================


@web_router.post("/admin/users/{email}/verify", tags=["web-admin/users"])
async def admin_verify_user(
    email: str,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Verify (approve) a pending user.

    Requires admin privileges.
    """
    if not getattr(user, 'is_admin', False):
        raise HTTPException(status_code=403, detail="Admin access required")
    return auth_service.verify_user(email, user.email)


@web_router.post("/admin/users/{email}/reject", tags=["web-admin/users"])
async def admin_reject_user(
    email: str,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Reject a pending user.

    Requires admin privileges.
    """
    if not getattr(user, 'is_admin', False):
        raise HTTPException(status_code=403, detail="Admin access required")
    return auth_service.reject_user(email, user.email)


@web_router.delete("/admin/users/{email}/invitation", tags=["web-admin/users"])
async def admin_revoke_invitation(
    email: str,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Revoke a pending invitation.

    Invalidates the invitation token and deletes the unverified user.
    Can only be used for users who have not yet accepted their invitation.

    Requires admin privileges.
    """
    if not getattr(user, 'is_admin', False):
        raise HTTPException(status_code=403, detail="Admin access required")
    return auth_service.revoke_invitation(email, user.email)


@web_router.post("/admin/users/{email}/resend-invitation", tags=["web-admin/users"])
async def admin_resend_invitation(
    email: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Resend an invitation to a pending user.

    Generates a new invitation link (48-hour expiry) and sends a new email.
    Can only be used for users who have not yet accepted their invitation.

    Requires admin privileges.
    """
    return admin_service.resend_invitation(user, email)


@web_router.post("/admin/users/{email}/suspend", tags=["web-admin/users"])
async def admin_suspend_user(
    email: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Suspend a user.

    Requires admin privileges.
    """
    return admin_service.suspend_user(user, email)


@web_router.post("/admin/users/{email}/unsuspend", tags=["web-admin/users"])
async def admin_unsuspend_user(
    email: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Unsuspend a user.

    Requires admin privileges.
    """
    return admin_service.unsuspend_user(user, email)


@web_router.delete("/admin/users/{email}", tags=["web-admin/users"])
async def admin_delete_user(
    email: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Delete a user.

    Requires admin privileges.
    """
    return admin_service.delete_user(user, email)


class BulkInviteRequest(BaseModel):
    users: List[dict]


@web_router.post("/admin/users/bulk-invite", tags=["web-admin/users"])
async def admin_bulk_invite_users(
    body: BulkInviteRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Bulk invite users from a list.

    Requires admin privileges.
    """
    return admin_service.bulk_invite_users(user, body.users)


class SetPermissionRequest(BaseModel):
    dataset_id: str
    permission: str  # 'VIEW', 'DOWNLOAD', or 'NONE'


@web_router.post("/admin/users/{email}/permissions", tags=["web-admin/users"])
async def admin_set_user_permission(
    email: str,
    body: SetPermissionRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Set a user's permission for a dataset.

    Requires admin privileges.
    """
    return admin_service.set_user_dataset_permission(
        user, email, body.dataset_id, body.permission
    )


@web_router.delete("/admin/groups/{group_email}", tags=["web-admin/groups"])
async def admin_delete_group(
    group_email: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Delete a group.

    Requires admin privileges.
    """
    return admin_service.delete_group(user, group_email)


@web_router.post("/admin/groups/{group_email}/permissions", tags=["web-admin/groups"])
async def admin_set_group_permission(
    group_email: str,
    body: SetPermissionRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    Set a group's permission for a dataset.

    Requires admin privileges.
    """
    return admin_service.set_group_dataset_permission(
        user, group_email, body.dataset_id, body.permission
    )


@web_router.get("/admin/datasets", tags=["web-admin/datasets"])
async def admin_list_datasets(
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """
    List datasets for permission management.

    Requires admin privileges.
    """
    return admin_service.list_datasets_for_permissions(
        user, search=search, limit=limit, offset=offset
    )


# =============================================================================
# Chat Endpoints (AI Assistant)
# =============================================================================


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    history: Optional[List[dict]] = None
    provider: Optional[str] = None  # "bedrock" or "openrouter"


class ChatSessionCreate(BaseModel):
    title: Optional[str] = None


@web_router.post("/chat", tags=["chat"])
async def chat(
    body: ChatRequest,
    user: User = Depends(get_current_web_user),
):
    """
    Send a message to the AI assistant and get a response.

    This is a non-streaming endpoint that returns the complete response.
    For streaming responses, use the /chat/stream endpoint.

    The AI assistant can search datasets, get details, and help users
    discover data relevant to their needs.

    Supports optional `provider` parameter: "bedrock" (default) or "openrouter".
    """
    from dataio.api.services.chat_service import ChatService

    chat_service = ChatService(provider=body.provider)
    history = body.history or []

    result = await chat_service.chat(
        user_message=body.message,
        conversation_history=history,
        user_email=user.email,
    )

    return {
        "response": result["response"],
        "tool_calls": result["tool_calls"],
    }


@web_router.post("/chat/stream", tags=["chat"])
async def chat_stream(
    body: ChatRequest,
    user: User = Depends(get_current_web_user),
):
    """
    Send a message to the AI assistant and stream the response.

    Returns a Server-Sent Events (SSE) stream with the following event types:
    - text: Partial text content
    - tool_use: Tool being called
    - tool_result: Tool execution result
    - done: Stream complete
    - error: Error occurred

    The AI assistant can search datasets, get details, and help users
    discover data relevant to their needs.

    Supports optional `provider` parameter: "bedrock" (default) or "openrouter".
    """
    from fastapi.responses import StreamingResponse
    from dataio.api.services.chat_service import ChatService
    import json

    chat_service = ChatService(provider=body.provider)
    history = body.history or []

    async def generate():
        async for event in chat_service.chat_stream(
            user_message=body.message,
            conversation_history=history,
            user_email=user.email,
            session_id=body.session_id,
        ):
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@web_router.get("/chat/sessions", tags=["chat"])
async def list_chat_sessions(
    limit: int = 20,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
):
    """
    List the user's chat sessions.
    """
    from dataio.api.services.chat_service import ChatHistoryService

    history_service = ChatHistoryService()
    sessions = await history_service.list_sessions(
        user_email=user.email,
        limit=limit,
        offset=offset,
    )
    return {"sessions": sessions}


@web_router.post("/chat/sessions", tags=["chat"])
async def create_chat_session(
    body: ChatSessionCreate,
    user: User = Depends(get_current_web_user),
):
    """
    Create a new chat session.
    """
    from dataio.api.services.chat_service import ChatHistoryService

    history_service = ChatHistoryService()
    session_id = await history_service.create_session(
        user_email=user.email,
        title=body.title,
    )
    return {"session_id": session_id}


@web_router.get("/chat/sessions/{session_id}", tags=["chat"])
async def get_chat_session(
    session_id: str,
    user: User = Depends(get_current_web_user),
):
    """
    Get chat history for a session.
    """
    from dataio.api.services.chat_service import ChatHistoryService

    history_service = ChatHistoryService()
    history = await history_service.get_session_history(
        session_id=session_id,
        user_email=user.email,
    )
    return {"session_id": session_id, "messages": history}


@web_router.delete("/chat/sessions/{session_id}", tags=["chat"])
async def delete_chat_session(
    session_id: str,
    user: User = Depends(get_current_web_user),
):
    """
    Delete a chat session.
    """
    from dataio.api.services.chat_service import ChatHistoryService

    history_service = ChatHistoryService()
    deleted = await history_service.delete_session(
        session_id=session_id,
        user_email=user.email,
    )
    return {"deleted": deleted}

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

web_router = APIRouter(prefix="/api/v1/web", tags=["web"])


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


@web_router.get("/admin/users", tags=["admin/users"])
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


@web_router.get("/admin/users/{email}", tags=["admin/users"])
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


@web_router.post("/admin/users", tags=["admin/users"])
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


@web_router.put("/admin/users/{email}", tags=["admin/users"])
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


@web_router.get("/admin/groups", tags=["admin/groups"])
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


@web_router.get("/admin/groups/{group_email}", tags=["admin/groups"])
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


@web_router.post("/admin/groups", tags=["admin/groups"])
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


@web_router.post("/admin/groups/{group_email}/members", tags=["admin/groups"])
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


@web_router.delete("/admin/groups/{group_email}/members/{user_email}", tags=["admin/groups"])
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

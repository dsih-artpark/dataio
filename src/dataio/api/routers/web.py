"""
Web router for browser-based authentication and user interface.

Provides endpoints for:
- Email OTP and Passkey authentication
- User profile and API key management
- Dataset browsing and downloading
- Admin user and group management
"""

import logging
import json
import os
import secrets
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlencode

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Request,
    Response,
    UploadFile,
)
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr

from dataio.api.database.enums import VersionType
from dataio.api.database.models import User
from dataio.api.auth.jwt import REFRESH_COOKIE_NAME, get_current_web_user
from dataio.api.models import (
    ClassifyColumnsRequest,
    DatasetCreate,
    DatasetDocumentationUpdate,
    DatasetUpdate,
    ManifestDraftEdit,
    ManifestDraftFlagField,
    ManifestDraftReject,
    RawDatasetCreate,
    RawDatasetUpdate,
)
from dataio.api.services.web_auth_service import WebAuthService
from dataio.api.services.web_admin_service import WebAdminService
from dataio.api.services.web_user_service import WebUserService
from dataio.validate import DatasetKind

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
    refresh_token: Optional[str] = None


class LogoutRequest(BaseModel):
    refresh_token: Optional[str] = None


class PasskeyRegisterRequest(BaseModel):
    credential: dict
    device_name: Optional[str] = None


class PasskeyAuthRequest(BaseModel):
    email: Optional[EmailStr] = None
    credential: dict


class PasskeyOptionsRequest(BaseModel):
    email: Optional[EmailStr] = None


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


class DatasetDeleteVerifyRequest(BaseModel):
    code: str
    confirmation_dataset_id: str


class ReserveDatasetIdRequest(BaseModel):
    ds_id: str
    collection_id: Optional[str] = None
    note: Optional[str] = None


class AcceptInvitationRequest(BaseModel):
    token: str


class OAuthCallbackRequest(BaseModel):
    code: str
    state: str


class AuthProvidersResponse(BaseModel):
    google: bool
    github: bool
    passkey: bool


class SessionInfoResponse(BaseModel):
    id: str
    created_at: str
    last_seen_at: str
    expires_at: str
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    current: bool


# =============================================================================
# Helper Functions
# =============================================================================


def get_client_info(request: Request) -> tuple[Optional[str], Optional[str]]:
    """Extract user agent and IP address from request."""
    user_agent = request.headers.get("user-agent")
    ip_address = request.client.host if request.client else None

    if os.getenv("TRUST_PROXY_HEADERS", "false").lower() == "true":
        forwarded_for = request.headers.get("x-forwarded-for")
        real_ip = request.headers.get("x-real-ip")
        cf_ip = request.headers.get("cf-connecting-ip")
        if cf_ip:
            ip_address = cf_ip.strip()
        elif real_ip:
            ip_address = real_ip.strip()
        elif forwarded_for:
            ip_address = forwarded_for.split(",")[0].strip()
    return user_agent, ip_address


def set_refresh_cookie(response: Response, refresh_token: str) -> None:
    response.set_cookie(
        key=REFRESH_COOKIE_NAME,
        value=refresh_token,
        httponly=True,
        secure=os.getenv("COOKIE_SECURE", "false").lower() == "true",
        samesite=os.getenv("COOKIE_SAMESITE", "lax"),
        max_age=7 * 24 * 60 * 60,
        path="/",
    )


def clear_refresh_cookie(response: Response) -> None:
    response.delete_cookie(key=REFRESH_COOKIE_NAME, path="/")


# =============================================================================
# Authentication Endpoints
# =============================================================================


@web_router.get("/auth/providers", tags=["auth"], response_model=AuthProvidersResponse)
async def get_auth_providers():
    """Return which login providers are configured and should be shown in the UI."""
    return {
        "google": bool(
            os.getenv("GOOGLE_OAUTH_CLIENT_ID") and os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
        ),
        "github": bool(
            os.getenv("GITHUB_OAUTH_CLIENT_ID") and os.getenv("GITHUB_OAUTH_CLIENT_SECRET")
        ),
        "passkey": True,
    }


@web_router.post("/auth/login/initiate", tags=["auth"])
async def initiate_login(
    body: LoginInitiateRequest,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Initiate login by sending an OTP code to the user's email.

    No authentication required.
    """
    user_agent, ip_address = get_client_info(request)
    return auth_service.initiate_login(
        body.email,
        user_agent=user_agent,
        ip_address=ip_address,
    )


@web_router.post("/auth/login/verify", tags=["auth"])
async def verify_login(
    body: LoginVerifyRequest,
    request: Request,
    response: Response,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Verify OTP code and create session.

    Returns access token, refresh token, and user info.
    Also indicates if passkey setup is needed.
    """
    user_agent, ip_address = get_client_info(request)
    result = auth_service.verify_login(
        email=body.email,
        code=body.code,
        user_agent=user_agent,
        ip_address=ip_address,
    )
    if result.get("refresh_token"):
        set_refresh_cookie(response, result["refresh_token"])
    return {k: v for k, v in result.items() if k != "refresh_token"}


@web_router.post("/auth/refresh", tags=["auth"])
async def refresh_tokens(
    body: RefreshRequest,
    request: Request,
    response: Response,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Refresh access token using refresh token.

    Returns new access token and refresh token.
    """
    user_agent, ip_address = get_client_info(request)
    refresh_token = body.refresh_token or request.cookies.get(REFRESH_COOKIE_NAME)
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Missing refresh token")
    result = auth_service.refresh_tokens(
        refresh_token=refresh_token,
        user_agent=user_agent,
        ip_address=ip_address,
    )
    if result.get("refresh_token"):
        set_refresh_cookie(response, result["refresh_token"])
    return {k: v for k, v in result.items() if k != "refresh_token"}


@web_router.post("/auth/logout", tags=["auth"])
async def logout(
    body: Optional[LogoutRequest] = None,
    request: Request = None,
    response: Response = None,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Logout by revoking the current session.
    """
    refresh_token = (body.refresh_token if body else None) or (
        request.cookies.get(REFRESH_COOKIE_NAME) if request else None
    )
    if refresh_token:
        auth_service.logout(refresh_token)
    if response:
        clear_refresh_cookie(response)
    return {"logged_out": True}


@web_router.post("/auth/logout-all", tags=["auth"])
async def logout_all(
    user: User = Depends(get_current_web_user),
    response: Response = None,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Logout from all sessions.

    Requires authentication.
    """
    if response:
        clear_refresh_cookie(response)
    return auth_service.logout_all_sessions(user.email)


@web_router.get("/auth/sessions", tags=["auth"])
async def list_auth_sessions(
    request: Request,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    List the user's active browser sessions.

    Requires authentication.
    """
    return auth_service.list_sessions(
        user.email,
        current_refresh_token=request.cookies.get(REFRESH_COOKIE_NAME),
    )


@web_router.delete("/auth/sessions/{session_id}", tags=["auth"])
async def revoke_auth_session(
    session_id: str,
    request: Request,
    response: Response,
    user: User = Depends(get_current_web_user),
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Revoke one active browser session.

    Requires authentication.
    """
    current_refresh_token = request.cookies.get(REFRESH_COOKIE_NAME)
    current_session = auth_service.list_sessions(
        user.email,
        current_refresh_token=current_refresh_token,
    )
    result = auth_service.revoke_session_by_id(user.email, session_id)
    if any(session["id"] == session_id and session["current"] for session in current_session["sessions"]):
        clear_refresh_cookie(response)
    return result


# =============================================================================
# Registration Endpoints
# =============================================================================


@web_router.post("/auth/register/initiate", tags=["auth"])
async def initiate_registration(
    body: RegisterInitiateRequest,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Initiate registration by sending a verification email.

    Validates email domain against allowed institutional patterns.
    Sends both OTP code and magic link for verification.

    No authentication required.
    """
    user_agent, ip_address = get_client_info(request)
    return auth_service.initiate_registration(
        body.email,
        user_agent=user_agent,
        ip_address=ip_address,
    )


@web_router.post("/auth/register/verify", tags=["auth"])
async def verify_registration(
    body: RegisterVerifyRequest,
    request: Request,
    response: Response,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Complete registration by verifying OTP code or magic link token.

    Creates user account and returns session tokens.

    No authentication required.
    """
    user_agent, ip_address = get_client_info(request)
    result = auth_service.verify_registration(
        email=body.email,
        code=body.code,
        magic_token=body.magic_token,
        user_agent=user_agent,
        ip_address=ip_address,
    )
    if result.get("refresh_token"):
        set_refresh_cookie(response, result["refresh_token"])
    return {k: v for k, v in result.items() if k != "refresh_token"}


@web_router.get("/auth/oauth/{provider}/start", tags=["auth"])
async def start_oauth(
    provider: str,
    request: Request,
    next: Optional[str] = None,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    state = secrets.token_urlsafe(24)
    redirect_url = auth_service.get_oauth_authorize_url(provider, state)
    response = RedirectResponse(url=redirect_url, status_code=302)
    response.set_cookie(
        key=f"oauth_state_{provider}",
        value=state,
        httponly=True,
        secure=os.getenv("COOKIE_SECURE", "false").lower() == "true",
        samesite="lax",
        max_age=600,
        path="/",
    )
    safe_next = next if next and next.startswith("/") and not next.startswith("//") else "/datasets"
    response.set_cookie(
        key=f"oauth_next_{provider}",
        value=safe_next,
        httponly=True,
        secure=os.getenv("COOKIE_SECURE", "false").lower() == "true",
        samesite="lax",
        max_age=600,
        path="/",
    )
    return response


@web_router.get("/auth/oauth/{provider}/callback", tags=["auth"])
async def oauth_callback(
    provider: str,
    code: str,
    state: str,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    expected_state = request.cookies.get(f"oauth_state_{provider}")
    if not expected_state or expected_state != state:
        raise HTTPException(status_code=400, detail="Invalid OAuth state")

    user_agent, ip_address = get_client_info(request)
    if provider == "google":
        result = auth_service.complete_google_oauth(
            code=code,
            user_agent=user_agent,
            ip_address=ip_address,
        )
    elif provider == "github":
        result = auth_service.complete_github_oauth(
            code=code,
            user_agent=user_agent,
            ip_address=ip_address,
        )
    else:
        raise HTTPException(status_code=404, detail="Unsupported OAuth provider")

    next_target = request.cookies.get(f"oauth_next_{provider}") or "/datasets"
    if not next_target.startswith("/") or next_target.startswith("//"):
        next_target = "/datasets"

    params = {}
    if result.get("needs_passkey"):
        params["needs_passkey"] = "true"
    if result.get("verification_status"):
        params["verification_status"] = result["verification_status"]
    if result.get("verification_message"):
        params["verification_message"] = result["verification_message"]

    frontend_base = os.getenv('FRONTEND_URL', 'http://localhost:3000')
    if result.get("needs_passkey") or result.get("verification_status") == "pending":
        params["oauth"] = "success"
        params["next"] = next_target
        redirect_target = f"{frontend_base}/login"
        if params:
            redirect_target = f"{redirect_target}#{urlencode(params)}"
    else:
        redirect_target = f"{frontend_base}{next_target}"

    response = RedirectResponse(url=redirect_target, status_code=302)
    response.delete_cookie(key=f"oauth_state_{provider}", path="/")
    response.delete_cookie(key=f"oauth_next_{provider}", path="/")
    if result.get("refresh_token"):
        set_refresh_cookie(response, result["refresh_token"])
    return response


# =============================================================================
# Invitation Endpoints
# =============================================================================


@web_router.post("/auth/accept-invite", tags=["auth"])
async def accept_invitation(
    body: AcceptInvitationRequest,
    request: Request,
    response: Response,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Accept an invitation by verifying the magic link token.

    Creates a session and returns tokens for the invited user.
    Invitation links expire after 48 hours.

    No authentication required.
    """
    user_agent, ip_address = get_client_info(request)
    result = auth_service.accept_invitation(
        token=body.token,
        user_agent=user_agent,
        ip_address=ip_address,
    )
    if result.get("refresh_token"):
        set_refresh_cookie(response, result["refresh_token"])
    return {k: v for k, v in result.items() if k != "refresh_token"}


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
    body: PasskeyOptionsRequest,
    request: Request,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Get WebAuthn authentication options for passkey login.

    No authentication required.
    """
    user_agent, ip_address = get_client_info(request)
    return auth_service.get_passkey_authentication_options(
        body.email,
        user_agent=user_agent,
        ip_address=ip_address,
    )


@web_router.post("/auth/passkey/login/verify", tags=["passkey"])
async def verify_passkey_login(
    body: PasskeyAuthRequest,
    request: Request,
    response: Response,
    auth_service: WebAuthService = Depends(WebAuthService),
):
    """
    Verify passkey authentication and create session.

    Returns access token, refresh token, and user info.
    """
    user_agent, ip_address = get_client_info(request)
    result = auth_service.verify_passkey_authentication(
        email=body.email,
        credential=body.credential,
        user_agent=user_agent,
        ip_address=ip_address,
    )
    if result.get("refresh_token"):
        set_refresh_cookie(response, result["refresh_token"])
    return {k: v for k, v in result.items() if k != "refresh_token"}


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


@web_router.get("/datasets/{dataset_id}/manifest", tags=["datasets"])
async def get_dataset_manifest(
    dataset_id: str,
    user: User = Depends(get_current_web_user),
    user_service: WebUserService = Depends(WebUserService),
):
    """
    Get the canonical manifest for a standardised dataset the user can view.

    Requires authentication and dataset view access.
    """
    return user_service.get_dataset_manifest(user, dataset_id)


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


class DocumentationSyncRequest(BaseModel):
    dataset_id: Optional[str] = None
    only_outdated: bool = True
    force: bool = False


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


@web_router.get("/admin/datasets/suggest-id", tags=["web-admin/datasets"])
async def admin_suggest_dataset_id(
    collection_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Suggest the next sequential dataset ID for a collection."""
    return admin_service.suggest_next_dataset_id(user, collection_id)


@web_router.get("/admin/datasets/next-id-number", tags=["web-admin/datasets"])
async def admin_get_next_dataset_id_number(
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """The next dataset ID number, catalogue-wide (independent of collection)."""
    return admin_service.get_next_dataset_id_number(user)


@web_router.get("/admin/raw-datasets/suggest-id", tags=["web-admin/datasets"])
async def admin_suggest_raw_dataset_id(
    collection_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Suggest the next sequential raw dataset ID for a collection."""
    return admin_service.suggest_next_raw_dataset_id(user, collection_id)


@web_router.get("/admin/raw-datasets/suggest-id-by-category", tags=["web-admin/datasets"])
async def admin_suggest_raw_dataset_id_by_category(
    category_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Suggest the next raw dataset ID for a category (shared across its collections)."""
    return admin_service.suggest_next_raw_dataset_id_for_category(user, category_id)


@web_router.get("/admin/dataset-id-reservations", tags=["web-admin/datasets"])
async def admin_list_reserved_dataset_ids(
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """List reserved dataset IDs."""
    return admin_service.list_reserved_dataset_ids(user, search=search, limit=limit, offset=offset)


@web_router.post("/admin/dataset-id-reservations", tags=["web-admin/datasets"])
async def admin_reserve_dataset_id(
    body: ReserveDatasetIdRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Reserve a dataset ID for future use."""
    return admin_service.reserve_dataset_id(user, body.ds_id, body.collection_id, body.note)


@web_router.delete("/admin/dataset-id-reservations/{dataset_id}", tags=["web-admin/datasets"])
async def admin_delete_reserved_dataset_id(
    dataset_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Remove a dataset ID reservation."""
    return admin_service.delete_reserved_dataset_id(user, dataset_id)


@web_router.get("/admin/datasets/{dataset_id}", tags=["web-admin/datasets"])
async def admin_get_dataset_detail(
    dataset_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Get detailed dataset information for admin editing."""
    return admin_service.get_dataset_detail(user, dataset_id)


@web_router.post("/admin/datasets", tags=["web-admin/datasets"])
async def admin_create_dataset(
    body: DatasetCreate,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Create a dataset."""
    return admin_service.create_dataset(user, body)


@web_router.put("/admin/datasets/{dataset_id}", tags=["web-admin/datasets"])
async def admin_update_dataset(
    dataset_id: str,
    body: DatasetUpdate,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Update dataset metadata and raw dataset links."""
    return admin_service.update_dataset(user, dataset_id, body)


@web_router.put("/admin/datasets/{dataset_id}/documentation", tags=["web-admin/datasets"])
async def admin_update_dataset_documentation(
    dataset_id: str,
    body: DatasetDocumentationUpdate,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Update cached dataset README and data dictionary content."""
    return admin_service.update_dataset_documentation(user, dataset_id, body)


@web_router.post("/admin/datasets/import/preview", tags=["web-admin/datasets"])
async def admin_preview_dataset_import(
    info_file: UploadFile = File(...),
    metadata_file: UploadFile = File(...),
    csv_files: List[UploadFile] = File(default=[]),
    dataset_override_json: Optional[str] = Form(default=None),
    raw_dataset_override_json: Optional[str] = Form(default=None),
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Preview a dataset package import from info.yml, metadata.yml, and optional CSV files."""
    dataset_override = json.loads(dataset_override_json) if dataset_override_json else None
    raw_dataset_override = json.loads(raw_dataset_override_json) if raw_dataset_override_json else None
    return admin_service.preview_dataset_package_import(
        user,
        info_file,
        metadata_file,
        csv_files=csv_files,
        dataset_override=dataset_override,
        raw_dataset_override=raw_dataset_override,
    )


@web_router.post("/admin/datasets/import/apply", tags=["web-admin/datasets"])
async def admin_apply_dataset_import(
    info_file: UploadFile = File(...),
    metadata_file: UploadFile = File(...),
    csv_files: List[UploadFile] = File(...),
    dataset_override_json: Optional[str] = Form(default=None),
    raw_dataset_override_json: Optional[str] = Form(default=None),
    bucket_type: VersionType = Form(default=VersionType.STANDARDISED),
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Create a dataset from an uploaded package and upload the referenced CSV tables."""
    dataset_override = json.loads(dataset_override_json) if dataset_override_json else None
    raw_dataset_override = json.loads(raw_dataset_override_json) if raw_dataset_override_json else None
    return admin_service.import_dataset_package(
        user,
        info_file,
        metadata_file,
        csv_files=csv_files,
        dataset_override=dataset_override,
        raw_dataset_override=raw_dataset_override,
        bucket_type=bucket_type,
    )


@web_router.post("/admin/datasets/{dataset_id}/delete/initiate", tags=["web-admin/datasets"])
async def admin_initiate_dataset_deletion(
    dataset_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Send an email verification code before deleting a dataset."""
    return admin_service.initiate_dataset_deletion(user, dataset_id)


@web_router.post("/admin/datasets/{dataset_id}/delete/verify", tags=["web-admin/datasets"])
async def admin_verify_dataset_deletion(
    dataset_id: str,
    body: DatasetDeleteVerifyRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Delete a dataset after OTP verification and explicit dataset-ID confirmation."""
    return admin_service.verify_dataset_deletion(
        user,
        dataset_id,
        body.code,
        body.confirmation_dataset_id,
    )


@web_router.get("/admin/raw-datasets", tags=["web-admin/datasets"])
async def admin_list_raw_datasets(
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """List raw datasets for admin editing."""
    return admin_service.list_raw_datasets(user, search=search, limit=limit, offset=offset)


@web_router.post("/admin/raw-datasets", tags=["web-admin/datasets"])
async def admin_create_raw_dataset(
    body: RawDatasetCreate,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Create a raw dataset."""
    return admin_service.create_raw_dataset(user, body)


@web_router.put("/admin/raw-datasets/{raw_dataset_id}", tags=["web-admin/datasets"])
async def admin_update_raw_dataset(
    raw_dataset_id: str,
    body: RawDatasetUpdate,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Update a raw dataset."""
    return admin_service.update_raw_dataset(user, raw_dataset_id, body)


@web_router.get("/admin/datasets/{dataset_id}/{bucket_type}/tables", tags=["web-admin/datasets"])
async def admin_list_dataset_tables(
    dataset_id: str,
    bucket_type: VersionType,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """List tables stored for a dataset/version."""
    return admin_service.list_dataset_tables(user, dataset_id, bucket_type)


@web_router.post("/admin/datasets/{dataset_id}/{bucket_type}/tables", tags=["web-admin/datasets"])
async def admin_create_dataset_table(
    dataset_id: str,
    bucket_type: VersionType,
    file: UploadFile = File(...),
    table_metadata_file: UploadFile = File(...),
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Upload a new table for a dataset/version."""
    return admin_service.create_dataset_table(
        user,
        dataset_id,
        bucket_type,
        file,
        table_metadata_file,
    )


@web_router.get("/admin/datasets/{dataset_id}/{bucket_type}/manifest", tags=["web-admin/datasets"])
async def admin_get_dataset_manifest(
    dataset_id: str,
    bucket_type: VersionType,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Get the canonical manifest for a dataset/version."""
    return admin_service.get_dataset_manifest(user, dataset_id, bucket_type)


@web_router.put("/admin/datasets/{dataset_id}/{bucket_type}/manifest", tags=["web-admin/datasets"])
async def admin_upsert_dataset_manifest(
    dataset_id: str,
    bucket_type: VersionType,
    manifest_file: UploadFile = File(...),
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Validate and persist the canonical manifest for a dataset/version."""
    return admin_service.upsert_dataset_manifest(
        user,
        dataset_id,
        bucket_type,
        manifest_file,
    )


@web_router.post("/admin/manifest-drafts/generate", tags=["web-admin/manifest-drafts"])
async def web_generate_manifest_draft(
    csv_files: List[UploadFile] = File(...),
    category_id: str = Form(...),
    collection_id: str = Form(...),
    data_owner_name: str = Form(...),
    dataset_id: Optional[str] = Form(None),
    digitization_log_file: Optional[UploadFile] = File(None),
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.generate_manifest_draft(
        user,
        csv_files,
        category_id,
        collection_id,
        data_owner_name,
        dataset_id=dataset_id,
        digitization_log_file=digitization_log_file,
    )


@web_router.post(
    "/admin/manifest-drafts/generate-deterministic", tags=["web-admin/manifest-drafts"]
)
async def web_generate_deterministic_manifest_draft(
    csv_files: List[UploadFile] = File(...),
    category_id: str = Form(...),
    collection_id: str = Form(...),
    data_owner_name: str = Form(...),
    curator_input: str = Form(...),
    dataset_id: Optional[str] = Form(None),
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.generate_deterministic_manifest_draft(
        user,
        csv_files,
        category_id,
        collection_id,
        data_owner_name,
        json.loads(curator_input),
        dataset_id=dataset_id,
    )


@web_router.post("/admin/manifest-drafts/classify-columns", tags=["web-admin/manifest-drafts"])
async def web_classify_columns(
    body: ClassifyColumnsRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.classify_columns(user, body.column_names)


@web_router.get("/admin/manifest-drafts", tags=["web-admin/manifest-drafts"])
async def web_list_manifest_drafts(
    status: Optional[str] = None,
    dataset_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.list_manifest_drafts(user, status=status, dataset_id=dataset_id, limit=limit, offset=offset)


@web_router.get("/admin/manifest-drafts/{draft_id}", tags=["web-admin/manifest-drafts"])
async def web_get_manifest_draft(
    draft_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.get_manifest_draft(user, draft_id)


@web_router.delete("/admin/manifest-drafts/{draft_id}", tags=["web-admin/manifest-drafts"])
async def web_delete_manifest_draft(
    draft_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    admin_service.delete_manifest_draft(user, draft_id)
    return {"message": "Manifest draft deleted", "draft_id": draft_id}


@web_router.post("/admin/manifest-drafts/{draft_id}/validate", tags=["web-admin/manifest-drafts"])
async def web_revalidate_manifest_draft(
    draft_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.revalidate_manifest_draft(user, draft_id)


@web_router.post("/admin/manifest-drafts/{draft_id}/approve", tags=["web-admin/manifest-drafts"])
async def web_approve_manifest_draft(
    draft_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.approve_manifest_draft(user, draft_id)


@web_router.post("/admin/manifest-drafts/{draft_id}/reject", tags=["web-admin/manifest-drafts"])
async def web_reject_manifest_draft(
    draft_id: str,
    body: ManifestDraftReject,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.reject_manifest_draft(user, draft_id, reason=body.reason)


@web_router.put("/admin/manifest-drafts/{draft_id}", tags=["web-admin/manifest-drafts"])
async def web_update_manifest_draft(
    draft_id: str,
    body: ManifestDraftEdit,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.update_manifest_draft_content(user, draft_id, body.draft_yaml)


@web_router.post("/admin/manifest-drafts/{draft_id}/flag-field", tags=["web-admin/manifest-drafts"])
async def web_flag_manifest_draft_field(
    draft_id: str,
    body: ManifestDraftFlagField,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.flag_manifest_draft_field(user, draft_id, body.field_path, body.note)


@web_router.post("/admin/manifest-drafts/{draft_id}/regenerate", tags=["web-admin/manifest-drafts"])
async def web_regenerate_manifest_draft(
    draft_id: str,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    return admin_service.regenerate_manifest_draft(user, draft_id)


@web_router.get("/admin/documentation-sync", tags=["web-admin/datasets"])
async def admin_check_documentation_sync(
    dataset_id: Optional[str] = None,
    check_all: bool = False,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Check which datasets have documentation out of sync with filestore.
    Pass dataset_id for the interactive per-dataset check, or check_all=true
    for a summary count across every dataset (e.g. the admin overview).
    """
    return admin_service.check_dataset_documentation_sync(user, dataset_id, check_all=check_all)


@web_router.post("/admin/documentation-sync", tags=["web-admin/datasets"])
async def admin_run_documentation_sync(
    body: DocumentationSyncRequest,
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Sync dataset documentation from filestore into cached database fields."""
    return admin_service.sync_dataset_documentation(
        user,
        dataset_id=body.dataset_id,
        only_outdated=body.only_outdated,
        force=body.force,
    )


@web_router.post("/admin/validate/tabular", tags=["web-admin/validate"])
async def admin_validate_tabular(
    manifest_file: UploadFile = File(...),
    table_file: UploadFile | None = File(None),
    table_name: str | None = Form(None),
    deep_check: bool = Form(False),
    extra_column_policy: str = Form("warn"),
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Validate a tabular manifest and optional data file."""
    return admin_service.validate_dataset(
        user,
        dataset_kind=DatasetKind.TABULAR,
        manifest_file=manifest_file,
        data_file=table_file,
        table_name=table_name,
        deep_check=deep_check,
        extra_column_policy=extra_column_policy,
    )


@web_router.post("/admin/validate/geojson", tags=["web-admin/validate"])
async def admin_validate_geojson(
    manifest_file: UploadFile = File(...),
    geojson_file: UploadFile | None = File(None),
    deep_check: bool = Form(False),
    user: User = Depends(get_current_web_user),
    admin_service: WebAdminService = Depends(WebAdminService),
):
    """Validate a GeoJSON manifest and optional GeoJSON payload."""
    return admin_service.validate_dataset(
        user,
        dataset_kind=DatasetKind.GEOJSON,
        manifest_file=manifest_file,
        data_file=geojson_file,
        deep_check=deep_check,
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

    logger.info(f"Starting chat stream with provider: {body.provider}")

    async def generate():
        try:
            event_count = 0
            async for event in chat_service.chat_stream(
                user_message=body.message,
                conversation_history=history,
                user_email=user.email,
                session_id=body.session_id,
            ):
                event_count += 1
                logger.debug(f"Router yielding event #{event_count}: {event.get('type')}")
                yield f"data: {json.dumps(event)}\n\n"
            logger.info(f"Chat stream completed, yielded {event_count} events")
        except Exception as e:
            logger.exception(f"Error in chat stream generate: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

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

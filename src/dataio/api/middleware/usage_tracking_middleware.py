import logging
import time
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from ..services.usage_tracking_service import UsageTrackingService


class UsageTrackingMiddleware(BaseHTTPMiddleware):
    """Middleware to track API usage and log to SQLite database."""

    def __init__(self, app, db_path: str = "usage_tracking.db"):
        super().__init__(app)
        self.usage_service = UsageTrackingService(db_path)
        self.logger = logging.getLogger(__name__)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        # Extract request information
        endpoint = str(request.url.path)
        http_method = request.method
        user_agent = request.headers.get("user-agent", "")
        ip_address = request.client.host if request.client else ""

        # Skip tracking for certain endpoints
        if self._should_skip_tracking(endpoint):
            return await call_next(request)

        # Determine method type based on user agent
        method_type = self._determine_method_type(user_agent)

        # Extract user information from request (if available)
        user_email = self._extract_user_from_request(request)

        # Extract dataset/region information from path
        dataset_id, bucket_type, region_id = self._extract_resource_info(endpoint)

        # Process the request
        response = await call_next(request)

        # Log the usage
        self.usage_service.log_usage(
            user_email=user_email,
            method_type=method_type,
            endpoint=endpoint,
            http_method=http_method,
            dataset_id=dataset_id,
            bucket_type=bucket_type,
            region_id=region_id,
            user_agent=user_agent,
            ip_address=ip_address,
            response_status=response.status_code,
        )

        return response

    def _should_skip_tracking(self, endpoint: str) -> bool:
        """Determine if we should skip tracking for certain endpoints."""
        skip_endpoints = [
            "/docs",
            "/redoc",
            "/openapi.json",
            "/favicon.ico",
            "/health",
            "/metrics",
        ]

        # Skip static files and documentation
        for skip_endpoint in skip_endpoints:
            if endpoint.startswith(skip_endpoint):
                return True

        # Skip admin usage endpoints to avoid recursive logging
        if "/admin/usage/" in endpoint:
            return True

        return False

    def _determine_method_type(self, user_agent: str) -> str:
        """Determine if request is from API, SDK, or CLI based on user agent."""
        user_agent_lower = user_agent.lower()

        if "dataio" in user_agent_lower and "cli" in user_agent_lower:
            return "CLI"
        elif "python" in user_agent_lower or "requests" in user_agent_lower:
            return "SDK"
        else:
            return "API"

    def _extract_user_from_request(self, request: Request) -> str:
        """Extract user email from request headers or state."""
        # Try to get user from request state (set by auth middleware)
        if hasattr(request.state, "user") and request.state.user:
            return request.state.user.email

        # Try to get from headers (fallback)
        api_key = request.headers.get("X-API-Key")
        if api_key:
            # We could decode the API key to get user, but for now return "unknown"
            return "unknown_user"

        return "anonymous"

    def _extract_resource_info(self, endpoint: str) -> tuple:
        """Extract dataset_id, bucket_type, and region_id from endpoint."""
        dataset_id = None
        bucket_type = None
        region_id = None

        # Parse dataset endpoints: /api/v1/datasets/{dataset_id}/{bucket_type}/tables
        if "/datasets/" in endpoint and "/tables" in endpoint:
            parts = endpoint.split("/")
            try:
                dataset_id = parts[3] if len(parts) > 3 else None
                bucket_type = parts[4] if len(parts) > 4 else None
            except (IndexError, ValueError):
                pass

        # Parse shapefile endpoints: /api/v1/shapefiles/{region_id}
        elif "/shapefiles/" in endpoint:
            parts = endpoint.split("/")
            try:
                region_id = parts[3] if len(parts) > 3 else None
            except (IndexError, ValueError):
                pass

        # Parse regions endpoints: /api/v1/regions/{region_id}/children
        elif "/regions/" in endpoint and "/children" in endpoint:
            parts = endpoint.split("/")
            try:
                region_id = parts[3] if len(parts) > 3 else None
            except (IndexError, ValueError):
                pass

        return dataset_id, bucket_type, region_id

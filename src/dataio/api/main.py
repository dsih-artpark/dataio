import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from dataio.api.routers.admin import admin_router
from dataio.api.routers.user import user_router
from dataio.api.routers.validate import validate_router
from dataio.api.routers.web import web_router
from dataio.api.auth.providers import API_KEY_PREFIX

# Set up logging
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO, format=log_format, filename="api.log", filemode="a"
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="DataIO API",
    docs_url="/api/v1",
    openapi_url="/api/v1/openapi.json",
    redoc_url=None,
)


class LegacyAPIKeyDeprecationMiddleware(BaseHTTPMiddleware):
    """
    Middleware to add deprecation warning headers when legacy API keys are used.

    Legacy API keys (without the 'dio_' prefix) are deprecated and will be removed
    in a future version. This middleware adds standard HTTP deprecation headers
    to responses when a legacy key is detected.
    """

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Check if a legacy API key was used (key without dio_ prefix)
        api_key = request.headers.get("X-API-Key", "")
        if api_key and not api_key.startswith(API_KEY_PREFIX):
            # Add deprecation headers per RFC 8594
            response.headers["Deprecation"] = "true"
            response.headers["Sunset"] = "2025-12-31T23:59:59Z"
            response.headers["X-Deprecation-Notice"] = (
                "Legacy API keys are deprecated. "
                "Please generate a new API key at https://data.artpark.ai/account"
            )

        return response


# Add deprecation middleware (must be added before CORS middleware)
app.add_middleware(LegacyAPIKeyDeprecationMiddleware)

# CORS configuration for web frontend
# Configurable via environment variable, comma-separated list of origins
CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:3000,http://localhost:4321"  # Default for local dev
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Deprecation", "Sunset", "X-Deprecation-Notice"],
)

# Include routers
app.include_router(user_router)
app.include_router(admin_router)
app.include_router(web_router)
app.include_router(validate_router)


# Global exception handler to ensure all errors return JSON
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred. Please try again."},
    )

if os.path.isdir("docs/build/"):
    app.mount("/docs", StaticFiles(directory="docs/build/", html=True), name="docs")


@app.get("/api")
@app.get("/api/")
async def redirect_api_to_v1():
    return RedirectResponse(url="/api/v1", status_code=301)


@app.get("/")
async def redirect_root_to_docs():
    return RedirectResponse(url="/docs", status_code=301)

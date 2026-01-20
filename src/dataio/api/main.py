import os
import logging

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from dataio.api.routers.admin import admin_router
from dataio.api.routers.user import user_router
from dataio.api.routers.web import web_router

# Set up logging
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO, format=log_format, filename="api.log", filemode="a"
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dataset Management System API",
    docs_url="/api/v1",
    openapi_url="/api/v1/openapi.json",
    redoc_url=None,
    servers=[
        {"url": "https://data.artpark.ai", "description": "Production"},
        {"url": "https://staging.data.artpark.ai", "description": "Staging"},
        {"url": "http://localhost:8000", "description": "Local development"},
    ],
)

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
)

# Include routers
app.include_router(user_router)
app.include_router(admin_router)
app.include_router(web_router)

app.mount("/docs", StaticFiles(directory="docs/build/", html=True), name="docs")


@app.get("/api")
async def redirect_api_to_v1():
    return RedirectResponse(url="/api/v1", status_code=301)


@app.get("/")
async def redirect_root_to_docs():
    return RedirectResponse(url="/docs", status_code=301)

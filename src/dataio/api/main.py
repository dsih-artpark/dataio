import logging

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from dataio.api.routers.admin import admin_router
from dataio.api.routers.user import user_router

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
)

app.include_router(user_router)
app.include_router(admin_router)

app.mount("/docs", StaticFiles(directory="docs/build/", html=True), name="docs")


@app.get("/api")
async def redirect_api_to_v1():
    return RedirectResponse(url="/api/v1", status_code=301)


@app.get("/")
async def redirect_root_to_docs():
    return RedirectResponse(url="/docs", status_code=301)

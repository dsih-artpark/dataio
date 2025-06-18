from fastapi import FastAPI
import logging
from dataio.api.api.routers.datasets import dataset_router
from dataio.api.api.routers.users import user_router

# Set up logging
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, filename="api.log", filemode="a")
logger = logging.getLogger(__name__)

app = FastAPI(title="Dataset Management System API")

app.include_router(dataset_router)
app.include_router(user_router)

@app.get("/")
async def root():
    return {"message": "Welcome to Dataset Management System API"}

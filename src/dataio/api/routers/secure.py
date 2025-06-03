from fastapi import APIRouter, Depends
from dataio.api.api.auth import get_user

router = APIRouter()

@router.get("/")
async def secure_endpoint(user: dict = Depends(get_user)):
    return user
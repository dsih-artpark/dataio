from fastapi import Security, HTTPException, status, Depends
from fastapi.security import APIKeyHeader
from dataio.api.database.functions import check_api_key, get_user_from_api_key
from fastapi.security import OAuth2PasswordBearer
from dataio.api.api.models import User

from typing import Annotated

api_key_header = APIKeyHeader(name="X-API-Key")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_user(api_key_header: str = Security(api_key_header)):
    if check_api_key(api_key_header):
        user = get_user_from_api_key(api_key_header)
        return user
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing or invalid API key"
    )

def fake_decode_token(token):
    return User(
        email="test@test.com",
        is_group=False
    )

def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    return fake_decode_token(token)

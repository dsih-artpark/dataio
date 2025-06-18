from fastapi import HTTPException, Depends, APIRouter
import logging
from dataio.api import database
from dataio.api.api.models import User, UserCreate
from dataio.api.api.auth import get_user
import sqlalchemy.exc

logger = logging.getLogger(__name__)

user_router = APIRouter(
    prefix = "/api/v1/users",
    tags = ["users"]
)

###
### USER MANAGEMENT ENDPOINTS
###

@user_router.post("/")
async def create_user(user_to_be_created: UserCreate, logged_in_user: User = Depends(get_user)):
    if not database.check_if_admin(logged_in_user):
        raise HTTPException(status_code=403, detail="You are not authorized to create a user")
    try:
        created_user = database.create_user(user_to_be_created)
        return created_user
    except sqlalchemy.exc.IntegrityError:
        raise HTTPException(status_code=400, detail="Error creating user. User already exists")
    except Exception as e:
        logger.error(f"Failed to create user: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create user. Contact support.")

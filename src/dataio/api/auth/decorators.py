from functools import wraps
from typing import Callable, Any
from fastapi import Depends, HTTPException, status
from dataio.api.database.models import AccessLevel, User
from .providers import get_user
from .permissions import require_admin, require_permission
from .exceptions import AuthorizationError


def admin_required(func: Callable) -> Callable:
    """
    Decorator to require admin privileges for a route.

    Usage:
        @admin_required
        @router.post("/admin-endpoint")
        async def admin_endpoint(user: User = Depends(get_user)):
            # This will only execute if user is admin
            pass
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Extract user from kwargs (assuming it's passed as dependency)
        user = None
        print(kwargs.items())
        for key, value in kwargs.items():
            if isinstance(value, User):
                print("yay")
                user = value
                break

        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin access required"
            )

        try:
            require_admin(user)
        except AuthorizationError as e:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))

        return await func(*args, **kwargs)

    return wrapper

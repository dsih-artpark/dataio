from functools import wraps
from typing import Callable
from dataio.api.database.models import User
from dataio.api.auth.permissions import require_admin
from dataio.api.auth.exceptions import AuthenticationError


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
        for _, value in kwargs.items():
            if isinstance(value, User):
                user = value
                break

        if user is None:
            raise AuthenticationError("Admin access required")

        require_admin(user)

        return await func(*args, **kwargs)

    return wrapper

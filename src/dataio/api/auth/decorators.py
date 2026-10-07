import inspect
from functools import wraps
from typing import Callable

from starlette.concurrency import run_in_threadpool

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

        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        # Plain `def` routes do blocking work (LLM calls, CSV profiling, full
        # validation); the wrapper itself is async, so run them in the
        # threadpool rather than on the event loop.
        return await run_in_threadpool(func, *args, **kwargs)

    return wrapper

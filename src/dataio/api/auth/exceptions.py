from fastapi import HTTPException, status


class AuthError(Exception):
    """Base exception for authentication and authorization errors."""

    pass


class AuthenticationError(AuthError):
    """Raised when authentication fails."""

    def __init__(self, message: str = "Authentication failed"):
        self.message = message
        super().__init__(self.message)

    def to_http_exception(self) -> HTTPException:
        """Convert to FastAPI HTTPException."""
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=self.message,
            headers={"WWW-Authenticate": "Bearer"},
        )


class AuthorizationError(AuthError):
    """Raised when authorization fails."""

    def __init__(self, message: str = "Insufficient permissions"):
        self.message = message
        super().__init__(self.message)

    def to_http_exception(self) -> HTTPException:
        """Convert to FastAPI HTTPException."""
        return HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=self.message)

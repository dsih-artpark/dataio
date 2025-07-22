from fastapi import HTTPException, status


class AuthenticationError(HTTPException):
    """Raised when authentication fails."""

    def __init__(self, message: str = "Authentication failed"):
        self.message = message
        self.status_code = status.HTTP_401_UNAUTHORIZED
        super().__init__(status_code=self.status_code, detail=self.message)


class AuthorizationError(HTTPException):
    """Raised when authorization fails."""

    def __init__(self, message: str = "Insufficient permissions"):
        self.message = message
        self.status_code = status.HTTP_403_FORBIDDEN
        super().__init__(status_code=self.status_code, detail=self.message)

import logging
from typing import Any

class BaseService:
    """Base service class with common functionality."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _handle_error(self, error: Exception, message: str) -> None:
        """Log error and re-raise it."""
        self.logger.error(f"{message}: {str(error)}")
        raise error
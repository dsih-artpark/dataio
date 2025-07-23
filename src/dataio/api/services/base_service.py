import logging


class BaseService:
    """Base service class with common functionality."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
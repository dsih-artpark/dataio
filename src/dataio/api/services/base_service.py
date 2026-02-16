import logging
import os
import warnings

_aws_key_warning_shown = False


def get_aws_access_key_id() -> str | None:
    """
    Get AWS access key ID with backward compatibility.

    Checks AWS_ACCESS_KEY_ID first (standard), falls back to AWS_ACCESS_KEY
    (deprecated) with a warning.
    """
    global _aws_key_warning_shown

    key_id = os.getenv("AWS_ACCESS_KEY_ID")
    if key_id:
        return key_id

    # Fallback to deprecated env var
    legacy_key = os.getenv("AWS_ACCESS_KEY")
    if legacy_key:
        if not _aws_key_warning_shown:
            warnings.warn(
                "AWS_ACCESS_KEY is deprecated. Use AWS_ACCESS_KEY_ID instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            logging.getLogger(__name__).warning(
                "AWS_ACCESS_KEY is deprecated. Please update to AWS_ACCESS_KEY_ID."
            )
            _aws_key_warning_shown = True
        return legacy_key

    return None


class BaseService:
    """Base service class with common functionality."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
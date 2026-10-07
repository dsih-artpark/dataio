from importlib.metadata import PackageNotFoundError, version

from dataio.sdk import DataIOAPI
from dataio.validate import DataIOValidator

__all__ = ["DataIOAPI", "DataIOValidator"]

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    try:
        __version__ = version("dataio-artpark")
    except PackageNotFoundError:
        __version__ = "dev"

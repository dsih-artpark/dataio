from importlib.metadata import PackageNotFoundError, version

from dataio.sdk import DataIOAPI

__all__ = ["DataIOAPI"]

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    try:
        __version__ = version("dataio-artpark")
    except PackageNotFoundError:
        __version__ = "dev"

import dataio.download  # noqa: F401
import dataio.upload  # noqa: F401
import importlib.metadata
import logging


__version__ = importlib.metadata.version("dataio")

# Configure global logging settings
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)
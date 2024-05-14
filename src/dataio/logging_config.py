# mypackage/logging_config.py
import logging
import logging.config

def setup_logging():
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            },
            'file': {
                'class': 'logging.FileHandler',
                'formatter': 'standard',
                'filename': 'dataio.log',
            },
        },
        'loggers': {
            '': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'mypackage': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False,
            },
        }
    }
    
    logging.config.dictConfig(logging_config)

# Optional: Set up a specific logger for the package
logger = logging.getLogger('dataio')
logger.setLevel(logging.INFO)

def set_logging_level(level):
    """
    Set the logging level for the 'mypackage' logger.

    Args:
        level (str): The logging level to set ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
    """
    logger = logging.getLogger('dataio')
    level = level.upper()
    if level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        logger.setLevel(getattr(logging, level))
    else:
        raise ValueError(f"Invalid logging level: {level}")
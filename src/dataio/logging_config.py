# dataio/logging_config.py
import logging
import logging.config

def setup_logging(default_level='WARNING'):
    """
    Set up logging configuration.

    Args:
        default_level (str): The default logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
    """
    default_level = default_level.upper()
    if default_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        raise ValueError(f"Invalid logging level: {default_level}")

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
                'level': default_level,
                'propagate': True,
            },
            'dataio': {
                'handlers': ['console', 'file'],
                'level': default_level,
                'propagate': False,
            },
        }
    }

    logging.config.dictConfig(logging_config)

def set_logging_level(level):
    """
    Set the logging level for the 'dataio' logger and all its handlers.

    Args:
        level (str): The logging level to set ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
    """
    level = level.upper()
    if level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        raise ValueError(f"Invalid logging level: {level}")

    logger = logging.getLogger('dataio')
    logger.setLevel(level)

    for handler in logger.handlers:
        handler.setLevel(level)

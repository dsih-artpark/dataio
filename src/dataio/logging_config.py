# dataio/logging_config.py
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
            'dataio': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
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
    if level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        logger = logging.getLogger('dataio')
        logger.setLevel(getattr(logging, level))
        for handler in logger.handlers:
            handler.setLevel(getattr(logging, level))
        
        # Also update the root logger to ensure consistency
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, level))
        for handler in root_logger.handlers:
            handler.setLevel(getattr(logging, level))
    else:
        raise ValueError(f"Invalid logging level: {level}")

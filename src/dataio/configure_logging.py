import logging

def logging_configure(log_level=logging.INFO, log_file=None):
    """
    Configures logging for the application.

    Arguments:
        log_level (int): The logging level (e.g., logging.INFO, logging.DEBUG).
        log_file (str, optional): The file path to output logs. If None, logs will be output to the console.
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    if log_file:
        logging.basicConfig(level=log_level, format=log_format, filename=log_file, filemode='w')
    else:
        logging.basicConfig(level=log_level, format=log_format)

    logger = logging.getLogger(__name__)
    logger.info("Logging is configured.")

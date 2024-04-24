import logging

def configure_logger():
    # Set the log levels for specific loggers to WARNING
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    
    # Configure the root logger with a custom log format
    logging.basicConfig(level=logging.DEBUG, format='%(message)s')

    # # Create a FileHandler and add it to the root logger
    # file_handler = logging.FileHandler('log_file.log')
    # file_handler.setLevel(logging.DEBUG)  # You can set the level for the file handler as needed
    # file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    # logging.getLogger().addHandler(file_handler)
    logger = logging.getLogger("SUPER")
    return logger

logger = configure_logger()

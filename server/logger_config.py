import logging

def configure_logger():
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    
    # Configure the root logger with a custom log format
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s: %(message)s'  # This format excludes the logger name
        )
    
    # Create a FileHandler and add it to the root logger
    file_handler = logging.FileHandler('log_file.log')
    file_handler.setLevel(logging.DEBUG)  # You can set the level for the file handler as needed
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)

    logger = logging.getLogger(__name__)
    return logger

logger = configure_logger()

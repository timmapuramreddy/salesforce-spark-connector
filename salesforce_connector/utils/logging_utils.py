import logging

def setup_logging() -> logging.Logger:
    """Set up logging configuration."""
    logger = logging.getLogger('ScalableSalesforceConnector')
    logger.setLevel(logging.INFO)
    
    # Avoid adding handlers if they already exist
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger 
import logging
import os


def get_logger(name: str = "databricks") -> logging.Logger:
    """
    Returns a configured logger instance.

    Usage:
        from configs.setup_logging import get_logger
        logger = get_logger()
        logger.info("Hello!")
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, log_level, logging.INFO))

        handler = logging.StreamHandler()
        handler.setLevel(getattr(logging, log_level, logging.INFO))

        formatter = logging.Formatter(
            fmt="%(asctime)s  [%(levelname)s]  %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

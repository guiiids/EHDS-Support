"""
TeamSupport Archive Viewer - Logging Configuration

Provides structured logging for usage monitoring and error tracking.
Logs are written to:
  - Console (development)
  - logs/app.log (general application logs)
  - logs/error.log (errors only)
  - logs/access.log (request/usage tracking)
"""

import logging
import os
import sys
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path

# === Constants ===
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5  # Keep 5 backup files

# Log format patterns
DETAILED_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
ACCESS_FORMAT = "%(asctime)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(app=None, log_level=None):
    """
    Initialize application logging with rotating file handlers.
    
    Args:
        app: Flask application instance (optional, for Flask integration)
        log_level: Logging level (default: INFO, or DEBUG if FLASK_DEBUG=1)
    
    Returns:
        logging.Logger: Configured application logger
    """
    # Create logs directory if it doesn't exist
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Determine log level
    if log_level is None:
        debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
        log_level = logging.DEBUG if debug_mode else logging.INFO
    
    # === Application Logger (main.py and general app logs) ===
    app_logger = logging.getLogger("teamsupport")
    app_logger.setLevel(log_level)
    app_logger.handlers.clear()  # Prevent duplicate handlers on reload
    
    # Console handler (always show INFO+ in console)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(DETAILED_FORMAT, DATE_FORMAT))
    app_logger.addHandler(console_handler)
    
    # File handler for general logs (rotating)
    app_file_handler = RotatingFileHandler(
        LOG_DIR / "app.log",
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding="utf-8"
    )
    app_file_handler.setLevel(log_level)
    app_file_handler.setFormatter(logging.Formatter(DETAILED_FORMAT, DATE_FORMAT))
    app_logger.addHandler(app_file_handler)
    
    # === Error Logger (errors only, separate file) ===
    error_handler = RotatingFileHandler(
        LOG_DIR / "error.log",
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding="utf-8"
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(DETAILED_FORMAT, DATE_FORMAT))
    app_logger.addHandler(error_handler)
    
    # === Access Logger (request tracking) ===
    access_logger = logging.getLogger("teamsupport.access")
    access_logger.setLevel(logging.INFO)
    access_logger.handlers.clear()
    access_logger.propagate = False  # Don't send to parent logger
    
    access_file_handler = RotatingFileHandler(
        LOG_DIR / "access.log",
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding="utf-8"
    )
    access_file_handler.setFormatter(logging.Formatter(ACCESS_FORMAT, DATE_FORMAT))
    access_logger.addHandler(access_file_handler)
    
    # === Flask Integration ===
    if app is not None:
        # Attach loggers to Flask app
        app.logger.handlers = app_logger.handlers
        app.logger.setLevel(log_level)
        
        # Configure Werkzeug logging (HTTP server logs)
        werkzeug_logger = logging.getLogger("werkzeug")
        werkzeug_logger.setLevel(logging.WARNING)  # Reduce noise
    
    app_logger.info("=" * 60)
    app_logger.info("TeamSupport Archive Viewer - Logging initialized")
    app_logger.info(f"Log directory: {LOG_DIR}")
    app_logger.info(f"Log level: {logging.getLevelName(log_level)}")
    app_logger.info("=" * 60)
    
    return app_logger


def get_logger(name: str = "teamsupport") -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Logger name (use __name__ for module-specific logging)
    
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)


def get_access_logger() -> logging.Logger:
    """Get the access/request logger."""
    return logging.getLogger("teamsupport.access")


class RequestLogger:
    """
    Flask middleware for logging HTTP requests with timing.
    
    Usage:
        request_logger = RequestLogger(app)
    """
    
    def __init__(self, app=None):
        self.app = app
        self.logger = get_access_logger()
        self.app_logger = get_logger()
        
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize request logging for Flask app."""
        from flask import g, request
        
        @app.before_request
        def log_request_start():
            """Record request start time."""
            g.request_start_time = datetime.now()
        
        @app.after_request
        def log_request_end(response):
            """Log completed request with timing."""
            # Calculate request duration
            duration_ms = 0
            if hasattr(g, 'request_start_time'):
                duration = datetime.now() - g.request_start_time
                duration_ms = duration.total_seconds() * 1000
            
            # Build log message
            client_ip = request.remote_addr or "-"
            method = request.method
            path = request.path
            query_string = request.query_string.decode("utf-8", errors="replace")
            if query_string:
                path = f"{path}?{query_string}"
            
            status_code = response.status_code
            content_length = response.content_length or 0
            user_agent = request.headers.get("User-Agent", "-")[:100]  # Truncate
            
            # Format: IP | METHOD | PATH | STATUS | DURATION | SIZE | USER_AGENT
            log_msg = (
                f"{client_ip} | {method} | {path} | "
                f"{status_code} | {duration_ms:.0f}ms | {content_length}B | {user_agent}"
            )
            
            # Log level based on status code
            if status_code >= 500:
                self.logger.error(log_msg)
            elif status_code >= 400:
                self.logger.warning(log_msg)
            else:
                self.logger.info(log_msg)
            
            return response
        
        @app.errorhandler(Exception)
        def log_exception(error):
            """Log unhandled exceptions and return appropriate error response."""
            from werkzeug.exceptions import HTTPException, InternalServerError
            import traceback
            
            # If it's a standard HTTP exception (e.g., 404, 403, 401),
            # just return it and let Flask handle the response.
            # These are already logged as warnings in log_request_end.
            if isinstance(error, HTTPException):
                return error
            
            # For truly unhandled exceptions (500), log the detail and traceback
            self.app_logger.error(
                f"Unhandled exception on {request.method} {request.path}: {error}"
            )
            self.app_logger.error(traceback.format_exc())
            
            # In debug mode, re-raise to show the Flask debugger
            if app.debug:
                raise error
                
            # Otherwise return a clean 500 error
            return InternalServerError()


def log_function_call(func):
    """
    Decorator to log function calls with timing.
    
    Usage:
        @log_function_call
        def my_function():
            ...
    """
    logger = get_logger()
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        logger.debug(f"Calling {func_name}")
        
        start_time = datetime.now()
        try:
            result = func(*args, **kwargs)
            duration = (datetime.now() - start_time).total_seconds() * 1000
            logger.debug(f"{func_name} completed in {duration:.1f}ms")
            return result
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds() * 1000
            logger.error(f"{func_name} failed after {duration:.1f}ms: {e}")
            raise
    
    return wrapper


# === Convenience logging functions ===

def log_info(message: str, **kwargs):
    """Log an info message with optional context."""
    logger = get_logger()
    if kwargs:
        context = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        message = f"{message} | {context}"
    logger.info(message)


def log_warning(message: str, **kwargs):
    """Log a warning message with optional context."""
    logger = get_logger()
    if kwargs:
        context = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        message = f"{message} | {context}"
    logger.warning(message)


def log_error(message: str, exc_info: bool = False, **kwargs):
    """Log an error message with optional exception traceback."""
    logger = get_logger()
    if kwargs:
        context = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        message = f"{message} | {context}"
    logger.error(message, exc_info=exc_info)


def log_debug(message: str, **kwargs):
    """Log a debug message with optional context."""
    logger = get_logger()
    if kwargs:
        context = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        message = f"{message} | {context}"
    logger.debug(message)

"""
Logger Setup Script
File: utils/utils_logger.py

This script provides logging functions for the project. 
Logging is an essential way to track events and issues during execution. 

Features:
- Logs information, warnings, and errors to a designated log file.
- Ensures the log directory exists.
- Sanitizes logs to remove personal/identifying information for GitHub sharing.
"""

# Imports from Python Standard Library
import pathlib
import os
import getpass

# Imports from external packages
from loguru import logger

# Get this file name without the extension
CURRENT_SCRIPT = pathlib.Path(__file__).stem

# Set directory where logs will be stored
LOG_FOLDER: pathlib.Path = pathlib.Path("logs")

# Set the name of the log file
LOG_FILE: pathlib.Path = LOG_FOLDER.joinpath("project_log.log")

# Sanitization function to remove identifying info
def sanitize_message(record):
    """Remove personal/identifying information from log messages."""
    message = record["message"]
    
    # Replace username with generic placeholder
    current_user = getpass.getuser()
    message = message.replace(current_user, "USER")
    
    # Replace home directory paths
    home_path = str(pathlib.Path.home())
    message = message.replace(home_path, "~")
    
    # Replace absolute paths with relative ones
    cwd = str(pathlib.Path.cwd())
    message = message.replace(cwd, "PROJECT_ROOT")
    
    # Replace Windows paths with forward slashes for consistency
    message = message.replace("\\", "/")
    
    # Update the record
    record["message"] = message
    return message

# Custom format that uses sanitized messages
def format_sanitized(record):
    """Custom formatter that sanitizes messages."""
    sanitize_message(record)
    return "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}\n"

# Ensure the log folder exists or create it
try:
    LOG_FOLDER.mkdir(exist_ok=True)
    logger.info(f"Log folder created at: {LOG_FOLDER}")
except Exception as e:
    logger.error(f"Error creating log folder: {e}")

# Configure Loguru to write to the log file with sanitization
try:
    logger.add(
        LOG_FILE, 
        level="INFO",
        rotation="50 kB",      # Small files
        retention=0,           # Keep only current log
        compression=None,      # No compression needed
        format=format_sanitized,  # Use sanitized format
        filter=lambda record: sanitize_message(record) or True  # Sanitize before writing
    )
    logger.info(f"Logging to file: {LOG_FILE}")
    logger.info("Log sanitization enabled, personal info will be removed")
except Exception as e:
    logger.error(f"Error configuring logger to write to file: {e}")


def get_log_file_path() -> pathlib.Path:
    """Return the path to the log file."""
    return LOG_FILE


def log_example() -> None:
    """Example logging function to demonstrate logging behavior."""
    try:
        logger.info("This is an example info message.")
        logger.info(f"Current working directory: {pathlib.Path.cwd()}")
        logger.info(f"User home directory: {pathlib.Path.home()}")
        logger.warning("This is an example warning message.")
        logger.error("This is an example error message.")
    except Exception as e:
        logger.error(f"An error occurred during logging: {e}")


def main() -> None:
    """Main function to execute logger setup and demonstrate its usage."""
    logger.info(f"STARTING {CURRENT_SCRIPT}.py")

    # Call the example logging function
    log_example()

    logger.info(f"View the log output at {LOG_FILE}")
    logger.info(f"EXITING {CURRENT_SCRIPT}.py.")


# Conditional execution block that calls main() only when this file is executed directly
if __name__ == "__main__":
    main()
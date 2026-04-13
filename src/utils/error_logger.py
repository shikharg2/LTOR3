"""
Centralized error logging utility.
Logs errors to per-scenario files under ./errors/<scenario_id>.txt.
Errors without a scenario context go to ./errors/general.txt.
"""

import logging
import os
import threading

ERRORS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "errors")

_initialized = False
_init_lock = threading.Lock()
_logger_lock = threading.Lock()

# Thread-local storage for current scenario_id
_thread_local = threading.local()


def init_error_logger() -> None:
    """Create the errors directory if it doesn't exist.

    Safe to call multiple times; only the first call has an effect.
    """
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        os.makedirs(ERRORS_DIR, exist_ok=True)
        _initialized = True


def set_current_scenario(scenario_id: str | None) -> None:
    """Set the current scenario_id for this thread.

    Called by the worker/scheduler before executing a scenario so that
    all log_error() calls within that thread are automatically routed
    to the correct per-scenario error file.
    """
    _thread_local.scenario_id = scenario_id


def get_current_scenario() -> str | None:
    """Get the current scenario_id for this thread."""
    return getattr(_thread_local, "scenario_id", None)


def _get_logger(scenario_id: str | None) -> logging.Logger:
    """Get or create a logger that writes to the appropriate error file."""
    init_error_logger()

    if scenario_id:
        filename = f"{scenario_id}.txt"
        logger_name = f"loadtest_error.{scenario_id}"
    else:
        filename = "general.txt"
        logger_name = "loadtest_error.general"

    logger = logging.getLogger(logger_name)
    if not logger.handlers:
        with _logger_lock:
            if not logger.handlers:
                logger.setLevel(logging.ERROR)
                log_path = os.path.join(ERRORS_DIR, filename)
                handler = logging.FileHandler(log_path, mode="a")
                formatter = logging.Formatter(
                    "%(asctime)s | %(levelname)s | %(message)s"
                )
                handler.setFormatter(formatter)
                logger.addHandler(handler)

    return logger


def log_error(module: str, function: str, error, context: str = "",
              scenario_id: str | None = None):
    """Log an error to the appropriate scenario error file.

    Args:
        module: Name of the module where the error occurred.
        function: Name of the function where the error occurred.
        error: The exception or error message.
        context: Optional additional context string.
        scenario_id: Optional explicit scenario_id. If not provided,
                     falls back to the thread-local current scenario.
    """
    sid = scenario_id or get_current_scenario()
    logger = _get_logger(sid)
    msg = f"[{module}.{function}] {type(error).__name__}: {error}"
    if context:
        msg += f" | Context: {context}"
    logger.error(msg)

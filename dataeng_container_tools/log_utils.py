"""Logging utilities.

Supports multithreading/multiproccessing.

Functions:
    log_memory_usage: Log the current memory usage of the process.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Literal

import psutil
from psutil._common import bytes2human

logging.basicConfig(level=logging.INFO)

root_logger = logging.getLogger(None)


def log_memory_usage(pid: int = os.getpid()) -> None:
    """Log the current memory usage of the process and subprocesses."""
    try:
        process = psutil.Process(pid)
        main_memory_usage = process.memory_info()
        total_rss = main_memory_usage.rss
        total_vms = main_memory_usage.vms
        root_logger.info(
            "Main Process Memory: %s",
            json.dumps(
                {i: bytes2human(j) for i, j in main_memory_usage._asdict().items()},
                indent=4,
            ),
        )

        # Iterate over all subprocesses for the main process
        for child in process.children(recursive=True):
            if child.is_running() and child.status() != psutil.STATUS_ZOMBIE:
                try:
                    mem_info = child.memory_info()
                    total_rss += mem_info.rss
                    total_vms += mem_info.vms
                    root_logger.info(
                        "PID %d, Name: %s, RSS: %s, VMS: %s",
                        child.pid,
                        child.name(),
                        bytes2human(mem_info.rss),
                        bytes2human(mem_info.vms),
                    )
                except psutil.AccessDenied:
                    root_logger.warning("Access denied for PID %d", child.pid)

        # Log combined memory usage
        root_logger.info(
            "Combined Memory Usage - Total RSS: %s, Total VMS: %s",
            bytes2human(total_rss),
            bytes2human(total_vms),
        )

    except Exception:
        root_logger.exception("Failed to track memory")


def configure_logger(
    logger_name: str,
    id_type: Literal["process", "thread", "none"] = "none",
) -> logging.Logger:
    """Generate a logger with specifications."""
    logger = logging.getLogger(logger_name)

    logger.propagate = False
    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)

    if id_type == "process":
        formatter = logging.Formatter("[%(levelname)s] %(name)s-%(process)d: %(message)s")
    elif id_type == "thread":
        formatter = logging.Formatter("[%(levelname)s] %(name)s-%(thread)d: %(message)s")
    else:
        formatter = logging.Formatter("[%(levelname)s] %(name)s: %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

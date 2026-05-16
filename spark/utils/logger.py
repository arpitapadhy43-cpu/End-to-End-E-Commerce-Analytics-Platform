import logging
import os
from datetime import datetime


class PipelineLogger:
    """
    Centralised logger for the retail pipeline.
    Usage:
        from utils.logger import PipelineLogger
        logger = PipelineLogger("silver_layer")
        logger.info("Silver orders written: 518446 rows")
        logger.warn("Null customer_ids: 107927")
        logger.error("Row count dropped below threshold")
    """

    def __init__(self, name: str, log_to_file: bool = False, log_dir: str = "/opt/spark/jobs/logs"):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Avoid adding duplicate handlers if logger already exists
        if self.logger.handlers:
            self.logger.handlers.clear()

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # ── Console handler (always on) ──────────────────────────────────────
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # ── File handler (optional) ───────────────────────────────────────────
        # Writes to /opt/spark/logs/<script_name>/<date>.log
        # Useful when running inside Docker containers via Airflow
        if log_to_file:
            log_subdir = os.path.join(log_dir, name)
            os.makedirs(log_subdir, exist_ok=True)
            log_filename = os.path.join(
                log_subdir,
                f"{datetime.utcnow().strftime('%Y%m%d')}.log"
            )
            file_handler = logging.FileHandler(log_filename)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.info(f"File logging enabled → {log_filename}")

    def info(self, message: str):
        self.logger.info(message)

    def warn(self, message: str):
        self.logger.warning(message)

    def error(self, message: str):
        self.logger.error(message)

    def debug(self, message: str):
        self.logger.debug(message)

    def section(self, title: str):
        """Prints a visible section divider — useful for separating pipeline stages."""
        divider = "=" * 60
        self.logger.info(divider)
        self.logger.info(f"  {title.upper()}")
        self.logger.info(divider)

    def log_count(self, label: str, count: int):
        """Standardised row count log."""
        self.logger.info(f"ROW COUNT | {label:<40} | {count:>10,} rows")

    def log_metric(self, check: str, expected, actual, status: str):
        """Standardised data quality metric log."""
        self.logger.info(
            f"DQ CHECK  | {check:<40} | expected={expected} actual={actual} | [{status}]"
        )

    def log_drop(self, stage: str, before: int, after: int):
        """Logs row drop between two stages with percentage."""
        dropped = before - after
        pct = round(dropped / before * 100, 2) if before > 0 else 0
        level = "WARNING" if pct > 5 else "INFO"
        msg = f"ROW DROP  | {stage:<40} | {before:,} → {after:,} | dropped={dropped:,} ({pct}%)"
        if level == "WARNING":
            self.logger.warning(msg)
        else:
            self.logger.info(msg)
import yaml
import sys
import logging
from pathlib import Path
from contextlib import contextmanager
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
CONFIG_PATH = Path("src/config.yaml")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open("r") as f:
        return yaml.safe_load(f)


def get_logger(name: str = "pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


@contextmanager
def get_db(config: dict | None = None):
    if config is None:
        config = load_config()
    con = duckdb.connect(os.getenv("WORK_DIR") + config["db"]["path"])
    try:
        yield con
    finally:
        con.close()


def chunk_list(lst: list, size: int) -> list[list]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]

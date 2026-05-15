import sys
import logging
from pathlib import Path
from contextlib import contextmanager
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
WORK_DIR = os.getenv("WORK_DIR", ".")
DB_PATH = Path(WORK_DIR) / "data" / "warehouse.db"
SEED_PATH = Path(WORK_DIR) / "data" / "warehouse_seed.db"


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


def bootstrap_from_seed():
    if DB_PATH.exists():
        return False

    if not SEED_PATH.exists():
        return False

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    import shutil

    shutil.copy2(SEED_PATH, DB_PATH)
    return True


@contextmanager
def get_db():
    if not DB_PATH.exists() and SEED_PATH.exists():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        import shutil

        shutil.copy2(SEED_PATH, DB_PATH)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    try:
        yield con
    finally:
        con.close()


def chunk_list(lst: list, size: int) -> list[list]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]

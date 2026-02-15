import yaml
import sys
import logging
from pathlib import Path

CONFIG_PATH = Path("src/config.yaml")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open("r") as f:
        return yaml.safe_load(f)


def get_logger(name: str = "pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.propagate = True

    return logger

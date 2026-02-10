import yaml
from pathlib import Path

CONFIG_PATH = Path("src/config.yaml")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open("r") as f:
        return yaml.safe_load(f)

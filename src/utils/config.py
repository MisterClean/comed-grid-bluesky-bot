import yaml
from pathlib import Path

def load_config():
    """Load configuration from config.yaml"""
    config_path = Path(__file__).parents[2] / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

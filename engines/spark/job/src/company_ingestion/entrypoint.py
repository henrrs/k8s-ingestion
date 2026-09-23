import argparse
import json
from pathlib import Path

from .config import ExecutionConfig
from .engine import run

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    return run(ExecutionConfig.from_dict(json.loads(Path(args.config).read_text())))

if __name__ == "__main__":
    main()

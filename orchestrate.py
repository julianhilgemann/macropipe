"""Pipeline orchestrator — run fetch, dbt, or full pipeline."""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def run_cmd(cmd: list[str], description: str) -> None:
    print(f"\n{'='*60}\n{description}\n{'='*60}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, check=False)
    if result.returncode != 0:
        print(f"FAILED: {description}")
        sys.exit(result.returncode)


def fetch() -> None:
    from python.fetch import ingest
    ingest()


def transform() -> None:
    run_cmd(
        ["dbt", "run", "--profiles-dir", ".", "--project-dir", "."],
        "dbt run — staging → intermediate → marts",
    )


def test() -> None:
    run_cmd(
        ["dbt", "test", "--profiles-dir", ".", "--project-dir", "."],
        "dbt test",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="macropipe orchestrator")
    parser.add_argument(
        "step",
        choices=["fetch", "transform", "test", "full"],
        help="Pipeline step to run",
    )
    args = parser.parse_args()

    if args.step in ("fetch", "full"):
        fetch()
    if args.step in ("transform", "full"):
        transform()
    if args.step in ("test", "full"):
        test()


if __name__ == "__main__":
    main()

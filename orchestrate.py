"""Pipeline orchestrator — run fetch, dbt transform, forecast, or full pipeline."""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
VENV_BIN = PROJECT_ROOT / ".venv" / "bin"


def run_cmd(cmd: list[str], description: str) -> None:
    print(f"\n{'='*60}\n{description}\n{'='*60}")
    env = {**__import__("os").environ, "PATH": f"{VENV_BIN}:{__import__('os').environ['PATH']}"}
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, check=False, env=env)
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


def forecast() -> None:
    from python.forecast import run_hl_vol_forecast
    run_hl_vol_forecast()


def test() -> None:
    run_cmd(
        ["dbt", "test", "--profiles-dir", ".", "--project-dir", "."],
        "dbt test",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="macropipe orchestrator")
    parser.add_argument(
        "step",
        choices=["fetch", "transform", "forecast", "test", "full"],
        help="Pipeline step to run",
    )
    args = parser.parse_args()

    if args.step in ("fetch", "full"):
        fetch()
    if args.step in ("transform", "full"):
        # First dbt run: build staging + intermediate (needed by forecast)
        transform()
    if args.step in ("forecast", "full"):
        forecast()
    if args.step in ("transform", "full"):
        # Second dbt run: rebuild marts to pick up forecast tables
        transform()
    if args.step in ("test", "full"):
        test()


if __name__ == "__main__":
    main()

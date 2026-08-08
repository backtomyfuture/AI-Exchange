#!/usr/bin/env python3
# ruff: noqa: E402
"""Create internal deployment state and reduce `.env` to the user contract."""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.deployment.configuration import (
    DeploymentConfigurationError,
    configure_deployment,
)  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-name",
        help="Compose 项目名；首次绿色部署请使用从未使用过的小写 slug。",
    )
    arguments = parser.parse_args(argv)
    try:
        result = configure_deployment(
            PROJECT_ROOT,
            project_name=arguments.project_name,
        )
    except DeploymentConfigurationError as exc:
        print(f"Configuration failed: {exc}")
        return 1
    print(
        "Configuration ready: "
        f"user_keys={result.user_key_count} "
        f"generated_files={result.generated_secret_count} "
        f"advanced_keys={result.advanced_key_count} "
        f"project={result.project_name}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

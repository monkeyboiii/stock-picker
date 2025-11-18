#!/usr/bin/env python3
"""Generate OpenAPI specs for all services"""

import json
import sys
from pathlib import Path

# Add services to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "services" / "trading-api"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "services" / "backtest-api"))


def generate_spec(service_name: str, app_module: str, output_path: Path):
    """Generate OpenAPI spec for a service"""
    print(f"Generating OpenAPI spec for {service_name}...")

    # Import the app
    module = __import__(app_module, fromlist=["app"])
    app = module.app

    # Get OpenAPI schema
    schema = app.openapi()

    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(schema, f, indent=2)

    print(f"✓ Generated {output_path}")
    return schema


def main():
    """Generate OpenAPI specs for all services"""
    root = Path(__file__).parent.parent.parent

    # Generate Trading API spec
    trading_spec = generate_spec(
        "Trading API",
        "app.main",
        root / "services" / "trading-api" / "openapi.json",
    )

    # Reset path for backtest-api
    sys.path.pop(0)

    # Generate Backtest API spec
    backtest_spec = generate_spec(
        "Backtest API",
        "app.main",
        root / "services" / "backtest-api" / "openapi.json",
    )

    print("\n✓ All OpenAPI specs generated successfully!")
    print(f"  Trading API: {len(trading_spec['paths'])} endpoints")
    print(f"  Backtest API: {len(backtest_spec['paths'])} endpoints")


if __name__ == "__main__":
    main()

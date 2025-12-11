#!/usr/bin/env python3
"""
Setup Nessie catalog with namespaces for the lakehouse.
"""

import os
import requests

# Configuration
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")

NAMESPACES = [
    "bronze",
    "silver",
    "gold",
]


def check_nessie_health():
    """Check if Nessie is available."""
    try:
        response = requests.get(f"{NESSIE_URI}/config", timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def get_default_branch():
    """Get the default branch (usually 'main')."""
    response = requests.get(f"{NESSIE_URI}/trees", timeout=10)
    response.raise_for_status()
    return response.json().get("defaultBranch", {}).get("name", "main")


def namespace_exists(branch: str, namespace: str) -> bool:
    """Check if a namespace exists."""
    try:
        response = requests.get(
            f"{NESSIE_URI}/namespaces/namespace/{branch}",
            params={"name": namespace},
            timeout=10,
        )
        return response.status_code == 200
    except Exception:
        return False


def create_namespace(branch: str, namespace: str):
    """Create a namespace in Nessie."""
    # Nessie namespaces are created implicitly when tables are created
    # But we can create them explicitly via the content API
    # For now, we'll just verify the setup is ready
    pass


def setup_nessie():
    """Setup Nessie catalog."""
    if not check_nessie_health():
        raise Exception("Nessie is not available")

    branch = get_default_branch()
    print(f"    Using branch: {branch}")

    # Note: Nessie namespaces are typically created implicitly
    # when the first table is created in that namespace.
    # We're just validating connectivity here.

    for namespace in NAMESPACES:
        print(f"    Namespace '{namespace}' will be created when first table is added")

    print(f"    Nessie catalog ready at {NESSIE_URI}")


if __name__ == "__main__":
    setup_nessie()

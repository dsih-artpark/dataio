"""
Minimal test utilities for permission integration testing.
"""

import csv
import os
from typing import Dict
from fastapi.testclient import TestClient
from dataio.api import app

# Initialize test client
client = TestClient(app)


def load_test_api_keys() -> Dict[str, str]:
    """Load test API keys from CSV file."""
    repo_dir = os.getenv("REPO_DIR", ".")
    csv_path = f"{repo_dir}/api/tests/test_users.csv"

    api_keys = {}
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                api_keys[row["email"]] = row["unhashed_key"]
    except FileNotFoundError:
        print(f"Warning: API keys file not found at {csv_path}")

    return api_keys


def api_call(endpoint: str, api_key: str, method: str = "GET") -> dict:
    """Make API call with given key and return response."""
    headers = {"X-API-Key": api_key}

    if method == "GET":
        response = client.get(endpoint, headers=headers)
    elif method == "POST":
        response = client.post(endpoint, headers=headers)
    else:
        raise ValueError(f"Unsupported method: {method}")

    return {
        "status_code": response.status_code,
        "data": response.json() if response.status_code != 500 else None,
        "response": response,
    }




# Test data mappings for quick reference
TEST_USERS = {
    "admin": "admin@artpark.in",
    "analyst": "analyst@artpark.in",
    "external": "external_collaborator@psu.edu",
    "public": "public_user@artpark.in",
}

TEST_DATASETS = {
    "ds0001": "TS0001DS0001",  # Public DOWNLOAD
    "ds0002": "TS0001DS0002",  # Public VIEW, analyst DOWNLOAD
    "ds0003": "TS0001DS0003",  # Public VIEW, analyst+external DOWNLOAD
    "ds0004": "TS0001DS0004",  # Public NONE, analyst+external DOWNLOAD
    "ds0005": "TS0001DS0005",  # Admin-only
    "ds0006": "TS0001DS0006",  # Resource group
    "ds0007": "TS0001DS0007",  # Resource group
}

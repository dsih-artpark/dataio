import dotenv
import os

from dataio.api.api import app
from fastapi.testclient import TestClient

dotenv.load_dotenv()
TEST_ADMIN_KEY = os.getenv("TEST_ADMIN_KEY")

client = TestClient(app)

admin_headers = {"X-API-Key": TEST_ADMIN_KEY}


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to Dataset Management System API"}


def test_get_datasets():
    response = client.get("/api/v1/datasets", headers=admin_headers)
    assert response.status_code == 200
    assert isinstance(response.json(), list)

"""
Smoke tests for DataIO.

Quick sanity checks to verify the system is working.
These tests should be fast and have minimal dependencies.
"""

import os
import sys
import pytest
from pathlib import Path


class TestPackageImports:
    """Smoke tests for package imports."""

    def test_import_main_package(self):
        """Test that main dataio package can be imported."""
        import dataio

        assert hasattr(dataio, "__version__")

    def test_import_cli_module(self):
        """Test that CLI module can be imported."""
        from dataio.cli import cli

        assert hasattr(cli, "app")

    def test_import_sdk_module(self):
        """Test that SDK module can be imported."""
        from dataio.sdk import DataIOAPI

        assert DataIOAPI is not None

    def test_import_sdk_user_module(self):
        """Test that SDK user module can be imported."""
        from dataio.sdk.user import DataIOAPI, DatasetList

        assert DataIOAPI is not None
        assert DatasetList is not None


class TestPackageStructure:
    """Smoke tests for package structure."""

    def test_package_has_version(self):
        """Test that package has version defined."""
        import dataio

        version = dataio.__version__
        assert version is not None
        assert len(version) > 0

    def test_package_directories_exist(self):
        """Test that expected package directories exist."""
        import dataio

        package_path = Path(dataio.__file__).parent

        assert (package_path / "cli").exists()
        assert (package_path / "sdk").exists()

    def test_package_init_files_exist(self):
        """Test that __init__.py files exist."""
        import dataio

        package_path = Path(dataio.__file__).parent

        assert (package_path / "__init__.py").exists()
        assert (package_path / "cli" / "__init__.py").exists()
        assert (package_path / "sdk" / "__init__.py").exists()


class TestSDKSmoke:
    """Smoke tests for SDK functionality."""

    def test_sdk_class_exists(self):
        """Test that DataIOAPI class exists and has expected methods."""
        from dataio.sdk import DataIOAPI

        # Check for expected methods
        assert hasattr(DataIOAPI, "list_datasets")
        assert hasattr(DataIOAPI, "get_dataset_details")
        assert hasattr(DataIOAPI, "download_dataset")
        assert hasattr(DataIOAPI, "list_weather_datasets")

    def test_sdk_requires_api_key(self):
        """Test that SDK properly validates API key requirement."""
        from dataio.sdk import DataIOAPI

        # Clear environment
        original_key = os.environ.pop("DATAIO_API_KEY", None)
        original_url = os.environ.pop("DATAIO_API_BASE_URL", None)

        try:
            with pytest.raises(ValueError) as exc_info:
                DataIOAPI(base_url="https://test.example.com")
            assert "API_KEY" in str(exc_info.value)
        finally:
            # Restore environment
            if original_key:
                os.environ["DATAIO_API_KEY"] = original_key
            if original_url:
                os.environ["DATAIO_API_BASE_URL"] = original_url

    def test_sdk_requires_base_url(self):
        """Test that SDK properly validates base URL requirement."""
        from dataio.sdk import DataIOAPI

        # Clear environment
        original_key = os.environ.pop("DATAIO_API_KEY", None)
        original_url = os.environ.pop("DATAIO_API_BASE_URL", None)

        try:
            with pytest.raises(ValueError) as exc_info:
                DataIOAPI(api_key="test_key")
            assert "BASE_URL" in str(exc_info.value)
        finally:
            # Restore environment
            if original_key:
                os.environ["DATAIO_API_KEY"] = original_key
            if original_url:
                os.environ["DATAIO_API_BASE_URL"] = original_url

    def test_sdk_can_instantiate_with_values(self):
        """Test that SDK can be instantiated with valid values."""
        from dataio.sdk import DataIOAPI

        # Reset warning flag
        DataIOAPI._legacy_key_warning_shown = True

        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key_12345",
        )

        assert api.base_url == "https://test.example.com/api/v1"
        assert api.session is not None


class TestCLISmoke:
    """Smoke tests for CLI functionality."""

    def test_cli_app_exists(self):
        """Test that CLI app exists."""
        from dataio.cli.cli import app

        assert app is not None

    def test_cli_help_runs(self):
        """Test that CLI help command works."""
        from dataio.cli.cli import app
        from typer.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0

    def test_user_cli_help_runs(self):
        """Test that user CLI help command works."""
        from dataio.cli.cli import app
        from typer.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(app, ["user", "--help"])

        assert result.exit_code == 0

    def test_cli_has_expected_commands(self):
        """Test that CLI has expected commands registered."""
        from dataio.cli.user import app

        command_names = [cmd.name for cmd in app.registered_commands]

        assert "init" in command_names
        assert "list-datasets" in command_names
        assert "download-dataset" in command_names


class TestAPIModelsSmoke:
    """Smoke tests for API models (if available)."""

    def test_import_api_models(self):
        """Test that API models can be imported."""
        try:
            from dataio.api.models import DatasetCreate, User, UserCreate

            assert DatasetCreate is not None
            assert User is not None
            assert UserCreate is not None
        except ImportError:
            # API module may not be installed in all environments
            pytest.skip("API module not available")

    def test_api_model_validation(self):
        """Test that API models have validation."""
        try:
            from dataio.api.models import DatasetCreate
            from pydantic import ValidationError

            # Should fail due to missing required fields
            with pytest.raises(ValidationError):
                DatasetCreate()
        except ImportError:
            pytest.skip("API module not available")


class TestEnvironmentSmoke:
    """Smoke tests for environment and configuration."""

    def test_python_version(self):
        """Test that Python version is compatible."""
        assert sys.version_info >= (3, 12), "Python 3.12+ is required"

    def test_required_packages_available(self):
        """Test that required packages are importable."""
        import requests
        import typer
        import yaml

        assert requests is not None
        assert typer is not None
        assert yaml is not None


class TestDocumentationSmoke:
    """Smoke tests for documentation."""

    def test_readme_exists(self):
        """Test that README file exists."""
        readme_path = Path(__file__).parent.parent.parent / "README.md"
        assert readme_path.exists(), "README.md should exist in project root"

    def test_license_exists(self):
        """Test that LICENSE file exists."""
        license_path = Path(__file__).parent.parent.parent / "LICENSE"
        assert license_path.exists(), "LICENSE should exist in project root"


# Marker for smoke tests
pytestmark = pytest.mark.smoke

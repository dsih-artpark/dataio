"""
Unit tests for CLI commands.

Tests command parsing, option handling, and output.
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner


runner = CliRunner()


class TestCLIHelp:
    """Tests for CLI help commands."""

    def test_main_help(self):
        """Test main CLI help command."""
        from dataio.cli.cli import app

        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "dataio" in result.output.lower() or "user" in result.output.lower()

    def test_user_subcommand_help(self):
        """Test user subcommand help."""
        from dataio.cli.cli import app

        result = runner.invoke(app, ["user", "--help"])

        assert result.exit_code == 0
        assert "user" in result.output.lower()


class TestInitCommand:
    """Tests for the init command."""

    @patch("dataio.cli.user.DataIOAPI")
    @patch("dataio.cli.user.os.path.exists")
    @patch("dataio.cli.user.os.getenv")
    def test_init_with_existing_env(self, mock_getenv, mock_exists, mock_api):
        """Test init when .env file exists with valid credentials."""
        from dataio.cli.user import app

        mock_exists.return_value = True
        mock_getenv.side_effect = lambda key, default=None: {
            "DATAIO_API_KEY": "dio_test_key",
            "DATAIO_API_BASE_URL": "https://test.example.com",
        }.get(key, default)

        result = runner.invoke(app, ["init"])

        assert result.exit_code == 0
        assert "successfully" in result.output.lower() or "initialized" in result.output.lower()

    @patch("dataio.cli.user.DataIOAPI")
    @patch("dataio.cli.user.os.path.exists")
    @patch("dataio.cli.user.os.getenv")
    def test_init_api_error(self, mock_getenv, mock_exists, mock_api):
        """Test init when API initialization fails."""
        from dataio.cli.user import app

        mock_exists.return_value = True
        mock_getenv.side_effect = lambda key, default=None: {
            "DATAIO_API_KEY": "invalid_key",
            "DATAIO_API_BASE_URL": "https://test.example.com",
        }.get(key, default)
        mock_api.side_effect = Exception("API Error")

        result = runner.invoke(app, ["init"])

        assert "error" in result.output.lower()


class TestListDatasetsCommand:
    """Tests for the list-datasets command."""

    @patch("dataio.cli.user.DataIOAPI")
    @patch("dataio.cli.user.os.path.exists")
    def test_list_datasets_success(self, mock_exists, mock_api):
        """Test listing datasets successfully."""
        from dataio.cli.user import app

        mock_exists.return_value = True
        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.list_datasets.return_value = [
            {
                "ds_id": "GS0001DS0001",
                "title": "Test Dataset",
                "description": "A test dataset",
                "collection": {
                    "collection_id": "GS0001",
                    "collection_name": "Test Collection",
                    "category_id": "CAT001",
                    "category_name": "Test Category",
                },
                "data_owner": {"name": "Test Owner"},
            }
        ]

        result = runner.invoke(app, ["list-datasets"])

        assert result.exit_code == 0
        assert "GS0001DS0001" in result.output or "Test Dataset" in result.output

    @patch("dataio.cli.user.DataIOAPI")
    @patch("dataio.cli.user.os.path.exists")
    def test_list_datasets_with_limit(self, mock_exists, mock_api):
        """Test listing datasets with limit option."""
        from dataio.cli.user import app

        mock_exists.return_value = True
        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.list_datasets.return_value = []

        result = runner.invoke(app, ["list-datasets", "--limit", "50"])

        mock_client.list_datasets.assert_called_once_with(limit=50)

    @patch("dataio.cli.user.DataIOAPI")
    @patch("dataio.cli.user.os.path.exists")
    def test_list_datasets_with_collection_filter(self, mock_exists, mock_api):
        """Test listing datasets with collection filter."""
        from dataio.cli.user import app

        mock_exists.return_value = True
        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.list_datasets.return_value = [
            {
                "ds_id": "GS0001DS0001",
                "title": "Test Dataset",
                "description": "A test dataset",
                "collection": {
                    "collection_id": "GS0001",
                    "collection_name": "Target Collection",
                    "category_id": "CAT001",
                    "category_name": "Test Category",
                },
                "data_owner": {"name": "Test Owner"},
            },
            {
                "ds_id": "GS0002DS0001",
                "title": "Other Dataset",
                "description": "Another dataset",
                "collection": {
                    "collection_id": "GS0002",
                    "collection_name": "Other Collection",
                    "category_id": "CAT001",
                    "category_name": "Test Category",
                },
                "data_owner": {"name": "Test Owner"},
            },
        ]

        result = runner.invoke(app, ["list-datasets", "--collection", "Target Collection"])

        # Should only show the matching dataset
        assert result.exit_code == 0

    @patch("dataio.cli.user.DataIOAPI")
    @patch("dataio.cli.user.os.path.exists")
    def test_list_datasets_no_results(self, mock_exists, mock_api):
        """Test listing datasets with no results."""
        from dataio.cli.user import app

        mock_exists.return_value = True
        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.list_datasets.return_value = []

        result = runner.invoke(app, ["list-datasets", "--collection", "NonExistent"])

        assert result.exit_code == 0
        assert "no datasets" in result.output.lower()


class TestDownloadDatasetCommand:
    """Tests for the download-dataset command."""

    @patch("dataio.cli.user.DataIOAPI")
    def test_download_dataset_success(self, mock_api):
        """Test downloading a dataset successfully."""
        from dataio.cli.user import app

        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.download_dataset.return_value = "/tmp/data/GS0001DS0001-Test_Dataset"

        result = runner.invoke(app, ["download-dataset", "GS0001DS0001"])

        assert result.exit_code == 0
        assert "downloaded" in result.output.lower()

    @patch("dataio.cli.user.DataIOAPI")
    def test_download_dataset_with_options(self, mock_api):
        """Test downloading with bucket type and root dir options."""
        from dataio.cli.user import app

        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.download_dataset.return_value = "/custom/path"

        result = runner.invoke(
            app,
            [
                "download-dataset",
                "GS0001DS0001",
                "--bucket-type", "PREPROCESSED",
                "--root-dir", "/custom/path",
            ]
        )

        assert result.exit_code == 0
        mock_client.download_dataset.assert_called_once()
        call_kwargs = mock_client.download_dataset.call_args[1]
        assert call_kwargs["bucket_type"] == "PREPROCESSED"

    @patch("dataio.cli.user.DataIOAPI")
    def test_download_dataset_metadata_options(self, mock_api):
        """Test downloading with metadata options."""
        from dataio.cli.user import app

        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.download_dataset.return_value = "/tmp/data"

        result = runner.invoke(
            app,
            [
                "download-dataset",
                "GS0001DS0001",
                "--get-metadata",
                "--metadata-format", "json",
            ]
        )

        assert result.exit_code == 0
        call_kwargs = mock_client.download_dataset.call_args[1]
        assert call_kwargs["get_metadata"] is True
        assert call_kwargs["metadata_format"] == "json"


class TestDownloadShapefileCommand:
    """Tests for the download-shapefile command."""

    @patch("dataio.cli.user.DataIOAPI")
    def test_download_shapefile_success(self, mock_api):
        """Test downloading a shapefile successfully."""
        from dataio.cli.user import app

        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.data_dir = "data"
        mock_client.download_shapefile.return_value = "data/shapefiles/REG001.geojson"

        result = runner.invoke(app, ["download-shapefile", "REG001"])

        assert result.exit_code == 0
        assert "downloaded" in result.output.lower()

    @patch("dataio.cli.user.DataIOAPI")
    def test_download_shapefile_with_folder(self, mock_api):
        """Test downloading shapefile with custom folder."""
        from dataio.cli.user import app

        mock_client = MagicMock()
        mock_api.return_value = mock_client
        mock_client.data_dir = "data"
        mock_client.download_shapefile.return_value = "data/custom_folder/REG001.geojson"

        result = runner.invoke(
            app,
            ["download-shapefile", "REG001", "--shp-folder", "custom_folder"]
        )

        assert result.exit_code == 0


class TestCLIIntegration:
    """Integration-style tests for CLI workflows."""

    def test_cli_app_structure(self):
        """Test that CLI app has expected structure."""
        from dataio.cli.cli import app

        # Check that commands are registered
        command_names = [cmd.name for cmd in app.registered_commands]

        # Should have commands registered
        assert len(command_names) > 0 or len(app.registered_groups) > 0

    def test_user_app_commands(self):
        """Test that user app has expected commands."""
        from dataio.cli.user import app

        command_names = [cmd.name for cmd in app.registered_commands]

        assert "init" in command_names
        assert "list-datasets" in command_names
        assert "download-dataset" in command_names
        assert "download-shapefile" in command_names

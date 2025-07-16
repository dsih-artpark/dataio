# Test Documentation

This folder contains tests for the Dataio API, including comprehensive permission testing for different user types and dataset access levels.

## Test Environment Setup

The tests use environment variables for API keys:
- `TEST_ADMIN_KEY` - Admin user API key
- `TEST_ANALYST_KEY` - Analyst user API key
- `TEST_PUBLIC_KEY` - Public user API key
- `TEST_EXT_COLLABORATOR_KEY` - External collaborator API key

## Permission Mapping

The test suite validates the following permission structure across different user types and datasets:

### Datasets Used in Tests
- `TS0001DS0001` - Primary test dataset
- `TS0001DS0002` - Secondary test dataset
- `TS0001DS0003` - Tertiary test dataset
- `TS0001DS0004` - Restricted test dataset
- `TS0001DS0005` - Admin-only test dataset

### User Permission Matrix

| Dataset ID   | Admin    | Analyst  | Public   | Ext Collaborator |
|--------------|----------|----------|----------|------------------|
| TS0001DS0001 | DOWNLOAD | DOWNLOAD | DOWNLOAD | DOWNLOAD         |
| TS0001DS0002 | DOWNLOAD | DOWNLOAD | VIEW     | VIEW             |
| TS0001DS0003 | DOWNLOAD | DOWNLOAD | VIEW     | DOWNLOAD         |
| TS0001DS0004 | DOWNLOAD | DOWNLOAD | NONE     | DOWNLOAD         |
| TS0001DS0005 | DOWNLOAD | NONE     | NONE     | NONE             |

### Permission Levels

- **DOWNLOAD**: User can view dataset metadata and download dataset files
- **VIEW**: User can view dataset metadata but cannot download files
- **NONE**: User cannot access the dataset (it won't appear in their dataset list)

### Test Coverage

#### Admin Access Tests
- `test_admin_can_get_collections()` - Verifies admin can access collections endpoint
- `test_admin_can_get_users()` - Verifies admin can access users endpoint
- `test_admin_can_get_data_owners()` - Verifies admin can access data owners endpoint

#### Dataset Access Tests
- `test_get_datasets_for_[user_type]()` - Tests that users get appropriate datasets in their list
- `test_[user_type]_datasets_permissions()` - Validates permission levels for each user type
- `test_all_datasets_are_downloadable_for_admin()` - Ensures admin has DOWNLOAD access to all datasets

#### Dataset Table Access Tests
- `test_[user_type]_can_get_dataset_table_with_download_permission()` - Tests successful access with DOWNLOAD permission
- `test_[user_type]_cannot_get_dataset_table_with_view_permission()` - Tests blocked access with VIEW permission (403)
- `test_[user_type]_cannot_get_dataset_table_with_none_permission()` - Tests blocked access with NONE permission (403)

## Running Tests

```bash
# Run all tests
pytest src/dataio/api/tests/test_api.py

# Run specific test
pytest src/dataio/api/tests/test_api.py::test_function_name

# Run tests with verbose output
pytest -v src/dataio/api/tests/test_api.py
```

## Test Data Requirements

The tests assume the following test data exists in the database:
- Users with the specified API keys and roles
- Datasets TS0001DS0001 through TS0001DS0005
- Proper permission assignments matching the matrix above
- At least one collection, user, and data owner for admin access tests

## API Endpoints Tested

### User Endpoints
- `GET /api/v1/datasets` - Get datasets for user
- `GET /api/v1/datasets/{dataset_id}/{bucket_type}/tables` - Get dataset table files

### Admin Endpoints
- `GET /api/v1/admin/collections` - Get all collections
- `GET /api/v1/admin/users` - Get all users
- `GET /api/v1/admin/data-owners` - Get all data owners

## Notes

- All tests use the FastAPI TestClient for HTTP requests
- Tests are designed to run independently and can be executed in any order
- The permission matrix is based on the actual database configuration and should be updated if permissions change
- Dataset table access tests use the `raw` bucket type for testing file access permissions
# Test Documentation

This folder contains tests for the Dataio API, including comprehensive permission testing for different user types and dataset access levels.

## Test Environment Setup

The tests use environment variables for API keys:
- `TEST_ADMIN_KEY` - Admin user API key (admin@artpark.in)
- `TEST_ANALYST_KEY` - Analyst user API key (analyst@artpark.in)
- `TEST_PUBLIC_KEY` - Public user API key (public_user@artpark.in)
- `TEST_EXT_COLLABORATOR_KEY` - External collaborator API key (external_collaborator@psu.edu)

## Permission Mapping

The test suite validates the following permission structure across different user types and datasets:

### Datasets Used in Tests
- `TS0001DS0001` - Public DOWNLOAD dataset (everyone can download)
- `TS0001DS0002` - Public VIEW dataset (public can view, analyst has individual DOWNLOAD permission)
- `TS0001DS0003` - Public VIEW dataset (public can view, analyst+collaborator have individual DOWNLOAD permissions)
- `TS0001DS0004` - Public NONE dataset (only analyst+collaborator have individual DOWNLOAD permissions)
- `TS0001DS0005` - Public NONE dataset (admin-only access)
- `TS0001DS0006` - Resource group dataset (analyst has access via resource group)
- `TS0001DS0007` - Resource group dataset (analyst has access via resource group)

### User Permission Matrix

| Dataset ID   | Admin    | Analyst                | Public   | Ext Collaborator       | Permission Source         |
|--------------|----------|------------------------|----------|------------------------|---------------------------|
| TS0001DS0001 | DOWNLOAD | DOWNLOAD               | DOWNLOAD | DOWNLOAD               | Dataset default access    |
| TS0001DS0002 | DOWNLOAD | DOWNLOAD               | VIEW     | VIEW                   | Default VIEW + individual |
| TS0001DS0003 | DOWNLOAD | DOWNLOAD               | VIEW     | DOWNLOAD               | Default VIEW + individual |
| TS0001DS0004 | DOWNLOAD | DOWNLOAD               | NONE     | DOWNLOAD               | Individual permissions    |
| TS0001DS0005 | DOWNLOAD | NONE                   | NONE     | NONE                   | Admin-only access         |
| TS0001DS0006 | DOWNLOAD | DOWNLOAD               | NONE     | NONE                   | Resource group access     |
| TS0001DS0007 | DOWNLOAD | DOWNLOAD               | NONE     | NONE                   | Resource group access     |

### Test Users
- **admin@artpark.in** - Admin user with global access to all datasets
- **analyst@artpark.in** - Analyst user with individual permissions + resource group access
- **public_user@artpark.in** - Public user with default access levels only
- **external_collaborator@psu.edu** - External collaborator with individual permissions

### Resource Groups
- **test_resource_group** - Contains TS0001DS0006 and TS0001DS0007
  - analyst@artpark.in has DOWNLOAD access to this resource group

### Permission Levels

- **DOWNLOAD**: User can view dataset metadata and download dataset files
- **VIEW**: User can view dataset metadata but cannot download files
- **NONE**: User cannot access the dataset (it won't appear in their dataset list)

### Test Coverage

#### User Dataset Access Tests
- `test_admin_access_all_datasets()` - Tests admin has DOWNLOAD access to all datasets
- `test_analyst_access_pattern()` - Tests analyst's complete access pattern including resource groups
- `test_external_collaborator_access_pattern()` - Tests external collaborator's access pattern
- `test_public_user_access_pattern()` - Tests public user's minimal access pattern

#### Authentication Tests
- `test_authentication_failures()` - Tests invalid/missing API key scenarios
- `test_admin_endpoints_require_admin()` - Tests admin endpoints require admin privileges

## Running Tests

```bash
# Setup test data
python src/dataio/api/tests/insert_test_data.py --clean --all

# Run all tests
pytest src/dataio/api/tests/test_comprehensive_permissions.py -v

# Run specific test
pytest src/dataio/api/tests/test_comprehensive_permissions.py::test_function_name
```

## Test Data Requirements

The tests assume the following test data exists in the database:
- Users: admin@artpark.in, analyst@artpark.in, public_user@artpark.in, external_collaborator@psu.edu
- Datasets: TS0001DS0001 through TS0001DS0007
- Resource group: test_resource_group containing TS0001DS0006 and TS0001DS0007
- Proper permission assignments matching the matrix above

## API Endpoints Tested

### User Endpoints
- `GET /api/v1/datasets` - Get datasets for user

### Admin Endpoints
- `GET /api/v1/admin/collections` - Get all collections
- `GET /api/v1/admin/users` - Get all users
- `GET /api/v1/admin/data-owners` - Get all data owners

## Notes

- All tests use the FastAPI TestClient for HTTP requests
- Tests are designed to run independently and can be executed in any order
- The permission matrix is based on the actual database configuration and should be updated if permissions change
- Resource group tests validate that permissions can be granted through resource group membership
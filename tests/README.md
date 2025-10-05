# FPL Dataset Builder Tests

This directory contains tests for the FPL Dataset Builder project.

## Running Tests

### Using pytest (recommended)

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_snapshot_pytest.py

# Run with verbose output
uv run pytest -v

# Run with coverage report
uv run pytest --cov

# Run specific test class
uv run pytest tests/test_snapshot_pytest.py::TestSnapshotDatabaseOperations

# Run specific test function
uv run pytest tests/test_snapshot_pytest.py::test_snapshot_cli_command_exists
```

### Using the legacy test scripts directly

```bash
# Run database tests
uv run python tests/test_database.py

# Run client library tests
uv run python tests/test_client_library.py
```

## Test Files

### test_snapshot_pytest.py (NEW)
Pytest-compatible tests for player availability snapshot functionality:
- **TestSnapshotDatabaseOperations**: Tests for database CRUD operations
- **TestSnapshotDataStructure**: Tests for data schema and types
- **TestSnapshotProcessor**: Tests for snapshot data processing
- **test_snapshot_cli_command_exists**: Tests CLI command registration

### test_database.py
Comprehensive database operation tests with performance benchmarks:
- Connection testing
- Table creation and schema validation
- Data insertion performance
- Query performance
- Snapshot operations (NEW)
- Migration system testing

### test_client_library.py
Tests for the FPL data client library:
- Core data retrieval functions
- Raw API data access
- Derived analytics functions
- Manager data functions
- Snapshot functions (NEW)

## Test Coverage

The snapshot functionality is tested across multiple dimensions:

1. **Database Operations**
   - Save/retrieve snapshots
   - Duplicate prevention
   - Backfill filtering
   - Range queries

2. **Data Structure**
   - Schema validation
   - Field presence
   - Data types
   - Required fields

3. **Processing**
   - Bootstrap data conversion
   - Validation
   - Error handling

4. **CLI Integration**
   - Command registration
   - Command execution

## Writing New Tests

When adding new snapshot-related tests, follow these patterns:

```python
import pytest
import pandas as pd
from client.fpl_data_client import FPLDataClient

@pytest.fixture
def client():
    """Provide FPL data client instance."""
    return FPLDataClient(auto_init=True)

def test_snapshot_feature(client):
    """Test a snapshot feature."""
    snapshot = client.get_player_availability_snapshot(gameweek=1)
    assert isinstance(snapshot, pd.DataFrame)
    # Add your assertions
```

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: uv run pytest --cov --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
```

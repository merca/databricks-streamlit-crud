"""
Pytest configuration and fixtures for the Databricks Unity Catalog CRUD Application tests.
"""

import os
import sys
import pytest
from unittest.mock import Mock, MagicMock
from typing import Dict, Any

# Add the parent directory to the Python path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def mock_env_vars():
    """Fixture providing mock environment variables for testing."""
    return {
        'DATABRICKS_SERVER_HOSTNAME': 'test-workspace.cloud.databricks.com',
        'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/test-warehouse-id',
        'DATABRICKS_TOKEN': 'test-token-123',
        'UNITY_CATALOG_NAME': 'test_catalog',
        'UNITY_SCHEMA_NAME': 'test_schema',
        'UNITY_TABLE_NAME': 'test_table',
        'APP_DEBUG': 'true'
    }


@pytest.fixture
def mock_databricks_connection():
    """Fixture providing a mock Databricks SQL connection."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    
    # Configure cursor methods
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_cursor.description = []
    mock_cursor.execute.return_value = None
    
    # Configure connection methods
    mock_connection.cursor.return_value = mock_cursor
    mock_connection.close.return_value = None
    
    return mock_connection, mock_cursor


@pytest.fixture
def sample_user_data():
    """Fixture providing sample user data for testing."""
    return [
        {
            'id': 1,
            'name': 'John Doe',
            'email': 'john.doe@example.com',
            'department': 'Engineering',
            'status': 'Active',
            'notes': 'Test user 1',
            'owner_user': 'john.doe@company.com',
            'created_at': '2025-01-01 10:00:00',
            'updated_at': '2025-01-01 10:00:00'
        },
        {
            'id': 2,
            'name': 'Jane Smith',
            'email': 'jane.smith@example.com',
            'department': 'Marketing',
            'status': 'Active',
            'notes': 'Test user 2',
            'owner_user': 'jane.smith@company.com',
            'created_at': '2025-01-01 11:00:00',
            'updated_at': '2025-01-01 11:00:00'
        }
    ]


@pytest.fixture
def mock_streamlit_session_state():
    """Fixture providing mock Streamlit session state."""
    class MockSessionState:
        def __init__(self):
            self._state = {}
        
        def __getitem__(self, key):
            return self._state.get(key)
        
        def __setitem__(self, key, value):
            self._state[key] = value
        
        def __contains__(self, key):
            return key in self._state
        
        def get(self, key, default=None):
            return self._state.get(key, default)
        
        def setdefault(self, key, default):
            if key not in self._state:
                self._state[key] = default
            return self._state[key]
    
    return MockSessionState()


@pytest.fixture
def mock_streamlit_components():
    """Fixture providing mock Streamlit UI components."""
    components = {
        'success': Mock(),
        'error': Mock(),
        'warning': Mock(),
        'info': Mock(),
        'write': Mock(),
        'header': Mock(),
        'subheader': Mock(),
        'text_input': Mock(return_value=''),
        'text_area': Mock(return_value=''),
        'selectbox': Mock(return_value=''),
        'button': Mock(return_value=False),
        'checkbox': Mock(return_value=False),
        'form': Mock(),
        'form_submit_button': Mock(return_value=False),
        'dataframe': Mock(),
        'tabs': Mock(return_value=['tab1', 'tab2']),
        'expander': Mock(),
        'columns': Mock(return_value=[Mock(), Mock()]),
        'spinner': Mock(),
    }
    
    # Configure form context manager
    form_mock = MagicMock()
    form_mock.__enter__ = Mock(return_value=form_mock)
    form_mock.__exit__ = Mock(return_value=None)
    components['form'] = Mock(return_value=form_mock)
    
    # Configure expander context manager
    expander_mock = MagicMock()
    expander_mock.__enter__ = Mock(return_value=expander_mock)
    expander_mock.__exit__ = Mock(return_value=None)
    components['expander'] = Mock(return_value=expander_mock)
    
    # Configure spinner context manager
    spinner_mock = MagicMock()
    spinner_mock.__enter__ = Mock(return_value=spinner_mock)
    spinner_mock.__exit__ = Mock(return_value=None)
    components['spinner'] = Mock(return_value=spinner_mock)
    
    return components


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch, mock_env_vars):
    """Automatically set up test environment for all tests."""
    # Set environment variables
    for key, value in mock_env_vars.items():
        monkeypatch.setenv(key, value)
    
    # Mock streamlit imports to avoid import errors in tests
    streamlit_mock = MagicMock()
    monkeypatch.setattr('sys.modules', {
        **sys.modules,
        'streamlit': streamlit_mock,
        'databricks.sql': MagicMock(),
        'databricks': MagicMock()
    })


@pytest.fixture
def clean_imports():
    """Clean up imports between tests to avoid module caching issues."""
    modules_to_remove = []
    for module_name in sys.modules:
        if module_name.startswith(('config', 'app')):
            modules_to_remove.append(module_name)
    
    for module_name in modules_to_remove:
        if module_name in sys.modules:
            del sys.modules[module_name]
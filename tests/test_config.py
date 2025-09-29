"""
Unit tests for the config.py module.

Tests configuration loading, validation, environment variable handling,
and error scenarios.
"""

import os
import pytest
from unittest.mock import patch, Mock
import sys

# Import the config module
try:
    import config
except ImportError:
    # If direct import fails, try to add parent directory to path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import config


class TestDatabricksConfig:
    """Test cases for DatabricksConfig class."""

    def test_config_initialization_with_valid_env_vars(self, mock_env_vars):
        """Test that config initializes correctly with valid environment variables."""
        with patch.dict(os.environ, mock_env_vars):
            cfg = config.DatabricksConfig()
            
            assert cfg.server_hostname == 'test-workspace.cloud.databricks.com'
            assert cfg.http_path == '/sql/1.0/warehouses/test-warehouse-id'
            assert cfg.access_token == 'test-token-123'
            assert cfg.catalog_name == 'test_catalog'
            assert cfg.schema_name == 'test_schema'
            assert cfg.table_name == 'test_table'
            assert cfg.debug is True

    def test_config_initialization_with_missing_required_vars(self):
        """Test that config raises ValueError when required environment variables are missing."""
        # Clear all environment variables that might be set
        required_vars = ['DATABRICKS_SERVER_HOSTNAME', 'DATABRICKS_HTTP_PATH', 'DATABRICKS_TOKEN']
        
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                config.DatabricksConfig()
            
            error_message = str(exc_info.value)
            # Check that the error mentions missing required variables
            assert "Missing required environment variable" in error_message or "DATABRICKS_SERVER_HOSTNAME" in error_message

    def test_config_initialization_with_partial_required_vars(self):
        """Test config behavior when some but not all required variables are provided."""
        partial_vars = {
            'DATABRICKS_SERVER_HOSTNAME': 'test-workspace.cloud.databricks.com'
            # Missing DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN
        }
        
        with patch.dict(os.environ, partial_vars, clear=True):
            with pytest.raises(ValueError):
                config.DatabricksConfig()

    def test_config_default_values(self):
        """Test that default values are used when optional environment variables are not set."""
        required_vars = {
            'DATABRICKS_SERVER_HOSTNAME': 'test-workspace.cloud.databricks.com',
            'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/test-warehouse-id',
            'DATABRICKS_TOKEN': 'test-token-123'
        }
        
        with patch.dict(os.environ, required_vars, clear=True):
            cfg = config.DatabricksConfig()
            
            assert cfg.catalog_name == 'main'  # Default value
            assert cfg.schema_name == 'default'  # Default value
            assert cfg.table_name == 'user_data'  # Default value
            assert cfg.debug is False  # Default value

    def test_config_debug_flag_parsing(self):
        """Test that debug flag is correctly parsed from environment variables."""
        base_vars = {
            'DATABRICKS_SERVER_HOSTNAME': 'test-workspace.cloud.databricks.com',
            'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/test-warehouse-id',
            'DATABRICKS_TOKEN': 'test-token-123'
        }
        
        # Test debug=true
        debug_true_vars = {**base_vars, 'APP_DEBUG': 'true'}
        with patch.dict(os.environ, debug_true_vars, clear=True):
            cfg = config.DatabricksConfig()
            assert cfg.debug is True
        
        # Test debug=false
        debug_false_vars = {**base_vars, 'APP_DEBUG': 'false'}
        with patch.dict(os.environ, debug_false_vars, clear=True):
            cfg = config.DatabricksConfig()
            assert cfg.debug is False
        
        # Test debug=1
        debug_one_vars = {**base_vars, 'APP_DEBUG': '1'}
        with patch.dict(os.environ, debug_one_vars, clear=True):
            cfg = config.DatabricksConfig()
            assert cfg.debug is True
        
        # Test debug=0
        debug_zero_vars = {**base_vars, 'APP_DEBUG': '0'}
        with patch.dict(os.environ, debug_zero_vars, clear=True):
            cfg = config.DatabricksConfig()
            assert cfg.debug is False

    def test_config_validation_with_empty_values(self):
        """Test that config validation fails with empty required values."""
        invalid_vars = {
            'DATABRICKS_SERVER_HOSTNAME': '',  # Empty value
            'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/test-warehouse-id',
            'DATABRICKS_TOKEN': 'test-token-123'
        }
        
        with patch.dict(os.environ, invalid_vars, clear=True):
            with pytest.raises(ValueError) as exc_info:
                config.DatabricksConfig()
            
            assert "cannot be empty" in str(exc_info.value) or "DATABRICKS_SERVER_HOSTNAME" in str(exc_info.value)

    def test_config_table_full_name_property(self, mock_env_vars):
        """Test the table_full_name property returns correct format."""
        with patch.dict(os.environ, mock_env_vars):
            cfg = config.DatabricksConfig()
            expected_name = f"{cfg.catalog_name}.{cfg.schema_name}.{cfg.table_name}"
            assert cfg.table_full_name == expected_name
            assert cfg.table_full_name == "test_catalog.test_schema.test_table"

    def test_config_to_dict_method(self, mock_env_vars):
        """Test that to_dict method returns all configuration values."""
        with patch.dict(os.environ, mock_env_vars):
            cfg = config.DatabricksConfig()
            config_dict = cfg.to_dict()
            
            assert isinstance(config_dict, dict)
            assert 'server_hostname' in config_dict
            assert 'http_path' in config_dict
            assert 'catalog_name' in config_dict
            assert 'schema_name' in config_dict
            assert 'table_name' in config_dict
            assert 'debug' in config_dict
            
            # Verify values
            assert config_dict['server_hostname'] == 'test-workspace.cloud.databricks.com'
            assert config_dict['debug'] is True
            
            # Access token should be masked or not included for security
            assert 'access_token' not in config_dict or config_dict.get('access_token') == '***'

    def test_config_repr_method(self, mock_env_vars):
        """Test that __repr__ method provides useful debugging information."""
        with patch.dict(os.environ, mock_env_vars):
            cfg = config.DatabricksConfig()
            repr_str = repr(cfg)
            
            assert 'DatabricksConfig' in repr_str
            assert 'test-workspace.cloud.databricks.com' in repr_str
            assert 'test_catalog.test_schema.test_table' in repr_str
            
            # Access token should be masked in repr
            assert 'test-token-123' not in repr_str

    def test_config_with_custom_values(self):
        """Test config with custom catalog, schema, and table names."""
        custom_vars = {
            'DATABRICKS_SERVER_HOSTNAME': 'custom-workspace.cloud.databricks.com',
            'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/custom-warehouse-id',
            'DATABRICKS_TOKEN': 'custom-token-456',
            'UNITY_CATALOG_NAME': 'custom_catalog',
            'UNITY_SCHEMA_NAME': 'custom_schema',
            'UNITY_TABLE_NAME': 'custom_table'
        }
        
        with patch.dict(os.environ, custom_vars, clear=True):
            cfg = config.DatabricksConfig()
            
            assert cfg.catalog_name == 'custom_catalog'
            assert cfg.schema_name == 'custom_schema'
            assert cfg.table_name == 'custom_table'
            assert cfg.table_full_name == 'custom_catalog.custom_schema.custom_table'

    def test_config_validation_edge_cases(self):
        """Test config validation with edge cases."""
        base_vars = {
            'DATABRICKS_SERVER_HOSTNAME': 'test-workspace.cloud.databricks.com',
            'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/test-warehouse-id',
            'DATABRICKS_TOKEN': 'test-token-123'
        }
        
        # Test with whitespace in values
        whitespace_vars = {
            **base_vars,
            'UNITY_CATALOG_NAME': '  spaced_catalog  ',
            'UNITY_SCHEMA_NAME': '  spaced_schema  '
        }
        
        with patch.dict(os.environ, whitespace_vars, clear=True):
            cfg = config.DatabricksConfig()
            # Values should be stripped
            assert cfg.catalog_name == 'spaced_catalog'
            assert cfg.schema_name == 'spaced_schema'

    @patch('config.load_dotenv')
    def test_config_loads_dotenv(self, mock_load_dotenv, mock_env_vars):
        """Test that config attempts to load .env file."""
        with patch.dict(os.environ, mock_env_vars):
            config.DatabricksConfig()
            mock_load_dotenv.assert_called_once()

    def test_config_singleton_pattern(self, mock_env_vars):
        """Test that config behaves consistently across multiple instantiations."""
        with patch.dict(os.environ, mock_env_vars):
            cfg1 = config.DatabricksConfig()
            cfg2 = config.DatabricksConfig()
            
            # Should have the same values
            assert cfg1.server_hostname == cfg2.server_hostname
            assert cfg1.catalog_name == cfg2.catalog_name
            assert cfg1.table_full_name == cfg2.table_full_name


class TestConfigModule:
    """Test cases for module-level functions and utilities."""

    def test_module_imports(self):
        """Test that the config module imports correctly."""
        assert hasattr(config, 'DatabricksConfig')
        assert callable(config.DatabricksConfig)

    def test_module_constants(self):
        """Test that module constants are defined correctly."""
        # If the module defines any constants, test them here
        cfg_class = config.DatabricksConfig
        
        # Test that class exists and is properly defined
        assert cfg_class is not None
        assert hasattr(cfg_class, '__init__')

    @patch('config.os.getenv')
    def test_environment_variable_access(self, mock_getenv):
        """Test that environment variables are accessed correctly."""
        mock_getenv.side_effect = lambda key, default=None: {
            'DATABRICKS_SERVER_HOSTNAME': 'test-workspace.cloud.databricks.com',
            'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/test-warehouse-id',
            'DATABRICKS_TOKEN': 'test-token-123'
        }.get(key, default)
        
        cfg = config.DatabricksConfig()
        
        # Verify that os.getenv was called for required environment variables
        expected_calls = [
            'DATABRICKS_SERVER_HOSTNAME',
            'DATABRICKS_HTTP_PATH', 
            'DATABRICKS_TOKEN',
            'UNITY_CATALOG_NAME',
            'UNITY_SCHEMA_NAME',
            'UNITY_TABLE_NAME',
            'APP_DEBUG'
        ]
        
        for env_var in expected_calls:
            mock_getenv.assert_any_call(env_var, default=None) or mock_getenv.assert_any_call(env_var)
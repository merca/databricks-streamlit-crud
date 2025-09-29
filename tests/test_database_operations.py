"""
Unit tests for database operations in the Databricks Unity Catalog CRUD Application.

Tests CRUD operations, connection handling, error scenarios, and row-level security.
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os

# Import the app module
try:
    import app
except ImportError:
    # If direct import fails, try to add parent directory to path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import app


class TestDatabaseManager:
    """Test cases for DatabaseManager class."""

    @pytest.fixture
    def config(self):
        """Fixture providing database configuration."""
        return {
            'server_hostname': 'test-workspace.cloud.databricks.com',
            'http_path': '/sql/1.0/warehouses/test-warehouse-id',
            'access_token': 'test-token-123',
            'catalog': 'test_catalog',
            'schema': 'test_schema',
            'table': 'test_table'
        }

    @pytest.fixture
    def db_manager(self, config):
        """Fixture providing DatabaseManager instance."""
        return app.DatabaseManager(config)

    def test_database_manager_initialization(self, config):
        """Test that DatabaseManager initializes correctly."""
        db_manager = app.DatabaseManager(config)
        
        assert db_manager.config == config
        assert db_manager.connection is None

    @patch('app.sql.connect')
    def test_get_connection_success(self, mock_connect, db_manager):
        """Test successful database connection."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        connection = db_manager.get_connection()
        
        assert connection == mock_connection
        assert db_manager.connection == mock_connection
        mock_connect.assert_called_once_with(
            server_hostname='test-workspace.cloud.databricks.com',
            http_path='/sql/1.0/warehouses/test-warehouse-id',
            access_token='test-token-123'
        )

    @patch('app.sql.connect')
    @patch('streamlit.error')
    def test_get_connection_failure(self, mock_st_error, mock_connect, db_manager):
        """Test database connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        connection = db_manager.get_connection()
        
        assert connection is None
        assert db_manager.connection is None
        mock_st_error.assert_called_once_with("Failed to connect to Databricks: Connection failed")

    @patch('app.sql.connect')
    def test_get_connection_reuse(self, mock_connect, db_manager):
        """Test that existing connection is reused."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # First call creates connection
        connection1 = db_manager.get_connection()
        # Second call should reuse existing connection
        connection2 = db_manager.get_connection()
        
        assert connection1 == connection2 == mock_connection
        # connect should only be called once
        mock_connect.assert_called_once()

    def test_execute_query_select_success(self, db_manager, mock_databricks_connection):
        """Test successful SELECT query execution."""
        mock_connection, mock_cursor = mock_databricks_connection
        db_manager.connection = mock_connection
        
        # Mock cursor result
        mock_cursor.fetchall.return_value = [('user1', 'test@example.com'), ('user2', 'test2@example.com')]
        
        result = db_manager.execute_query("SELECT name, email FROM test_table")
        
        assert result == [('user1', 'test@example.com'), ('user2', 'test2@example.com')]
        mock_cursor.execute.assert_called_once_with("SELECT name, email FROM test_table")
        mock_cursor.fetchall.assert_called_once()

    def test_execute_query_insert_success(self, db_manager, mock_databricks_connection):
        """Test successful INSERT query execution."""
        mock_connection, mock_cursor = mock_databricks_connection
        db_manager.connection = mock_connection
        
        query = "INSERT INTO test_table (name, email) VALUES (?, ?)"
        parameters = ['John Doe', 'john@example.com']
        
        result = db_manager.execute_query(query, parameters)
        
        assert result is True
        mock_cursor.execute.assert_called_once_with(query, parameters)
        mock_connection.commit.assert_called_once()

    def test_execute_query_no_connection(self, db_manager):
        """Test query execution when connection fails."""
        with patch.object(db_manager, 'get_connection', return_value=None):
            result = db_manager.execute_query("SELECT * FROM test_table")
            assert result is None

    @patch('streamlit.error')
    def test_execute_query_exception(self, mock_st_error, db_manager, mock_databricks_connection):
        """Test query execution with database exception."""
        mock_connection, mock_cursor = mock_databricks_connection
        db_manager.connection = mock_connection
        mock_cursor.execute.side_effect = Exception("SQL execution error")
        
        result = db_manager.execute_query("SELECT * FROM test_table")
        
        assert result is None
        mock_st_error.assert_called_once_with("Query execution failed: SQL execution error")

    def test_get_current_user_success(self, db_manager):
        """Test successful current user retrieval."""
        with patch.object(db_manager, 'execute_query', return_value=[('test.user@company.com',)]):
            user = db_manager.get_current_user()
            assert user == 'test.user@company.com'

    def test_get_current_user_no_result(self, db_manager):
        """Test current user retrieval with no result."""
        with patch.object(db_manager, 'execute_query', return_value=[]):
            user = db_manager.get_current_user()
            assert user == 'unknown_user'

    def test_get_current_user_exception(self, db_manager):
        """Test current user retrieval with exception."""
        with patch.object(db_manager, 'execute_query', side_effect=Exception("Query failed")):
            user = db_manager.get_current_user()
            assert user == 'unknown_user'

    def test_create_record_success(self, db_manager):
        """Test successful record creation."""
        test_data = {
            'name': 'John Doe',
            'email': 'john@example.com',
            'department': 'IT',
            'status': 'Active',
            'notes': 'Test user'
        }
        user_id = 'test.user@company.com'
        
        with patch.object(db_manager, 'execute_query', return_value=True) as mock_execute:
            with patch('app.datetime') as mock_datetime:
                mock_now = datetime(2025, 1, 1, 10, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                result = db_manager.create_record(test_data, user_id)
                
                assert result is True
                
                # Check that execute_query was called with correct parameters
                call_args = mock_execute.call_args
                query = call_args[0][0]
                params = call_args[0][1]
                
                assert 'INSERT INTO test_catalog.test_schema.test_table' in query
                assert 'name, email, department, status, notes, owner_user, created_at, updated_at' in query
                assert params[:5] == ['John Doe', 'john@example.com', 'IT', 'Active', 'Test user']
                assert params[5] == user_id
                assert params[6] == mock_now
                assert params[7] == mock_now

    def test_read_records_success(self, db_manager, sample_user_data):
        """Test successful record reading."""
        user_id = 'test.user@company.com'
        
        # Mock the database results
        mock_result = [
            (1, 'John Doe', 'john.doe@example.com', 'Engineering', 'Active', 'Test user 1', 
             'john.doe@company.com', '2025-01-01 10:00:00', '2025-01-01 10:00:00')
        ]
        
        # Mock the DESCRIBE query result
        mock_columns = [('id',), ('name',), ('email',), ('department',), ('status',), 
                       ('notes',), ('owner_user',), ('created_at',), ('updated_at',)]
        
        with patch.object(db_manager, 'execute_query', return_value=mock_result) as mock_execute:
            with patch.object(db_manager, 'get_connection') as mock_get_conn:
                mock_connection = MagicMock()
                mock_cursor = MagicMock()
                mock_cursor.fetchall.return_value = mock_columns
                mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
                mock_get_conn.return_value = mock_connection
                
                result = db_manager.read_records(user_id)
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 1
                assert result.iloc[0]['name'] == 'John Doe'
                assert result.iloc[0]['email'] == 'john.doe@example.com'

    def test_read_records_with_filters(self, db_manager):
        """Test record reading with filters."""
        user_id = 'test.user@company.com'
        filters = {'name': 'John', 'email': 'doe'}
        
        with patch.object(db_manager, 'execute_query', return_value=[]) as mock_execute:
            with patch.object(db_manager, 'get_connection') as mock_get_conn:
                mock_connection = MagicMock()
                mock_cursor = MagicMock()
                mock_cursor.fetchall.return_value = []
                mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
                mock_get_conn.return_value = mock_connection
                
                result = db_manager.read_records(user_id, filters)
                
                call_args = mock_execute.call_args
                query = call_args[0][0]
                params = call_args[0][1]
                
                assert 'WHERE owner_user = ?' in query
                assert 'AND name LIKE ?' in query
                assert 'AND email LIKE ?' in query
                assert 'ORDER BY updated_at DESC' in query
                assert params == [user_id, '%John%', '%doe%']

    def test_read_records_no_data(self, db_manager):
        """Test record reading when no data is found."""
        user_id = 'test.user@company.com'
        
        with patch.object(db_manager, 'execute_query', return_value=None):
            result = db_manager.read_records(user_id)
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_update_record_success(self, db_manager):
        """Test successful record update."""
        record_id = 1
        user_id = 'test.user@company.com'
        update_data = {
            'name': 'Jane Doe',
            'email': 'jane@example.com',
            'status': 'Inactive'
        }
        
        with patch.object(db_manager, 'execute_query', return_value=True) as mock_execute:
            with patch('app.datetime') as mock_datetime:
                mock_now = datetime(2025, 1, 1, 11, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                result = db_manager.update_record(record_id, update_data, user_id)
                
                assert result is True
                
                call_args = mock_execute.call_args
                query = call_args[0][0]
                params = call_args[0][1]
                
                assert 'UPDATE test_catalog.test_schema.test_table' in query
                assert 'SET name = ?, email = ?, status = ?, updated_at = ?' in query
                assert 'WHERE id = ? AND owner_user = ?' in query
                assert params == ['Jane Doe', 'jane@example.com', 'Inactive', mock_now, record_id, user_id]

    def test_delete_record_success(self, db_manager):
        """Test successful record deletion."""
        record_id = 1
        user_id = 'test.user@company.com'
        
        with patch.object(db_manager, 'execute_query', return_value=True) as mock_execute:
            result = db_manager.delete_record(record_id, user_id)
            
            assert result is True
            
            call_args = mock_execute.call_args
            query = call_args[0][0]
            params = call_args[0][1]
            
            assert 'DELETE FROM test_catalog.test_schema.test_table' in query
            assert 'WHERE id = ? AND owner_user = ?' in query
            assert params == [record_id, user_id]

    def test_row_level_security_enforcement(self, db_manager):
        """Test that all operations enforce row-level security with owner_user."""
        user_id = 'test.user@company.com'
        
        # Test create adds owner_user
        test_data = {'name': 'Test'}
        with patch.object(db_manager, 'execute_query') as mock_execute:
            with patch('app.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.now()
                db_manager.create_record(test_data, user_id)
                
                call_args = mock_execute.call_args[0]
                assert user_id in call_args[1]  # owner_user should be in parameters
        
        # Test read filters by owner_user
        with patch.object(db_manager, 'execute_query') as mock_execute:
            with patch.object(db_manager, 'get_connection'):
                db_manager.read_records(user_id)
                
                call_args = mock_execute.call_args[0]
                query, params = call_args[0], call_args[1]
                assert 'WHERE owner_user = ?' in query
                assert params[0] == user_id
        
        # Test update filters by owner_user
        with patch.object(db_manager, 'execute_query') as mock_execute:
            with patch('app.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.now()
                db_manager.update_record(1, {'name': 'Updated'}, user_id)
                
                call_args = mock_execute.call_args[0]
                query, params = call_args[0], call_args[1]
                assert 'WHERE id = ? AND owner_user = ?' in query
                assert params[-1] == user_id  # Last parameter should be user_id
        
        # Test delete filters by owner_user
        with patch.object(db_manager, 'execute_query') as mock_execute:
            db_manager.delete_record(1, user_id)
            
            call_args = mock_execute.call_args[0]
            query, params = call_args[0], call_args[1]
            assert 'WHERE id = ? AND owner_user = ?' in query
            assert params[1] == user_id


class TestUtilityFunctions:
    """Test cases for utility functions in the app module."""

    @patch('app.os.getenv')
    def test_get_config_with_env_vars(self, mock_getenv):
        """Test configuration loading from environment variables."""
        mock_getenv.side_effect = lambda key, default=None: {
            'DATABRICKS_SERVER_HOSTNAME': 'test-workspace.cloud.databricks.com',
            'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/test-id',
            'DATABRICKS_TOKEN': 'test-token',
            'UNITY_CATALOG_NAME': 'test_catalog',
            'UNITY_SCHEMA_NAME': 'test_schema',
            'UNITY_TABLE_NAME': 'test_table'
        }.get(key, default)
        
        config = app.get_config()
        
        assert config['server_hostname'] == 'test-workspace.cloud.databricks.com'
        assert config['http_path'] == '/sql/1.0/warehouses/test-id'
        assert config['access_token'] == 'test-token'
        assert config['catalog'] == 'test_catalog'
        assert config['schema'] == 'test_schema'
        assert config['table'] == 'test_table'

    @patch('app.os.getenv')
    def test_get_config_with_defaults(self, mock_getenv):
        """Test configuration loading with default values."""
        mock_getenv.side_effect = lambda key, default=None: {
            'DATABRICKS_SERVER_HOSTNAME': 'test-workspace.cloud.databricks.com',
            'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/test-id',
            'DATABRICKS_TOKEN': 'test-token'
        }.get(key, default)
        
        config = app.get_config()
        
        assert config['catalog'] == 'main'  # Default value
        assert config['schema'] == 'default'  # Default value
        assert config['table'] == 'user_data'  # Default value

    @patch('streamlit.session_state', {})
    @patch('app.get_config')
    @patch('app.DatabaseManager')
    def test_get_user_session_new_user(self, mock_db_manager_class, mock_get_config, mock_streamlit_session_state):
        """Test user session creation for new user."""
        mock_config = {'test': 'config'}
        mock_get_config.return_value = mock_config
        
        mock_db_manager = MagicMock()
        mock_db_manager.get_current_user.return_value = 'test.user@company.com'
        mock_db_manager_class.return_value = mock_db_manager
        
        with patch('app.st.session_state', mock_streamlit_session_state):
            with patch('app.hashlib.md5') as mock_md5:
                mock_hash = MagicMock()
                mock_hash.hexdigest.return_value = 'abcd1234'
                mock_md5.return_value = mock_hash
                
                user_id = app.get_user_session()
                
                assert user_id == 'test.user@company.com'
                mock_db_manager_class.assert_called_once_with(mock_config)
                mock_db_manager.get_current_user.assert_called_once()

    @patch('app.st.session_state', {'user_id': 'existing.user@company.com'})
    def test_get_user_session_existing_user(self):
        """Test user session retrieval for existing user."""
        user_id = app.get_user_session()
        assert user_id == 'existing.user@company.com'


class TestErrorHandling:
    """Test cases for error handling scenarios."""

    def test_database_connection_errors(self, config):
        """Test various database connection error scenarios."""
        db_manager = app.DatabaseManager(config)
        
        # Test connection timeout
        with patch('app.sql.connect', side_effect=TimeoutError("Connection timeout")):
            with patch('streamlit.error') as mock_error:
                connection = db_manager.get_connection()
                assert connection is None
                mock_error.assert_called_once_with("Failed to connect to Databricks: Connection timeout")
        
        # Test authentication error
        with patch('app.sql.connect', side_effect=Exception("Authentication failed")):
            with patch('streamlit.error') as mock_error:
                db_manager.connection = None  # Reset connection
                connection = db_manager.get_connection()
                assert connection is None
                mock_error.assert_called_once_with("Failed to connect to Databricks: Authentication failed")

    def test_query_execution_errors(self, db_manager, mock_databricks_connection):
        """Test query execution error scenarios."""
        mock_connection, mock_cursor = mock_databricks_connection
        db_manager.connection = mock_connection
        
        # Test SQL syntax error
        mock_cursor.execute.side_effect = Exception("SQL syntax error")
        with patch('streamlit.error') as mock_error:
            result = db_manager.execute_query("INVALID SQL")
            assert result is None
            mock_error.assert_called_once_with("Query execution failed: SQL syntax error")
        
        # Test permission error
        mock_cursor.execute.side_effect = Exception("Permission denied")
        with patch('streamlit.error') as mock_error:
            result = db_manager.execute_query("SELECT * FROM restricted_table")
            assert result is None
            mock_error.assert_called_once_with("Query execution failed: Permission denied")

    def test_malformed_data_handling(self, db_manager):
        """Test handling of malformed or invalid data."""
        user_id = 'test.user@company.com'
        
        # Test create with invalid data types
        invalid_data = {
            'name': None,  # Invalid name
            'email': 123,  # Invalid email type
            'department': 'InvalidDepartment'
        }
        
        with patch.object(db_manager, 'execute_query', side_effect=Exception("Data type error")):
            result = db_manager.create_record(invalid_data, user_id)
            assert result is None

    def test_concurrent_access_scenarios(self, db_manager):
        """Test scenarios involving concurrent database access."""
        user_id = 'test.user@company.com'
        
        # Simulate record being deleted by another session during update
        with patch.object(db_manager, 'execute_query', return_value=True) as mock_execute:
            # First call succeeds (checking if record exists)
            # Second call fails (record deleted by another session)
            mock_execute.side_effect = [True, None]
            
            result = db_manager.delete_record(1, user_id)
            # Should handle gracefully even if record doesn't exist
            assert result is None
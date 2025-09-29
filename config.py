"""
Configuration helper for Databricks Streamlit App
Provides additional configuration management and validation
"""

import os
import streamlit as st
from dotenv import load_dotenv
from typing import Dict, Optional

# Load environment variables from .env file if it exists
load_dotenv()

class Config:
    """Configuration management class for the Databricks app"""
    
    def __init__(self):
        self._config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Optional[str]]:
        """Load configuration from environment variables"""
        return {
            # Required Databricks connection settings
            "server_hostname": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
            "access_token": os.getenv("DATABRICKS_TOKEN"),
            
            # Unity Catalog settings with defaults
            "catalog": os.getenv("UNITY_CATALOG_NAME", "main"),
            "schema": os.getenv("UNITY_SCHEMA_NAME", "default"),
            "table": os.getenv("UNITY_TABLE_NAME", "user_data"),
            
            # Optional application settings
            "app_title": os.getenv("APP_TITLE", "Databricks Unity Catalog CRUD App"),
            "app_debug": os.getenv("APP_DEBUG", "false").lower() == "true",
            
            # Connection settings
            "connection_timeout": int(os.getenv("CONNECTION_TIMEOUT", "30")),
            "max_retries": int(os.getenv("MAX_RETRIES", "3")),
        }
    
    def _validate_config(self):
        """Validate required configuration parameters"""
        required_fields = ["server_hostname", "http_path", "access_token"]
        missing_fields = [field for field in required_fields if not self._config.get(field)]
        
        if missing_fields:
            error_msg = f"Missing required configuration: {', '.join(missing_fields)}"
            if hasattr(st, 'error'):  # Only show error if Streamlit is available
                st.error(error_msg)
                st.info("Please check your environment variables or .env file")
            raise ValueError(error_msg)
    
    @property
    def is_valid(self) -> bool:
        """Check if configuration is valid"""
        try:
            self._validate_config()
            return True
        except ValueError:
            return False
    
    @property
    def databricks_connection(self) -> Dict[str, str]:
        """Get Databricks connection parameters"""
        return {
            "server_hostname": self._config["server_hostname"],
            "http_path": self._config["http_path"],
            "access_token": self._config["access_token"]
        }
    
    @property
    def unity_catalog(self) -> Dict[str, str]:
        """Get Unity Catalog parameters"""
        return {
            "catalog": self._config["catalog"],
            "schema": self._config["schema"],
            "table": self._config["table"]
        }
    
    @property
    def table_full_name(self) -> str:
        """Get fully qualified table name"""
        return f"{self._config['catalog']}.{self._config['schema']}.{self._config['table']}"
    
    @property
    def app_settings(self) -> Dict:
        """Get application settings"""
        return {
            "title": self._config["app_title"],
            "debug": self._config["app_debug"],
            "connection_timeout": self._config["connection_timeout"],
            "max_retries": self._config["max_retries"]
        }
    
    def get(self, key: str, default=None):
        """Get configuration value by key"""
        return self._config.get(key, default)
    
    def display_debug_info(self):
        """Display debug information (without sensitive data)"""
        if self._config["app_debug"]:
            st.sidebar.subheader("Debug Information")
            st.sidebar.text(f"Catalog: {self._config['catalog']}")
            st.sidebar.text(f"Schema: {self._config['schema']}")
            st.sidebar.text(f"Table: {self._config['table']}")
            st.sidebar.text(f"Full Table: {self.table_full_name}")
            st.sidebar.text(f"Server: {self._config['server_hostname'][:20]}...")


# Global configuration instance
def get_config() -> Config:
    """Get global configuration instance"""
    if not hasattr(st.session_state, '_app_config'):
        st.session_state._app_config = Config()
    return st.session_state._app_config


# Configuration validation function for use in other modules
def validate_environment():
    """Validate environment configuration and display helpful errors"""
    try:
        config = get_config()
        return config.is_valid
    except ValueError as e:
        st.error(f"Configuration Error: {str(e)}")
        
        # Display helpful setup instructions
        with st.expander("🔧 Setup Instructions", expanded=True):
            st.markdown("""
            ### Required Environment Variables:
            
            Create a `.env` file in your project directory with:
            
            ```bash
            DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
            DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
            DATABRICKS_TOKEN=your-access-token
            ```
            
            ### How to get these values:
            
            1. **Server Hostname**: Your workspace URL without `https://`
            2. **HTTP Path**: Go to SQL Warehouses → Select warehouse → Connection Details
            3. **Access Token**: User Settings → Developer → Access Tokens → Generate New Token
            
            ### Optional Configuration:
            
            ```bash
            UNITY_CATALOG_NAME=main
            UNITY_SCHEMA_NAME=default  
            UNITY_TABLE_NAME=user_data
            APP_DEBUG=true
            ```
            """)
        
        return False


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = Config()
        print("Configuration loaded successfully!")
        print(f"Table: {config.table_full_name}")
        print(f"Debug mode: {config.app_settings['debug']}")
    except Exception as e:
        print(f"Configuration error: {e}")
import streamlit as st
import pandas as pd
from databricks import sql
import os
from datetime import datetime
import hashlib
import json

# Page configuration
st.set_page_config(
    page_title="Databricks Unity Catalog CRUD App",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
@st.cache_data
def get_config():
    """Load configuration for Databricks connection"""
    return {
        "server_hostname": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
        "access_token": os.getenv("DATABRICKS_TOKEN"),
        "catalog": os.getenv("UNITY_CATALOG_NAME", "main"),
        "schema": os.getenv("UNITY_SCHEMA_NAME", "default"),
        "table": os.getenv("UNITY_TABLE_NAME", "user_data")
    }

class DatabaseManager:
    """Manages database connections and operations with row-level security"""
    
    def __init__(self, config):
        self.config = config
        self.connection = None
        
    def get_connection(self):
        """Get or create database connection"""
        if self.connection is None:
            try:
                self.connection = sql.connect(
                    server_hostname=self.config["server_hostname"],
                    http_path=self.config["http_path"],
                    access_token=self.config["access_token"]
                )
            except Exception as e:
                st.error(f"Failed to connect to Databricks: {str(e)}")
                return None
        return self.connection
    
    def execute_query(self, query, parameters=None):
        """Execute a query with optional parameters"""
        conn = self.get_connection()
        if conn is None:
            return None
            
        try:
            with conn.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                
                if query.strip().upper().startswith('SELECT'):
                    return cursor.fetchall()
                else:
                    conn.commit()
                    return True
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            return None
    
    def get_current_user(self):
        """Get current user from Databricks context"""
        try:
            query = "SELECT current_user()"
            result = self.execute_query(query)
            if result and len(result) > 0:
                return result[0][0]
            return "unknown_user"
        except:
            return "unknown_user"
    
    def create_record(self, data, user_id):
        """Create a new record with user ownership"""
        table_name = f"{self.config['catalog']}.{self.config['schema']}.{self.config['table']}"
        
        # Add user ownership and timestamp
        data['owner_user'] = user_id
        data['created_at'] = datetime.now()
        data['updated_at'] = datetime.now()
        
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data.values()])
        
        query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        """
        
        return self.execute_query(query, list(data.values()))
    
    def read_records(self, user_id, filters=None):
        """Read records with row-level security (user can only see their own data)"""
        table_name = f"{self.config['catalog']}.{self.config['schema']}.{self.config['table']}"
        
        base_query = f"""
        SELECT * FROM {table_name}
        WHERE owner_user = ?
        """
        
        params = [user_id]
        
        if filters:
            for column, value in filters.items():
                if value:
                    base_query += f" AND {column} LIKE ?"
                    params.append(f"%{value}%")
        
        base_query += " ORDER BY updated_at DESC"
        
        result = self.execute_query(base_query, params)
        if result:
            # Get column names
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table_name}")
                columns = [row[0] for row in cursor.fetchall()]
            
            return pd.DataFrame(result, columns=columns)
        return pd.DataFrame()
    
    def update_record(self, record_id, data, user_id):
        """Update a record (user can only update their own records)"""
        table_name = f"{self.config['catalog']}.{self.config['schema']}.{self.config['table']}"
        
        # Add updated timestamp
        data['updated_at'] = datetime.now()
        
        set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
        
        query = f"""
        UPDATE {table_name}
        SET {set_clause}
        WHERE id = ? AND owner_user = ?
        """
        
        params = list(data.values()) + [record_id, user_id]
        return self.execute_query(query, params)
    
    def delete_record(self, record_id, user_id):
        """Delete a record (user can only delete their own records)"""
        table_name = f"{self.config['catalog']}.{self.config['schema']}.{self.config['table']}"
        
        query = f"""
        DELETE FROM {table_name}
        WHERE id = ? AND owner_user = ?
        """
        
        return self.execute_query(query, [record_id, user_id])

def get_user_session():
    """Get or create user session with identity"""
    if 'user_id' not in st.session_state:
        # In Databricks Apps, you can get the actual user context
        # For demo purposes, we'll use the database current_user()
        config = get_config()
        db_manager = DatabaseManager(config)
        current_user = db_manager.get_current_user()
        st.session_state.user_id = current_user
        st.session_state.session_hash = hashlib.md5(f"{current_user}_{datetime.now()}".encode()).hexdigest()[:8]
    
    return st.session_state.user_id

def main():
    """Main application"""
    st.title("🏢 Databricks Unity Catalog CRUD Application")
    st.markdown("---")
    
    # Get user session
    user_id = get_user_session()
    
    # Display user info in sidebar
    st.sidebar.title("User Information")
    st.sidebar.info(f"**User:** {user_id}")
    st.sidebar.info(f"**Session:** {st.session_state.get('session_hash', 'N/A')}")
    
    # Initialize database manager
    config = get_config()
    
    # Check if configuration is complete
    missing_config = [k for k, v in config.items() if not v and k in ['server_hostname', 'http_path', 'access_token']]
    if missing_config:
        st.error(f"Missing configuration: {', '.join(missing_config)}")
        st.info("Please set the following environment variables:")
        st.code("""
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=your-access-token
UNITY_CATALOG_NAME=main
UNITY_SCHEMA_NAME=default
UNITY_TABLE_NAME=user_data
        """)
        return
    
    db_manager = DatabaseManager(config)
    
    # Main navigation
    tab1, tab2, tab3, tab4 = st.tabs(["📊 View Data", "➕ Create Record", "✏️ Update Record", "🗑️ Delete Record"])
    
    with tab1:
        st.header("📊 Your Data")
        
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            name_filter = st.text_input("Filter by Name", key="name_filter")
        with col2:
            email_filter = st.text_input("Filter by Email", key="email_filter")
        
        filters = {}
        if name_filter:
            filters['name'] = name_filter
        if email_filter:
            filters['email'] = email_filter
        
        # Load and display data
        if st.button("🔄 Refresh Data"):
            st.session_state.data_cache = None
        
        if 'data_cache' not in st.session_state:
            with st.spinner("Loading your data..."):
                st.session_state.data_cache = db_manager.read_records(user_id, filters)
        
        data = st.session_state.data_cache
        
        if not data.empty:
            st.dataframe(data, use_container_width=True)
            st.info(f"Showing {len(data)} records that belong to you")
        else:
            st.info("No data found. Create some records to get started!")
    
    with tab2:
        st.header("➕ Create New Record")
        
        with st.form("create_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                name = st.text_input("Name *", placeholder="Enter full name")
                email = st.text_input("Email *", placeholder="Enter email address")
            
            with col2:
                department = st.selectbox("Department", ["IT", "HR", "Finance", "Marketing", "Operations"])
                status = st.selectbox("Status", ["Active", "Inactive", "Pending"])
            
            notes = st.text_area("Notes", placeholder="Additional notes...")
            
            submit = st.form_submit_button("Create Record", type="primary")
            
            if submit:
                if name and email:
                    record_data = {
                        'name': name,
                        'email': email,
                        'department': department,
                        'status': status,
                        'notes': notes
                    }
                    
                    with st.spinner("Creating record..."):
                        success = db_manager.create_record(record_data, user_id)
                    
                    if success:
                        st.success("✅ Record created successfully!")
                        st.session_state.data_cache = None  # Clear cache to refresh data
                    else:
                        st.error("❌ Failed to create record")
                else:
                    st.error("Please fill in all required fields (marked with *)")
    
    with tab3:
        st.header("✏️ Update Record")
        
        # Load current data for selection
        current_data = db_manager.read_records(user_id)
        
        if not current_data.empty:
            # Record selection
            record_options = {f"{row['name']} ({row['email']})": row['id'] 
                            for _, row in current_data.iterrows()}
            
            selected_record_label = st.selectbox("Select Record to Update", 
                                                options=list(record_options.keys()))
            
            if selected_record_label:
                selected_id = record_options[selected_record_label]
                selected_record = current_data[current_data['id'] == selected_id].iloc[0]
                
                with st.form("update_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        name = st.text_input("Name *", value=selected_record.get('name', ''))
                        email = st.text_input("Email *", value=selected_record.get('email', ''))
                    
                    with col2:
                        department = st.selectbox("Department", 
                                                ["IT", "HR", "Finance", "Marketing", "Operations"],
                                                index=["IT", "HR", "Finance", "Marketing", "Operations"].index(selected_record.get('department', 'IT')))
                        status = st.selectbox("Status", 
                                            ["Active", "Inactive", "Pending"],
                                            index=["Active", "Inactive", "Pending"].index(selected_record.get('status', 'Active')))
                    
                    notes = st.text_area("Notes", value=selected_record.get('notes', ''))
                    
                    update = st.form_submit_button("Update Record", type="primary")
                    
                    if update:
                        if name and email:
                            update_data = {
                                'name': name,
                                'email': email,
                                'department': department,
                                'status': status,
                                'notes': notes
                            }
                            
                            with st.spinner("Updating record..."):
                                success = db_manager.update_record(selected_id, update_data, user_id)
                            
                            if success:
                                st.success("✅ Record updated successfully!")
                                st.session_state.data_cache = None  # Clear cache
                            else:
                                st.error("❌ Failed to update record")
                        else:
                            st.error("Please fill in all required fields (marked with *)")
        else:
            st.info("No records available to update. Create some records first!")
    
    with tab4:
        st.header("🗑️ Delete Record")
        
        current_data = db_manager.read_records(user_id)
        
        if not current_data.empty:
            # Record selection for deletion
            record_options = {f"{row['name']} ({row['email']})": row['id'] 
                            for _, row in current_data.iterrows()}
            
            selected_record_label = st.selectbox("Select Record to Delete", 
                                                options=list(record_options.keys()),
                                                key="delete_select")
            
            if selected_record_label:
                selected_id = record_options[selected_record_label]
                selected_record = current_data[current_data['id'] == selected_id].iloc[0]
                
                # Show record details
                st.subheader("Record Details")
                col1, col2 = st.columns(2)
                with col1:
                    st.text(f"Name: {selected_record.get('name', 'N/A')}")
                    st.text(f"Email: {selected_record.get('email', 'N/A')}")
                with col2:
                    st.text(f"Department: {selected_record.get('department', 'N/A')}")
                    st.text(f"Status: {selected_record.get('status', 'N/A')}")
                
                # Confirmation
                st.warning("⚠️ This action cannot be undone!")
                confirm = st.checkbox("I understand that this will permanently delete the record")
                
                if st.button("🗑️ Delete Record", type="secondary", disabled=not confirm):
                    with st.spinner("Deleting record..."):
                        success = db_manager.delete_record(selected_id, user_id)
                    
                    if success:
                        st.success("✅ Record deleted successfully!")
                        st.session_state.data_cache = None  # Clear cache
                    else:
                        st.error("❌ Failed to delete record")
        else:
            st.info("No records available to delete.")

if __name__ == "__main__":
    main()
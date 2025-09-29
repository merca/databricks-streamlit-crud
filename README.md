# Databricks Unity Catalog CRUD Application

A Streamlit application designed for Databricks Apps that provides CRUD (Create, Read, Update, Delete) operations on Unity Catalog tables with built-in row-level security and user session management.

## Features

- 🏢 **Unity Catalog Integration**: Full integration with Databricks Unity Catalog
- 🔐 **Row-Level Security**: Users can only access their own data
- 👤 **User Identification**: Automatic user identification through Databricks session
- 📊 **CRUD Operations**: Complete Create, Read, Update, Delete functionality
- 🎨 **Modern UI**: Clean, intuitive Streamlit interface
- 🔍 **Data Filtering**: Built-in search and filter capabilities
- ⚡ **Optimized Performance**: Caching and optimized queries

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Streamlit     │    │   Databricks     │    │  Unity Catalog  │
│      App        │◄──►│   SQL Warehouse  │◄──►│     Table       │
│                 │    │                  │    │  (Row-Level     │
│  - User Session │    │  - Authentication│    │   Security)     │
│  - CRUD UI      │    │  - Query Engine  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **SQL Warehouse** running in your workspace
3. **Personal Access Token** or service principal credentials
4. **Python 3.8+** for local development

## Setup Instructions

### 1. Database Setup

First, create the Unity Catalog table with row-level security:

1. Open Databricks SQL or a notebook in your workspace
2. Run the SQL script from `setup_table.sql`:

```sql
-- This creates the table, row filter function, and applies row-level security
-- See setup_table.sql for the complete script
```

### 2. Application Configuration

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Fill in your Databricks connection details in `.env`:
   ```bash
   DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
   DATABRICKS_TOKEN=your-access-token
   ```

### 3. Local Development

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the application:
   ```bash
   streamlit run app.py
   ```

### 4. Deployment to Databricks Apps

#### Option A: Using Databricks Apps UI

1. Go to your Databricks workspace
2. Navigate to **Apps** in the sidebar
3. Click **Create App**
4. Choose **Streamlit** as the app type
5. Upload your application files:
   - `app.py` (main application)
   - `requirements.txt` (dependencies)
   - `.env` (configuration - ensure no sensitive data is exposed)

#### Option B: Using Databricks CLI

1. Install Databricks CLI:
   ```bash
   pip install databricks-cli
   ```

2. Configure CLI with your workspace:
   ```bash
   databricks configure --token
   ```

3. Deploy the app:
   ```bash
   databricks apps create streamlit-crud-app ./
   ```

## Configuration Options

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DATABRICKS_SERVER_HOSTNAME` | Your workspace hostname | Yes | - |
| `DATABRICKS_HTTP_PATH` | SQL Warehouse HTTP path | Yes | - |
| `DATABRICKS_TOKEN` | Access token | Yes | - |
| `UNITY_CATALOG_NAME` | Catalog name | No | `main` |
| `UNITY_SCHEMA_NAME` | Schema name | No | `default` |
| `UNITY_TABLE_NAME` | Table name | No | `user_data` |

### Getting Connection Details

1. **Server Hostname**: Found in workspace URL (e.g., `your-workspace.cloud.databricks.com`)
2. **HTTP Path**: Go to SQL Warehouses → Select your warehouse → Connection Details
3. **Access Token**: User Settings → Developer → Access Tokens → Generate New Token

## Security Features

### Row-Level Security Implementation

The application implements row-level security through:

1. **Owner Column**: Each record has an `owner_user` field identifying the data owner
2. **Row Filter Function**: SQL function that filters data based on `current_user()`
3. **Application Logic**: All CRUD operations enforce user ownership
4. **Database Constraints**: Table-level row filters ensure data isolation

### User Session Management

- Users are automatically identified through Databricks session context
- Session state is maintained throughout the application lifecycle
- All operations are scoped to the authenticated user

## Usage Guide

### Creating Records
1. Navigate to the **Create Record** tab
2. Fill in the required fields (Name, Email)
3. Select department and status
4. Add optional notes
5. Click **Create Record**

### Viewing Data
1. Go to **View Data** tab to see your records
2. Use filters to search by name or email
3. Click **Refresh Data** to reload

### Updating Records
1. Select **Update Record** tab
2. Choose a record from the dropdown
3. Modify the fields as needed
4. Click **Update Record**

### Deleting Records
1. Navigate to **Delete Record** tab
2. Select the record to delete
3. Review the record details
4. Check the confirmation box
5. Click **Delete Record**

## Data Schema

The `user_data` table has the following structure:

```sql
CREATE TABLE main.default.user_data (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY,
    name STRING NOT NULL,
    email STRING NOT NULL,
    department STRING NOT NULL,
    status STRING NOT NULL,
    notes STRING,
    owner_user STRING NOT NULL,  -- For row-level security
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify your server hostname and HTTP path
   - Check if your access token is valid and has necessary permissions
   - Ensure your SQL Warehouse is running

2. **Permission Denied**
   - Verify Unity Catalog permissions are granted
   - Check if the row filter function has proper permissions
   - Ensure your user has access to the catalog and schema

3. **Table Not Found**
   - Run the `setup_table.sql` script first
   - Verify the catalog, schema, and table names in your configuration
   - Check if Unity Catalog is enabled in your workspace

### Debug Mode

Set `APP_DEBUG=true` in your `.env` file to enable additional logging and error information.

## Performance Considerations

- Data is cached in session state to reduce database queries
- Queries are optimized with proper WHERE clauses
- Delta table auto-optimization is enabled
- Consider partitioning for large datasets

## Security Best Practices

1. **Never commit `.env` files** with real credentials to version control
2. **Use service principals** for production deployments instead of personal tokens
3. **Grant minimal permissions** - only what's needed for the application
4. **Regularly rotate access tokens** and credentials
5. **Monitor application usage** through Databricks audit logs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Support

For issues and questions:
- Check the troubleshooting section above
- Review Databricks documentation for Unity Catalog and Apps
- Create an issue in the repository

## License

This project is licensed under the MIT License - see the LICENSE file for details.
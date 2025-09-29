# Databricks App Deployment Guide

This guide explains how to deploy the Unity Catalog CRUD Streamlit application using Databricks Asset Bundles (DAB).

## Prerequisites

1. **Databricks CLI** installed and configured
2. **Unity Catalog** enabled in your workspace
3. **SQL Warehouse** created and running
4. **Appropriate permissions** to create apps and tables

## Installation

### 1. Install Databricks CLI

```bash
# Windows (using pip)
pip install databricks-cli

# Or using Homebrew (macOS/Linux)
brew install databricks-cli
```

### 2. Authenticate with Databricks

```bash
databricks configure --token
```

Enter your workspace URL and personal access token when prompted.

### 3. Install DAB (if not already installed)

The Databricks CLI v0.200.0+ includes DAB by default. Verify installation:

```bash
databricks bundle --help
```

## Configuration

### 1. Update Workspace Configuration

Edit `databricks.yml` and update the workspace host URLs for your environments:

```yaml
targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com  # Replace with your workspace URL
  staging:
    workspace:
      host: https://your-workspace.cloud.databricks.com  # Replace with your workspace URL  
  prod:
    workspace:
      host: https://your-workspace.cloud.databricks.com  # Replace with your workspace URL
```

### 2. Set Required Variables

Create a `configs/dev.json` file (optional, for environment-specific overrides):

```json
{
  "variables": {
    "catalog": "main_dev",
    "schema": "my_schema", 
    "table_name": "user_data",
    "warehouse_id": "your-warehouse-id"
  }
}
```

You can get your warehouse ID from:
- Databricks SQL → Warehouses → Select your warehouse → Connection details

## Deployment Commands

### Development Environment

```bash
# Validate the bundle configuration
databricks bundle validate --target dev

# Deploy to development environment
databricks bundle deploy --target dev

# Run the database setup job (first time only)
databricks bundle run setup_database_job --target dev
```

### Staging Environment

```bash
# Deploy to staging
databricks bundle deploy --target staging

# Run setup if needed
databricks bundle run setup_database_job --target staging
```

### Production Environment

```bash
# Deploy to production (requires service principal configuration)
databricks bundle deploy --target prod

# Run setup
databricks bundle run setup_database_job --target prod
```

## Post-Deployment Steps

### 1. Verify App Deployment

After deployment, your Streamlit app will be available in:
- Databricks workspace → Apps section
- App name: "Unity Catalog CRUD - {catalog}.{schema}"

### 2. Test the Application

1. Open the app from the Databricks Apps section
2. Verify you can:
   - View existing data (if any)
   - Create new records
   - Update existing records
   - Delete records
   - Confirm row-level security (you only see your own data)

### 3. Monitor the Application

- Check app logs in Databricks workspace
- Monitor performance and usage
- Set up alerts if needed

## Environment Variables

The following environment variables are automatically configured by DAB:

| Variable | Description | Default |
|----------|-------------|---------|
| `UNITY_CATALOG_NAME` | Unity Catalog name | `main` |
| `UNITY_SCHEMA_NAME` | Schema name | `default` |
| `UNITY_TABLE_NAME` | Table name | `user_data` |
| `APP_DEBUG` | Debug mode | `false` |
| `CONNECTION_TIMEOUT` | DB connection timeout | `30` |
| `MAX_RETRIES` | Max retry attempts | `3` |

## Troubleshooting

### Common Issues

1. **Bundle validation fails**
   ```bash
   # Check your YAML syntax
   databricks bundle validate --target dev
   ```

2. **Deployment fails due to permissions**
   - Ensure your user/service principal has necessary permissions
   - Check Unity Catalog permissions
   - Verify SQL Warehouse access

3. **App fails to start**
   - Check app logs in Databricks workspace
   - Verify environment variables are set correctly
   - Ensure Unity Catalog table exists and is accessible

4. **Database setup job fails**
   - Verify SQL Warehouse is running
   - Check Unity Catalog permissions
   - Review the setup_table.sql script

### Useful Commands

```bash
# Show bundle configuration
databricks bundle show --target dev

# List deployed resources
databricks bundle resources --target dev

# View app logs
databricks apps logs unity_catalog_crud_app

# Destroy deployment (careful!)
databricks bundle destroy --target dev
```

## File Structure

```
.
├── databricks.yml           # Main DAB configuration
├── resources/
│   ├── app.yml              # Streamlit app configuration
│   └── tables.yml           # Unity Catalog table setup
├── app.py                   # Main Streamlit application
├── config.py                # Configuration helper
├── requirements.txt         # Python dependencies
├── setup_table.sql         # Unity Catalog table setup
└── DEPLOYMENT.md           # This deployment guide
```

## Production Considerations

### Security

1. **Use Service Principals** for production deployments
2. **Implement proper RBAC** with Unity Catalog
3. **Enable audit logging** for compliance
4. **Regular security reviews** of permissions

### Monitoring

1. **Set up monitoring** for app health and performance
2. **Configure alerts** for failures
3. **Monitor resource usage** and costs
4. **Regular backup** of Unity Catalog data

### Maintenance

1. **Regular updates** of dependencies
2. **Monitor Databricks platform updates**
3. **Test deployments** in staging before production
4. **Document any customizations**

## Support

For issues with:
- **DAB deployment**: Check Databricks documentation
- **Unity Catalog**: Review Unity Catalog documentation  
- **Streamlit app**: Check app logs and application code
- **Permissions**: Contact your Databricks administrator

## Next Steps

After successful deployment:
1. Set up monitoring and alerts
2. Configure backup procedures
3. Plan for scaling if needed
4. Train users on the application
5. Consider additional features or integrations
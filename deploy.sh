#!/bin/bash

# Databricks Unity Catalog CRUD App Deployment Script
# Bash script for deploying the Streamlit application using DAB

set -e  # Exit on error

# Default values
TARGET=""
SETUP_DATABASE=false
VALIDATE=false
DESTROY=false

# Function to display usage
usage() {
    echo "🚀 Databricks Unity Catalog CRUD App Deployment"
    echo ""
    echo "Usage: $0 -t TARGET [OPTIONS]"
    echo ""
    echo "Required:"
    echo "  -t, --target TARGET     Target environment (dev, staging, prod)"
    echo ""
    echo "Options:"
    echo "  -s, --setup-database    Set up Unity Catalog table after deployment"
    echo "  -v, --validate          Only validate the bundle configuration"
    echo "  -d, --destroy           Destroy the deployment"
    echo "  -h, --help              Display this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -t dev                       # Deploy to dev environment"
    echo "  $0 -t dev -s                    # Deploy and setup database"
    echo "  $0 -t prod -v                   # Validate prod configuration"
    echo "  $0 -t dev -d                    # Destroy dev deployment"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)
            TARGET="$2"
            shift 2
            ;;
        -s|--setup-database)
            SETUP_DATABASE=true
            shift
            ;;
        -v|--validate)
            VALIDATE=true
            shift
            ;;
        -d|--destroy)
            DESTROY=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "❌ Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate target parameter
if [[ -z "$TARGET" ]]; then
    echo "❌ Target environment is required"
    usage
    exit 1
fi

if [[ "$TARGET" != "dev" && "$TARGET" != "staging" && "$TARGET" != "prod" ]]; then
    echo "❌ Target must be one of: dev, staging, prod"
    exit 1
fi

echo "🚀 Databricks Unity Catalog CRUD App Deployment"
echo "Target Environment: $TARGET"

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI not found. Please install it first:"
    echo "   pip install databricks-cli"
    exit 1
fi

CLI_VERSION=$(databricks --version)
echo "✅ Databricks CLI found: $CLI_VERSION"

# Validate bundle configuration
if [[ "$VALIDATE" == true ]] || [[ -n "$TARGET" ]]; then
    echo "📋 Validating bundle configuration..."
    if databricks bundle validate --target "$TARGET"; then
        echo "✅ Bundle configuration is valid"
    else
        echo "❌ Bundle validation failed"
        exit 1
    fi
fi

# Handle validation-only request
if [[ "$VALIDATE" == true ]]; then
    echo "✅ Validation completed successfully"
    exit 0
fi

# Destroy deployment if requested
if [[ "$DESTROY" == true ]]; then
    echo "⚠️  WARNING: This will destroy the deployment!"
    read -p "Are you sure you want to destroy the $TARGET deployment? (yes/no): " confirm
    if [[ "$confirm" == "yes" ]]; then
        echo "🗑️  Destroying deployment..."
        databricks bundle destroy --target "$TARGET"
        echo "✅ Deployment destroyed"
    else
        echo "❌ Deployment destruction cancelled"
    fi
    exit 0
fi

# Deploy the application
echo "📦 Deploying application to $TARGET environment..."
if databricks bundle deploy --target "$TARGET"; then
    echo "✅ Application deployed successfully"
else
    echo "❌ Deployment failed"
    exit 1
fi

# Set up database if requested
if [[ "$SETUP_DATABASE" == true ]]; then
    echo "🗄️  Setting up Unity Catalog table..."
    if databricks bundle run setup_database_job --target "$TARGET"; then
        echo "✅ Database setup completed"
    else
        echo "❌ Database setup failed"
        echo "💡 You may need to manually create the Unity Catalog table"
    fi
fi

# Display post-deployment information
echo ""
echo "🎉 Deployment completed!"
echo ""
echo "Next steps:"
echo "1. Go to your Databricks workspace"
echo "2. Navigate to Apps section"
echo "3. Look for 'Unity Catalog CRUD' app"
echo "4. Test the application functionality"
echo ""
echo "Useful commands:"
echo "  databricks bundle show --target $TARGET"
echo "  databricks bundle resources --target $TARGET"
echo "  databricks apps logs unity_catalog_crud_app"
echo ""

# Check if this is a first-time deployment
if [[ "$SETUP_DATABASE" != true ]]; then
    echo "💡 If this is your first deployment, run:"
    echo "   ./deploy.sh -t $TARGET -s"
    echo ""
fi
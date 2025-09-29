# Databricks Unity Catalog CRUD App Deployment Script
# PowerShell script for deploying the Streamlit application using DAB

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("dev", "staging", "prod")]
    [string]$Target,
    
    [Parameter(Mandatory=$false)]
    [switch]$SetupDatabase,
    
    [Parameter(Mandatory=$false)]
    [switch]$Validate,
    
    [Parameter(Mandatory=$false)]
    [switch]$Destroy
)

Write-Host "🚀 Databricks Unity Catalog CRUD App Deployment" -ForegroundColor Green
Write-Host "Target Environment: $Target" -ForegroundColor Yellow

# Check if Databricks CLI is installed
try {
    $cliVersion = databricks --version
    Write-Host "✅ Databricks CLI found: $cliVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Databricks CLI not found. Please install it first:" -ForegroundColor Red
    Write-Host "   pip install databricks-cli" -ForegroundColor White
    exit 1
}

# Validate bundle configuration
if ($Validate -or $Target) {
    Write-Host "📋 Validating bundle configuration..." -ForegroundColor Blue
    try {
        databricks bundle validate --target $Target
        Write-Host "✅ Bundle configuration is valid" -ForegroundColor Green
    } catch {
        Write-Host "❌ Bundle validation failed" -ForegroundColor Red
        exit 1
    }
}

# Destroy deployment if requested
if ($Destroy) {
    Write-Host "⚠️  WARNING: This will destroy the deployment!" -ForegroundColor Red
    $confirm = Read-Host "Are you sure you want to destroy the $Target deployment? (yes/no)"
    if ($confirm -eq "yes") {
        Write-Host "🗑️  Destroying deployment..." -ForegroundColor Red
        databricks bundle destroy --target $Target
        Write-Host "✅ Deployment destroyed" -ForegroundColor Green
    } else {
        Write-Host "❌ Deployment destruction cancelled" -ForegroundColor Yellow
    }
    exit 0
}

# Deploy the application
Write-Host "📦 Deploying application to $Target environment..." -ForegroundColor Blue
try {
    databricks bundle deploy --target $Target
    Write-Host "✅ Application deployed successfully" -ForegroundColor Green
} catch {
    Write-Host "❌ Deployment failed" -ForegroundColor Red
    exit 1
}

# Set up database if requested
if ($SetupDatabase) {
    Write-Host "🗄️  Setting up Unity Catalog table..." -ForegroundColor Blue
    try {
        databricks bundle run setup_database_job --target $Target
        Write-Host "✅ Database setup completed" -ForegroundColor Green
    } catch {
        Write-Host "❌ Database setup failed" -ForegroundColor Red
        Write-Host "💡 You may need to manually create the Unity Catalog table" -ForegroundColor Yellow
    }
}

# Display post-deployment information
Write-Host ""
Write-Host "🎉 Deployment completed!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Go to your Databricks workspace" -ForegroundColor White
Write-Host "2. Navigate to Apps section" -ForegroundColor White
Write-Host "3. Look for 'Unity Catalog CRUD' app" -ForegroundColor White
Write-Host "4. Test the application functionality" -ForegroundColor White
Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Yellow
Write-Host "  databricks bundle show --target $Target" -ForegroundColor White
Write-Host "  databricks bundle resources --target $Target" -ForegroundColor White
Write-Host "  databricks apps logs unity_catalog_crud_app" -ForegroundColor White
Write-Host ""

# Check if this is a first-time deployment
if (-not $SetupDatabase) {
    Write-Host "💡 If this is your first deployment, run:" -ForegroundColor Blue
    Write-Host "   .\deploy.ps1 -Target $Target -SetupDatabase" -ForegroundColor White
    Write-Host ""
}
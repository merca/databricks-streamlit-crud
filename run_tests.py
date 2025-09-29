#!/usr/bin/env python3
"""
Test runner script for the Databricks Unity Catalog CRUD Application.

This script provides a convenient way to run tests locally with different options.
"""

import subprocess
import sys
import os
import argparse


def run_command(command, description):
    """Run a shell command and handle errors."""
    print(f"\n{'='*50}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(command)}")
    print(f"{'='*50}")
    
    try:
        result = subprocess.run(command, check=True, capture_output=False)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"❌ Command not found: {command[0]}")
        print("Please ensure pytest is installed: pip install -r requirements.txt")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run tests for the Databricks Unity Catalog CRUD Application")
    parser.add_argument("--coverage", action="store_true", help="Run tests with coverage report")
    parser.add_argument("--html", action="store_true", help="Generate HTML test and coverage reports")
    parser.add_argument("--unit", action="store_true", help="Run only unit tests")
    parser.add_argument("--config", action="store_true", help="Run only configuration tests")
    parser.add_argument("--database", action="store_true", help="Run only database tests")
    parser.add_argument("--ui", action="store_true", help="Run only UI tests")
    parser.add_argument("--verbose", "-v", action="store_true", help="Run tests in verbose mode")
    parser.add_argument("--parallel", "-p", action="store_true", help="Run tests in parallel")
    parser.add_argument("--install-deps", action="store_true", help="Install dependencies before running tests")
    
    args = parser.parse_args()
    
    # Ensure we're in the correct directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    success = True
    
    # Install dependencies if requested
    if args.install_deps:
        if not run_command([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                          "Installing dependencies"):
            return False
    
    # Create test reports directory
    os.makedirs("tests/reports", exist_ok=True)
    
    # Build pytest command
    pytest_cmd = ["pytest"]
    
    # Add verbosity
    if args.verbose:
        pytest_cmd.append("-v")
    
    # Add parallel execution
    if args.parallel:
        pytest_cmd.extend(["-n", "auto"])
    
    # Add test selection based on markers
    if args.unit:
        pytest_cmd.extend(["-m", "unit"])
    elif args.config:
        pytest_cmd.extend(["-m", "config"])
    elif args.database:
        pytest_cmd.extend(["-m", "database"])
    elif args.ui:
        pytest_cmd.extend(["-m", "ui"])
    
    # Add coverage options
    if args.coverage:
        pytest_cmd.extend([
            "--cov=.",
            "--cov-report=term-missing",
            "--cov-report=xml:coverage.xml"
        ])
        
        if args.html:
            pytest_cmd.append("--cov-report=html:htmlcov")
    
    # Add HTML test report
    if args.html:
        pytest_cmd.extend([
            "--html=tests/reports/report.html",
            "--self-contained-html"
        ])
    
    # Add test path
    pytest_cmd.append("tests/")
    
    # Run the tests
    if not run_command(pytest_cmd, "Running tests"):
        success = False
    
    # Print summary
    print(f"\n{'='*60}")
    if success:
        print("🎉 All tests completed successfully!")
        
        if args.coverage or args.html:
            print("\nGenerated reports:")
            if args.coverage and args.html:
                print("  📊 Coverage report: htmlcov/index.html")
            if args.html:
                print("  📋 Test report: tests/reports/report.html")
            if args.coverage:
                print("  📈 Coverage XML: coverage.xml")
    else:
        print("❌ Some tests failed. Please check the output above.")
        return False
    
    print(f"{'='*60}")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
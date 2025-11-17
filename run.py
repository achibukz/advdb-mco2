#!/usr/bin/env python3
"""
Cross-platform launcher for Financial Dataset Dashboard
Works on Windows, macOS, and Linux
"""

import os
import sys
import subprocess
import platform
import time
from pathlib import Path


def print_header():
    print("=" * 48)
    print("Financial Dataset Dashboard Launcher")
    print("=" * 48)
    print()


def check_env_file():
    """Check if .env file exists and contains content"""
    env_file = Path(".env")
    
    if not env_file.exists():
        print()
        print("[ERROR] .env file not found!")
        print()
        print("Please create a .env file in the root folder and fill it with the required configuration.")
        print()
        input("Press Enter to exit...")
        sys.exit(1)
    
    if env_file.stat().st_size == 0:
        print()
        print("[ERROR] .env file is empty!")
        print()
        print("Please fill the .env file with the required configuration information.")
        print()
        input("Press Enter to exit...")
        sys.exit(1)
    
    print(".env file found and contains configuration.")
    print()


def get_python_command():
    """Get the appropriate Python command for the platform"""
    if platform.system() == "Windows":
        return "python"
    else:
        return "python3"


def get_venv_path():
    """Get the appropriate virtual environment activation path"""
    if platform.system() == "Windows":
        return Path(".venv") / "Scripts"
    else:
        return Path(".venv") / "bin"


def get_python_executable():
    """Get the Python executable in the virtual environment"""
    venv_path = get_venv_path()
    if platform.system() == "Windows":
        return venv_path / "python.exe"
    else:
        return venv_path / "python"


def get_pip_executable():
    """Get the pip executable in the virtual environment"""
    venv_path = get_venv_path()
    if platform.system() == "Windows":
        return venv_path / "pip.exe"
    else:
        return venv_path / "pip"


def create_venv():
    """Create virtual environment"""
    venv_path = Path(".venv")
    
    if not venv_path.exists():
        print("Virtual environment not found!")
        print("Creating virtual environment...")
        
        python_cmd = get_python_command()
        result = subprocess.run([python_cmd, "-m", "venv", ".venv"])
        
        if result.returncode != 0:
            print("Failed to create virtual environment.")
            input("Press Enter to exit...")
            sys.exit(1)
        
        print("Virtual environment created successfully.")
        print()


def install_dependencies():
    """Check and install dependencies"""
    print("Checking dependencies...")
    
    pip_executable = str(get_pip_executable())
    
    # Check if streamlit is installed
    result = subprocess.run(
        [pip_executable, "show", "streamlit"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    if result.returncode != 0:
        print("Installing required packages...")
        result = subprocess.run([pip_executable, "install", "-r", "python/requirements.txt"])
        
        if result.returncode != 0:
            print("Failed to install dependencies.")
            input("Press Enter to exit...")
            sys.exit(1)
        
        print("Dependencies installed successfully.")
        print()


def check_docker():
    """Check if Docker is running"""
    print("Checking Docker status...")
    
    result = subprocess.run(
        ["docker", "info"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    if result.returncode != 0:
        print()
        print("[ERROR] Docker Desktop is not running!")
        print()
        print("Please start Docker Desktop and wait for it to fully start,")
        print("then run this script again.")
        print()
        input("Press Enter to exit...")
        sys.exit(1)


def check_and_start_containers():
    """Check if Docker containers are running and start if needed"""
    print("Docker is running. Checking containers...")
    
    # Check if mysql-warehouse container is running
    result = subprocess.run(
        ["docker", "ps"],
        capture_output=True,
        text=True
    )
    
    if "mysql-warehouse" not in result.stdout:
        print("MySQL warehouse container is not running.")
        print("Starting Docker containers...")
        
        result = subprocess.run(["docker-compose", "up", "-d"])
        
        if result.returncode != 0:
            print()
            print("[ERROR] Failed to start Docker containers.")
            print("Please check docker-compose.yml and try again.")
            print("Check if any mysql instance is running, turn them off")
            print("in services.")
            print()
            input("Press Enter to exit...")
            sys.exit(1)
        
        print("Waiting for database to be ready...")
        time.sleep(15)
        print()
    else:
        print("Docker containers are already running.")
        print()


def run_streamlit():
    """Run the Streamlit application"""
    print("Starting Streamlit application...")
    print()
    print("=" * 48)
    print("Dashboard will open in your browser")
    print("Press Ctrl+C to stop the application")
    print("=" * 48)
    print()
    
    python_executable = str(get_python_executable())
    
    try:
        subprocess.run([python_executable, "-m", "streamlit", "run", "python/app.py"])
    except KeyboardInterrupt:
        print("\nApplication stopped by user.")


def main():
    print_header()
    check_env_file()
    create_venv()
    
    print("Activating virtual environment...")
    install_dependencies()
    check_docker()
    check_and_start_containers()
    run_streamlit()
    
    input("\nPress Enter to exit...")


if __name__ == "__main__":
    main()

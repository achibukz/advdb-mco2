#!/usr/bin/env python3
"""
Cross-platform launcher for Transaction Management
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
    print("Transaction Management Launcher")
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


def check_python_version():
    """Check if Python 3.13 is available"""
    python_cmd = "python" if platform.system() == "Windows" else "python3"

    try:
        result = subprocess.run(
            [python_cmd, "--version"],
            capture_output=True,
            text=True
        )

        version_output = result.stdout + result.stderr

        # Extract version number
        if "Python 3.13" in version_output:
            return python_cmd
        else:
            print()
            print(f"[ERROR] Python 3.13 is required, but found: {version_output.strip()}")
            print()
            print("Please install Python 3.13 and ensure it's in your PATH.")
            print()
            input("Press Enter to exit...")
            sys.exit(1)

    except FileNotFoundError:
        print()
        print(f"[ERROR] Python not found!")
        print()
        print("Please install Python 3.13 and ensure it's in your PATH.")
        print()
        input("Press Enter to exit...")
        sys.exit(1)


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
    """Create virtual environment with Python 3.13"""
    venv_path = Path(".venv")
    
    if not venv_path.exists():
        print("Virtual environment not found!")
        print("Creating virtual environment with Python 3.13...")

        python_cmd = check_python_version()
        result = subprocess.run([python_cmd, "-m", "venv", ".venv"])

        if result.returncode != 0:
            print("Failed to create virtual environment.")
            input("Press Enter to exit...")
            sys.exit(1)

        print("Virtual environment created successfully with Python 3.13.")
        print()
    else:
        # Verify existing venv is using Python 3.13
        python_executable = str(get_python_executable())
        result = subprocess.run(
            [python_executable, "--version"],
            capture_output=True,
            text=True
        )
        version_output = result.stdout + result.stderr

        if "Python 3.13" not in version_output:
            print()
            print(f"[WARNING] Existing virtual environment is not Python 3.13!")
            print(f"Found: {version_output.strip()}")
            print()
            print("Deleting old virtual environment and creating a new one with Python 3.13...")

            import shutil
            shutil.rmtree(venv_path)

            python_cmd = check_python_version()
            result = subprocess.run([python_cmd, "-m", "venv", ".venv"])

            if result.returncode != 0:
                print("Failed to create virtual environment.")
                input("Press Enter to exit...")
                sys.exit(1)

            print("Virtual environment created successfully with Python 3.13.")
            print()
        else:
            print(f"Virtual environment already exists with Python 3.13.")
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
        time.sleep(5)
        print()
    else:
        print("Docker containers are already running.")
        print()


def run_streamlit(node=None):
    """Run the Streamlit application

    Args:
        node (int, optional): Node number (1, 2, or 3) to connect to.
                             Only used for local development.
    """
    print("Starting Streamlit application...")
    if node:
        print(f"Connecting to Node {node}...")
    print()
    print("=" * 48)
    print("Dashboard will open in your browser")
    print("Press Ctrl+C to stop the application")
    print("=" * 48)
    print()
    
    python_executable = str(get_python_executable())
    
    # Set NODE_USE environment variable for local use only
    env = os.environ.copy()
    if node:
        env['NODE_USE'] = str(node)

    try:
        subprocess.run(
            [python_executable, "-m", "streamlit", "run", "python/gui/app.py"],
            env=env
        )
    except KeyboardInterrupt:
        print("\nApplication stopped by user.")


def main():
    print_header()

    # Parse command-line arguments for node selection (LOCAL USE ONLY)
    node = None
    if len(sys.argv) > 1:
        try:
            node = int(sys.argv[1])
            if node not in [1, 2, 3]:
                print()
                print("[ERROR] Invalid node number!")
                print()
                print("Usage: python run.py [node_number]")
                print("  node_number: 1, 2, or 3 (which database node to connect to)")
                print()
                print("Example: python run.py 1")
                print()
                print("Note: This argument is for LOCAL USE ONLY.")
                print("      For Streamlit Cloud deployment, set NODE_USE in secrets.toml")
                print()
                input("Press Enter to exit...")
                sys.exit(1)
            print(f"Node {node} selected via command-line argument (local use)")
            print()
        except ValueError:
            print()
            print("[ERROR] Invalid node number format!")
            print()
            print("Usage: python run.py [node_number]")
            print("  node_number: 1, 2, or 3 (which database node to connect to)")
            print()
            print("Example: python run.py 1")
            print()
            input("Press Enter to exit...")
            sys.exit(1)
    else:
        print("No node specified - will use default (Node 1) or NODE_USE from .env")
        print("Tip: Use 'python run.py [1|2|3]' to specify a node")
        print()

    check_env_file()
    create_venv()
    
    print("Activating virtual environment...")
    install_dependencies()
    check_docker()
    check_and_start_containers()
    run_streamlit(node)

    input("\nPress Enter to exit...")


if __name__ == "__main__":
    main()

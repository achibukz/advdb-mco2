"""
Quick Database Connection Tester - Non-Interactive

Usage:
    python quick_test.py 1       # Test local connections
    python quick_test.py 2       # Test cloud connections
    python quick_test.py         # Test current mode (from .env)
"""

import sys
import os

# Add parent directory to path to import db_config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_config import test_connection, get_node_config, USE_CLOUD_SQL
from dotenv import load_dotenv

# Reload environment variables
load_dotenv()


def test_with_mode(use_cloud):
    """Test connections with specified mode."""
    mode_name = "CLOUD SQL" if use_cloud else "LOCAL"
    
    # Temporarily set the mode
    original_value = os.environ.get('USE_CLOUD_SQL', 'False')
    os.environ['USE_CLOUD_SQL'] = 'True' if use_cloud else 'False'
    
    # Reload db_config to pick up the change
    import importlib
    import db_config
    importlib.reload(db_config)
    
    print("\n" + "="*60)
    print(f"TESTING {mode_name} DATABASE CONNECTIONS")
    print("="*60)
    
    results = {}
    for node in [1, 2, 3]:
        print(f"\n--- Node {node} ({mode_name}) ---")
        config = db_config.get_node_config(node)
        print(f"Host: {config['host']}:{config['port']}")
        print(f"User: {config['user']}")
        print(f"Database: {config['database']}")
        
        try:
            results[node] = db_config.test_connection(node)
        except Exception as e:
            print(f"Error: {str(e)}")
            results[node] = False
    
    # Restore original value
    os.environ['USE_CLOUD_SQL'] = original_value
    importlib.reload(db_config)
    
    print(f"\n{'='*60}")
    print(f"{mode_name} CONNECTIONS SUMMARY:")
    for node, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"  Node {node}: {status}")
    print("="*60 + "\n")
    
    return results


def main():
    """Main function with command-line argument support."""
    
    if len(sys.argv) > 1:
        choice = sys.argv[1]
        
        if choice == '1':
            print("Testing LOCAL connections...")
            test_with_mode(use_cloud=False)
        elif choice == '2':
            print("Testing CLOUD SQL connections...")
            test_with_mode(use_cloud=True)
        else:
            print(f"Invalid argument: {choice}")
            print("Usage:")
            print("  python quick_test.py 1    # Test local connections")
            print("  python quick_test.py 2    # Test cloud connections")
            sys.exit(1)
    else:
        # Test current mode from .env
        mode_name = "CLOUD SQL" if USE_CLOUD_SQL else "LOCAL"
        print(f"Testing current mode from .env: {mode_name}")
        test_with_mode(use_cloud=USE_CLOUD_SQL)


if __name__ == "__main__":
    main()


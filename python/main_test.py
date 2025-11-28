"""
Main Test Runner for Multi-Node Transaction Concurrency Testing
Provides menu-driven interface for running specific test cases
"""

import sys
import time
from pathlib import Path

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from test_scripts.test_runner import ConcurrencyTestSuite
from test_scripts.isolation_level_tests import IsolationLevelTests, run_all_isolation_tests
from utils.db_manager import DatabaseManager
from utils.logger import TransactionLogger

class MainTestRunner:
    def __init__(self):
        self.db = DatabaseManager()
        self.logger = TransactionLogger()
        
    def show_menu(self):
        """Display the main test menu"""
        print(f"\n{'='*100}")
        print(f"MULTI-NODE TRANSACTION CONCURRENCY TEST SUITE")
        print(f"{'='*100}")
        print()
        print("Test Configuration:")
        print("  - Nodes 1 & 2: Testing with trans_id = 1")
        print("  - Nodes 1 & 3: Testing with trans_id = 40845")
        print()
        print("Available Test Options:")
        print()
        print("INDIVIDUAL ISOLATION LEVEL TESTS:")
        print("  1) Test READ UNCOMMITTED isolation level")
        print("  2) Test READ COMMITTED isolation level") 
        print("  3) Test REPEATABLE READ isolation level")
        print("  4) Test SERIALIZABLE isolation level")
        print()
        print("COMPREHENSIVE TEST SUITES:")
        print("  5) Run ALL isolation level tests")
        print("  6) Run complete concurrency test suite")
        print()
        print("UTILITY FUNCTIONS:")
        print("  7) Test database connections")
        print("  8) Generate reports from existing logs")
        print("  9) Clear all test logs")
        print("  sql) Demonstrate SQL concurrency controls")
        print()
        print("  0) Exit")
        print(f"\n{'='*100}")
        
    def test_connections(self):
        """Test connections to all database nodes"""
        print("\nTesting Database Connections...")
        print("-" * 50)
        
        connection_status = self.db.test_connections()
        
        for node_id, status in connection_status.items():
            node_info = self.db.get_node_info(node_id)
            status_text = "PASS" if status else "FAIL"
            
            print(f"   Node {node_id}: {status_text}")
            print(f"      {node_info}")
            
            if status:
                # Try a sample query to verify functionality
                try:
                    result = self.db.execute_query(node_id, "SELECT COUNT(*) as count FROM trans LIMIT 1")
                    if result['success']:
                        count = result['results'][0]['count'] if result['results'] else 0
                        print(f"      Records available: {count}")
                    else:
                        print(f"      Query test failed: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    print(f"      Query test error: {e}")
            print()
        
        overall_status = all(connection_status.values())
        if overall_status:
            print("All database connections are working properly!")
        else:
            print("Some database connections failed. Please check your setup.")
            print("\nMake sure all Docker containers are running:")
            print("   docker-compose up -d")
        
        return overall_status
        
    def clear_logs(self):
        """Clear all test log files"""
        print("\nClearing test logs...")
        
        logs_dir = Path("logs")
        if logs_dir.exists():
            log_files = list(logs_dir.glob("*.json")) + list(logs_dir.glob("*.txt"))
            
            if log_files:
                for log_file in log_files:
                    try:
                        log_file.unlink()
                        print(f"   Deleted: {log_file.name}")
                    except Exception as e:
                        print(f"   Failed to delete {log_file.name}: {e}")
                
                print(f"\nCleared {len(log_files)} log files")
            else:
                print("   No log files found to clear")
        else:
            print("   Logs directory doesn't exist")
    
    def generate_reports(self):
        """Generate reports from existing test results"""
        print("\nGenerating reports from existing logs...")
        
        logs_dir = Path("logs")
        if not logs_dir.exists():
            print("   No logs directory found")
            return
            
        json_files = list(logs_dir.glob("*.json"))
        if not json_files:
            print("   No test result files found")
            return
            
        print(f"   Found {len(json_files)} test result files")
        
        # Load existing results into logger
        import json
        logger = TransactionLogger()
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    test_data = json.load(f)
                    
                # Convert string timestamps back to datetime objects for processing
                from datetime import datetime
                test_data['start_time'] = datetime.fromisoformat(test_data['start_time'])
                test_data['end_time'] = datetime.fromisoformat(test_data['end_time'])
                
                # Add to logger results
                logger.test_results[json_file.stem] = test_data
                
            except Exception as e:
                print(f"   Failed to load {json_file.name}: {e}")
        
        if logger.test_results:
            print(f"   Loaded {len(logger.test_results)} test results")
            
            # Generate comprehensive report
            logger.generate_comprehensive_report()
            
            # Generate individual isolation level reports
            isolation_levels = set(test['isolation_level'] for test in logger.test_results.values())
            for level in isolation_levels:
                logger.generate_isolation_level_report(level)
                
            print("Reports generated successfully!")
        else:
            print("   No valid test results could be loaded")
    
    def run_menu(self):
        """Run the interactive menu"""
        while True:
            self.show_menu()
            
            try:
                choice = input("Enter your choice (0-9): ").strip()
                
                if choice == '0':
                    print("\nGoodbye!")
                    break
                    
                elif choice == '1':
                    print("\nRunning READ UNCOMMITTED tests...")
                    if self.test_connections():
                        tester = IsolationLevelTests()
                        tester.test_read_uncommitted_isolation()
                        tester.db.cleanup_all_connections()
                    input("\nPress Enter to continue...")
                    
                elif choice == '2':
                    print("\nRunning READ COMMITTED tests...")
                    if self.test_connections():
                        tester = IsolationLevelTests()
                        tester.test_read_committed_isolation()
                        tester.db.cleanup_all_connections()
                    input("\nPress Enter to continue...")
                    
                elif choice == '3':
                    print("\nRunning REPEATABLE READ tests...")
                    if self.test_connections():
                        tester = IsolationLevelTests()
                        tester.test_repeatable_read_isolation()
                        tester.db.cleanup_all_connections()
                    input("\nPress Enter to continue...")
                    
                elif choice == '4':
                    print("\nRunning SERIALIZABLE tests...")
                    if self.test_connections():
                        tester = IsolationLevelTests()
                        tester.test_serializable_isolation()
                        tester.db.cleanup_all_connections()
                    input("\nPress Enter to continue...")
                    
                elif choice == '5':
                    print("\nRunning ALL isolation level tests...")
                    if self.test_connections():
                        run_all_isolation_tests()
                    input("\nPress Enter to continue...")
                    
                elif choice == '6':
                    print("\nRunning complete concurrency test suite...")
                    if self.test_connections():
                        suite = ConcurrencyTestSuite()
                        suite.run_all_tests()
                    input("\nPress Enter to continue...")
                    
                elif choice == '7':
                    self.test_connections()
                    input("\nPress Enter to continue...")
                    
                elif choice == '8':
                    self.generate_reports()
                    input("\nPress Enter to continue...")
                    
                elif choice == '9':
                    self.clear_logs()
                    input("\nPress Enter to continue...")
                    
                elif choice.lower() in ['sql', 'demo']:
                    print("\nRunning SQL Concurrency Demonstration...")
                    if self.test_connections():
                        from sql_concurrency_demo import demonstrate_sql_concurrency
                        demonstrate_sql_concurrency()
                    input("\nPress Enter to continue...")
                    
                else:
                    print("\nInvalid choice. Please enter a number between 0-9 or 'sql' for demo.")
                    input("Press Enter to continue...")
                    
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"\nAn error occurred: {e}")
                input("Press Enter to continue...")

def main():
    """Main entry point"""
    runner = MainTestRunner()
    
    if len(sys.argv) > 1:
        # Command line mode
        command = sys.argv[1].lower()
        
        if command == 'test-connections':
            runner.test_connections()
        elif command == 'read-uncommitted':
            if runner.test_connections():
                tester = IsolationLevelTests()
                tester.test_read_uncommitted_isolation()
        elif command == 'read-committed':
            if runner.test_connections():
                tester = IsolationLevelTests()
                tester.test_read_committed_isolation()
        elif command == 'repeatable-read':
            if runner.test_connections():
                tester = IsolationLevelTests()
                tester.test_repeatable_read_isolation()
        elif command == 'serializable':
            if runner.test_connections():
                tester = IsolationLevelTests()
                tester.test_serializable_isolation()
        elif command == 'all-isolation':
            if runner.test_connections():
                run_all_isolation_tests()
        elif command == 'full-suite':
            if runner.test_connections():
                suite = ConcurrencyTestSuite()
                suite.run_all_tests()
        elif command == 'clear-logs':
            runner.clear_logs()
        elif command == 'generate-reports':
            runner.generate_reports()
        elif command == 'sql-demo':
            if runner.test_connections():
                from sql_concurrency_demo import demonstrate_sql_concurrency
                demonstrate_sql_concurrency()
        else:
            print(f"Unknown command: {command}")
            print("Available commands:")
            print("  test-connections, read-uncommitted, read-committed,")
            print("  repeatable-read, serializable, all-isolation, full-suite,")
            print("  clear-logs, generate-reports, sql-demo")
    else:
        # Interactive menu mode
        runner.run_menu()

if __name__ == "__main__":
    main()
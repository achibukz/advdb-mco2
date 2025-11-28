"""
Comprehensive Transaction Concurrency Test Suite
Tests each isolation level with specific functions for concurrent transactions
"""

import threading
import time
from typing import Dict, List, Any
from utils.db_manager import DatabaseManager
from utils.logger import TransactionLogger

class ConcurrencyTestSuite:
    def __init__(self):
        self.db = DatabaseManager()
        self.logger = TransactionLogger()
        
        # Test transaction IDs as specified
        self.test_cases = {
            'nodes_1_2': {
                'nodes': [1, 2],
                'trans_id': 1,
                'description': 'Nodes 1 and 2 testing with trans_id = 1'
            },
            'nodes_1_3': {
                'nodes': [1, 3], 
                'trans_id': 40845,
                'description': 'Nodes 1 and 3 testing with trans_id = 40845'
            }
        }
        
        self.isolation_levels = [
            'READ UNCOMMITTED',
            'READ COMMITTED', 
            'REPEATABLE READ',
            'SERIALIZABLE'
        ]
        
    def run_all_tests(self):
        """Run all test cases for all isolation levels"""
        print("Starting Comprehensive Concurrency Test Suite")
        print("="*80)
        
        # Test connections first
        connection_status = self.db.test_connections()
        print("Testing Database Connections:")
        for node_id, status in connection_status.items():
            status_text = "Connected" if status else "Failed"
            print(f"   Node {node_id}: {status_text}")
        
        if not all(connection_status.values()):
            print("Some database connections failed. Please check your setup.")
            return
        
        print("\nRunning Tests for All Isolation Levels...\n")
        
        # Run tests for each isolation level
        for isolation_level in self.isolation_levels:
            print(f"\n{'='*100}")
            print(f"TESTING ISOLATION LEVEL: {isolation_level}")
            print(f"{'='*100}")
            
            # Run tests for each node pair
            for test_name, test_config in self.test_cases.items():
                self._run_isolation_level_tests(isolation_level, test_name, test_config)
                time.sleep(2)  # Brief pause between tests
            
            # Generate report for this isolation level
            self.logger.generate_isolation_level_report(isolation_level)
            time.sleep(1)
        
        # Generate comprehensive report
        self.logger.generate_comprehensive_report()
        
        # Cleanup
        self.db.cleanup_all_connections()
        print("\nAll tests completed!")
    
    def _run_isolation_level_tests(self, isolation_level: str, test_name: str, test_config: Dict):
        """Run all concurrency scenarios for a specific isolation level"""
        nodes = test_config['nodes']
        trans_id = test_config['trans_id']
        
        # Test Case 1: Concurrent Reads
        self._test_concurrent_reads(isolation_level, nodes, trans_id, test_name)
        time.sleep(1)
        
        # Test Case 2: Read-Write Concurrency  
        self._test_read_write_concurrency(isolation_level, nodes, trans_id, test_name)
        time.sleep(1)
        
        # Test Case 3: Concurrent Writes
        self._test_concurrent_writes(isolation_level, nodes, trans_id, test_name)
        time.sleep(1)
    
    def _test_concurrent_reads(self, isolation_level: str, nodes: List[int], 
                             trans_id: int, test_name: str):
        """Test Case 1: Concurrent transactions reading same data"""
        test_case_name = f"Case1_ConcurrentReads_{test_name}"
        
        self.logger.start_test_case(
            test_case_name, isolation_level, nodes, [trans_id]
        )
        
        # Set isolation level on both nodes
        for node_id in nodes:
            result = self.db.set_session_isolation_level(node_id, isolation_level)
            self.logger.log_transaction_step(
                f"SetIsolationLevel_Node{node_id}", node_id, 
                f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}",
                {'success': result, 'timestamp': time.strftime('%H:%M:%S.%f')[:-3], 'duration': 0.001},
                f"Setting isolation level for concurrent read test"
            )
        
        # Storage for results from both threads
        results = {}
        errors = {}
        
        def read_transaction(node_id: int, thread_name: str):
            """Execute read transaction on specified node"""
            connection_key = f"{test_case_name}_node{node_id}_read"
            
            try:
                # Start transaction
                start_result = self.db.start_transaction(node_id, connection_key)
                self.logger.log_transaction_step(
                    f"{thread_name}_StartTransaction", node_id, "START TRANSACTION",
                    start_result, f"Starting read transaction on Node {node_id}"
                )
                
                if not start_result['success']:
                    errors[thread_name] = f"Failed to start transaction: {start_result.get('error')}"
                    return
                
                # Read the transaction record multiple times with SQL delays
                for read_num in range(3):
                    if read_num > 0:
                        # Use SQL SLEEP between reads
                        sleep_query = f"SELECT SLEEP(0.5) as read_delay_{read_num}"
                        self.db.execute_query(node_id, sleep_query, connection_key=connection_key)
                    
                    read_result = self.db.get_transaction_record(node_id, trans_id, connection_key)
                    self.logger.log_transaction_step(
                        f"{thread_name}_Read{read_num + 1}", node_id, 
                        f"SELECT * FROM trans WHERE trans_id = {trans_id}",
                        read_result, f"Reading transaction record (attempt {read_num + 1}) with SQL timing"
                    )
                    
                    if read_result['success'] and read_result['results']:
                        if thread_name not in results:
                            results[thread_name] = []
                        results[thread_name].append(read_result['results'][0])
                
                # Commit transaction
                commit_result = self.db.commit_transaction(node_id, connection_key)
                self.logger.log_transaction_step(
                    f"{thread_name}_Commit", node_id, "COMMIT",
                    commit_result, f"Committing read transaction"
                )
                
            except Exception as e:
                errors[thread_name] = str(e)
                # Try to rollback
                try:
                    rollback_result = self.db.rollback_transaction(node_id, connection_key)
                    self.logger.log_transaction_step(
                        f"{thread_name}_Rollback", node_id, "ROLLBACK",
                        rollback_result, f"Rolling back due to error: {e}"
                    )
                except:
                    pass
            finally:
                self.db.close_persistent_connection(connection_key)
        
        # Create and start threads
        threads = []
        for i, node_id in enumerate(nodes):
            thread_name = f"ReadThread{i+1}_Node{node_id}"
            thread = threading.Thread(target=read_transaction, args=(node_id, thread_name))
            threads.append(thread)
        
        # Start all threads simultaneously
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Analyze results for consistency
        final_results = {
            'total_reads': sum(len(reads) for reads in results.values()),
            'errors': errors,
            'consistent_data': True
        }
        
        # Check for read consistency anomalies
        if len(results) >= 2:
            read_values = list(results.values())
            for i in range(len(read_values[0])):
                base_record = read_values[0][i] if i < len(read_values[0]) else None
                for j in range(1, len(read_values)):
                    compare_record = read_values[j][i] if i < len(read_values[j]) else None
                    
                    if base_record and compare_record:
                        if base_record['amount'] != compare_record['amount']:
                            self.logger.log_anomaly(
                                "Inconsistent Read",
                                f"Different values read simultaneously from nodes {nodes}",
                                base_record, compare_record
                            )
                            final_results['consistent_data'] = False
        
        final_results['read_consistency'] = 'PASS' if final_results['consistent_data'] else 'FAIL'
        
        self.logger.end_test_case(final_results)
    
    def _test_read_write_concurrency(self, isolation_level: str, nodes: List[int], 
                                   trans_id: int, test_name: str):
        """Test Case 2: One transaction writes while others read same data"""
        test_case_name = f"Case2_ReadWriteConcurrency_{test_name}"
        
        self.logger.start_test_case(
            test_case_name, isolation_level, nodes, [trans_id]
        )
        
        # Set isolation level on both nodes
        for node_id in nodes:
            result = self.db.set_session_isolation_level(node_id, isolation_level)
            self.logger.log_transaction_step(
                f"SetIsolationLevel_Node{node_id}", node_id,
                f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}",
                {'success': result, 'timestamp': time.strftime('%H:%M:%S.%f')[:-3], 'duration': 0.001},
                f"Setting isolation level for read-write test"
            )
        
        # Get initial value
        initial_result = self.db.get_transaction_record(nodes[0], trans_id)
        initial_value = initial_result['results'][0]['amount'] if (
            initial_result['success'] and initial_result['results']
        ) else 0
        
        new_value = float(initial_value) + 1000.00
        
        results = {}
        errors = {}
        
        def write_transaction(node_id: int):
            """Execute write transaction"""
            connection_key = f"{test_case_name}_node{node_id}_write"
            
            try:
                # Start transaction
                start_result = self.db.start_transaction(node_id, connection_key)
                self.logger.log_transaction_step(
                    f"WriteThread_StartTransaction", node_id, "START TRANSACTION",
                    start_result, f"Starting write transaction on Node {node_id}"
                )
                
                if not start_result['success']:
                    errors['write'] = f"Failed to start transaction: {start_result.get('error')}"
                    return
                
                # Read current value
                read_result = self.db.get_transaction_record(node_id, trans_id, connection_key)
                self.logger.log_transaction_step(
                    f"WriteThread_InitialRead", node_id,
                    f"SELECT * FROM trans WHERE trans_id = {trans_id}",
                    read_result, "Reading current value before update"
                )
                
                # Use SQL SLEEP to allow read transaction to start
                delay_query = "SELECT SLEEP(0.5) as write_delay"
                self.db.execute_query(node_id, delay_query, connection_key=connection_key)
                
                # Update the value with SQL delay to test isolation
                update_result = self.db.update_transaction_with_delay(node_id, trans_id, new_value, 2, connection_key)
                self.logger.log_transaction_step(
                    f"WriteThread_UpdateWithSleep", node_id,
                    f"UPDATE trans SET amount = {new_value} WHERE trans_id = {trans_id} AND SLEEP(2) = 0",
                    update_result, f"Updating amount to {new_value} with 2-second SQL SLEEP"
                )
                
                # Commit transaction
                commit_result = self.db.commit_transaction(node_id, connection_key)
                self.logger.log_transaction_step(
                    f"WriteThread_Commit", node_id, "COMMIT",
                    commit_result, "Committing write transaction"
                )
                
                results['write'] = {
                    'initial_value': initial_value,
                    'new_value': new_value,
                    'success': commit_result['success']
                }
                
            except Exception as e:
                errors['write'] = str(e)
                try:
                    rollback_result = self.db.rollback_transaction(node_id, connection_key)
                    self.logger.log_transaction_step(
                        f"WriteThread_Rollback", node_id, "ROLLBACK",
                        rollback_result, f"Rolling back due to error: {e}"
                    )
                except:
                    pass
            finally:
                self.db.close_persistent_connection(connection_key)
        
        def read_transaction(node_id: int):
            """Execute concurrent read transaction"""
            connection_key = f"{test_case_name}_node{node_id}_read"
            
            try:
                # Use SQL SLEEP to let write transaction start first
                delay_query = "SELECT SLEEP(0.3) as read_start_delay"
                self.db.execute_query(node_id, delay_query)
                
                # Start transaction
                start_result = self.db.start_transaction(node_id, connection_key)
                self.logger.log_transaction_step(
                    f"ReadThread_StartTransaction", node_id, "START TRANSACTION",
                    start_result, f"Starting concurrent read transaction on Node {node_id} after SQL delay"
                )
                
                if not start_result['success']:
                    errors['read'] = f"Failed to start transaction: {start_result.get('error')}"
                    return
                
                # Read multiple times during write transaction with SQL timing
                read_values = []
                for read_num in range(4):
                    if read_num > 0:
                        # SQL SLEEP between reads to spread them across write transaction
                        sleep_query = f"SELECT SLEEP(0.4) as concurrent_read_delay_{read_num}"
                        self.db.execute_query(node_id, sleep_query, connection_key=connection_key)
                    
                    read_result = self.db.get_transaction_record(node_id, trans_id, connection_key)
                    self.logger.log_transaction_step(
                        f"ReadThread_ConcurrentRead{read_num + 1}", node_id,
                        f"SELECT * FROM trans WHERE trans_id = {trans_id}",
                        read_result, f"Concurrent read attempt {read_num + 1} with SQL timing control"
                    )
                    
                    if read_result['success'] and read_result['results']:
                        read_values.append(read_result['results'][0]['amount'])
                
                # Commit read transaction
                commit_result = self.db.commit_transaction(node_id, connection_key)
                self.logger.log_transaction_step(
                    f"ReadThread_Commit", node_id, "COMMIT",
                    commit_result, "Committing read transaction"
                )
                
                results['read'] = {
                    'values_read': read_values,
                    'success': commit_result['success']
                }
                
            except Exception as e:
                errors['read'] = str(e)
                try:
                    rollback_result = self.db.rollback_transaction(node_id, connection_key)
                    self.logger.log_transaction_step(
                        f"ReadThread_Rollback", node_id, "ROLLBACK",
                        rollback_result, f"Rolling back due to error: {e}"
                    )
                except:
                    pass
            finally:
                self.db.close_persistent_connection(connection_key)
        
        # Create threads
        write_thread = threading.Thread(target=write_transaction, args=(nodes[0],))
        read_thread = threading.Thread(target=read_transaction, args=(nodes[1],))
        
        # Start threads
        write_thread.start()
        read_thread.start()
        
        # Wait for completion
        write_thread.join()
        read_thread.join()
        
        # Analyze results for isolation anomalies
        final_results = {
            'write_success': results.get('write', {}).get('success', False),
            'read_success': results.get('read', {}).get('success', False),
            'errors': errors
        }
        
        # Check for dirty reads
        if 'read' in results and 'write' in results:
            read_values = results['read']['values_read']
            initial_val = float(initial_value)
            new_val = float(new_value)
            
            dirty_read_detected = False
            for value in read_values:
                if float(value) == new_val and isolation_level in ['READ UNCOMMITTED']:
                    # This might be expected for READ UNCOMMITTED
                    pass
                elif float(value) == new_val and isolation_level != 'READ UNCOMMITTED':
                    self.logger.log_anomaly(
                        "Dirty Read",
                        f"Read uncommitted value {new_val} before write transaction committed",
                        initial_val, new_val
                    )
                    dirty_read_detected = True
            
            final_results['dirty_read_detected'] = dirty_read_detected
            final_results['isolation_maintained'] = not dirty_read_detected or isolation_level == 'READ UNCOMMITTED'
        
        # Restore original value for next test
        try:
            restore_result = self.db.update_transaction_record(nodes[0], trans_id, initial_value)
            self.db.commit_transaction(nodes[0])
            self.logger.log_transaction_step(
                "RestoreOriginalValue", nodes[0],
                f"UPDATE trans SET amount = {initial_value} WHERE trans_id = {trans_id}",
                restore_result, "Restoring original value for next test"
            )
        except:
            pass
        
        self.logger.end_test_case(final_results)
    
    def _test_concurrent_writes(self, isolation_level: str, nodes: List[int], 
                              trans_id: int, test_name: str):
        """Test Case 3: Concurrent transactions writing same data"""
        test_case_name = f"Case3_ConcurrentWrites_{test_name}"
        
        self.logger.start_test_case(
            test_case_name, isolation_level, nodes, [trans_id]
        )
        
        # Set isolation level on both nodes
        for node_id in nodes:
            result = self.db.set_session_isolation_level(node_id, isolation_level)
            self.logger.log_transaction_step(
                f"SetIsolationLevel_Node{node_id}", node_id,
                f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}",
                {'success': result, 'timestamp': time.strftime('%H:%M:%S.%f')[:-3], 'duration': 0.001},
                f"Setting isolation level for concurrent write test"
            )
        
        # Get initial value
        initial_result = self.db.get_transaction_record(nodes[0], trans_id)
        initial_value = initial_result['results'][0]['amount'] if (
            initial_result['success'] and initial_result['results']
        ) else 0
        
        results = {}
        errors = {}
        
        def write_transaction(node_id: int, thread_name: str, increment: float):
            """Execute write transaction with specific increment"""
            connection_key = f"{test_case_name}_node{node_id}_{thread_name}"
            
            try:
                # Start transaction
                start_result = self.db.start_transaction(node_id, connection_key)
                self.logger.log_transaction_step(
                    f"{thread_name}_StartTransaction", node_id, "START TRANSACTION",
                    start_result, f"Starting write transaction on Node {node_id}"
                )
                
                if not start_result['success']:
                    errors[thread_name] = f"Failed to start transaction: {start_result.get('error')}"
                    return
                
                # Read current value with FOR UPDATE lock
                read_result = self.db.get_transaction_record_with_lock(node_id, trans_id, connection_key)
                self.logger.log_transaction_step(
                    f"{thread_name}_SelectForUpdate", node_id,
                    f"SELECT * FROM trans WHERE trans_id = {trans_id} FOR UPDATE",
                    read_result, f"Reading current value with FOR UPDATE lock"
                )
                
                if not read_result['success'] or not read_result['results']:
                    errors[thread_name] = "Failed to read current value"
                    return
                
                current_value = float(read_result['results'][0]['amount'])
                new_value = current_value + increment
                
                # Use SQL SLEEP to simulate processing time while holding locks
                update_result = self.db.update_transaction_with_delay(node_id, trans_id, new_value, 2, connection_key)
                self.logger.log_transaction_step(
                    f"{thread_name}_UpdateWithSleep", node_id,
                    f"UPDATE trans SET amount = {new_value} WHERE trans_id = {trans_id} AND SLEEP(2) = 0",
                    update_result, f"Updating from {current_value} to {new_value} with 2-second SQL SLEEP"
                )
                
                # Commit transaction
                commit_result = self.db.commit_transaction(node_id, connection_key)
                self.logger.log_transaction_step(
                    f"{thread_name}_Commit", node_id, "COMMIT",
                    commit_result, f"Committing write transaction"
                )
                
                results[thread_name] = {
                    'initial_read': current_value,
                    'increment': increment,
                    'attempted_value': new_value,
                    'success': commit_result['success']
                }
                
            except Exception as e:
                errors[thread_name] = str(e)
                try:
                    rollback_result = self.db.rollback_transaction(node_id, connection_key)
                    self.logger.log_transaction_step(
                        f"{thread_name}_Rollback", node_id, "ROLLBACK",
                        rollback_result, f"Rolling back due to error: {e}"
                    )
                except:
                    pass
            finally:
                self.db.close_persistent_connection(connection_key)
        
        # Create threads with different increments
        threads = []
        increments = [500.00, 750.00]  # Different increments to detect lost updates
        
        for i, node_id in enumerate(nodes):
            thread_name = f"WriteThread{i+1}_Node{node_id}"
            thread = threading.Thread(
                target=write_transaction, 
                args=(node_id, thread_name, increments[i])
            )
            threads.append(thread)
        
        # Start all threads simultaneously
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check final value to detect lost updates
        final_result = self.db.get_transaction_record(nodes[0], trans_id)
        final_value = final_result['results'][0]['amount'] if (
            final_result['success'] and final_result['results']
        ) else initial_value
        
        # Analyze results
        expected_value = float(initial_value) + sum(increments)
        actual_value = float(final_value)
        
        final_results = {
            'initial_value': float(initial_value),
            'expected_final_value': expected_value,
            'actual_final_value': actual_value,
            'successful_writes': len([r for r in results.values() if r.get('success', False)]),
            'total_attempts': len(results),
            'errors': errors
        }
        
        # Check for lost update anomaly
        if abs(actual_value - expected_value) > 0.01:  # Allow small floating point differences
            self.logger.log_anomaly(
                "Lost Update",
                f"Final value {actual_value} doesn't match expected {expected_value}",
                expected_value, actual_value
            )
            final_results['lost_update_detected'] = True
        else:
            final_results['lost_update_detected'] = False
        
        # Check for serialization anomalies
        if final_results['successful_writes'] > 1 and isolation_level == 'SERIALIZABLE':
            if final_results['lost_update_detected']:
                self.logger.log_anomaly(
                    "Serialization Failure",
                    f"Multiple concurrent writes succeeded in SERIALIZABLE isolation",
                    1, final_results['successful_writes']
                )
        
        final_results['isolation_maintained'] = not final_results['lost_update_detected']
        
        # Restore original value
        try:
            restore_result = self.db.update_transaction_record(nodes[0], trans_id, initial_value)
            self.db.commit_transaction(nodes[0])
            self.logger.log_transaction_step(
                "RestoreOriginalValue", nodes[0],
                f"UPDATE trans SET amount = {initial_value} WHERE trans_id = {trans_id}",
                restore_result, "Restoring original value for next test"
            )
        except:
            pass
        
        self.logger.end_test_case(final_results)

if __name__ == "__main__":
    # Run the complete test suite
    test_suite = ConcurrencyTestSuite()
    test_suite.run_all_tests()

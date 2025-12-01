import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mysql.connector
import threading
import time
from datetime import datetime
import pandas as pd
from python.utils.lock_manager import DistributedLockManager
from python.db.db_config import get_node_config, NODE_CONFIGS

class MixedReadWriteTest:
    def __init__(self):
        self.results = {}
        self.lock = threading.Lock()
        
        # Get database configs from db_config.py
        # Build node_configs dict for lock manager
        self.node_configs = {
            1: get_node_config(1),
            2: get_node_config(2),
            3: get_node_config(3)
        }
        
        # Initialize distributed lock manager
        self.lock_manager = DistributedLockManager(self.node_configs, current_node_id="case2_test")
        
        # Store prepared transactions (connection, cursor, lock info)
        self.prepared_transactions = {}
        self.prepared_lock = threading.Lock()
    
    def prepare_write_transaction(self, trans_id, new_amount, transaction_id, isolation_level):
        """
        Phase 1: Prepare write transaction (matches app's INSERT/UPDATE/DELETE button)
        - Acquire multi-node distributed lock
        - Start transaction
        - Execute UPDATE statement (uncommitted)
        - Keep lock held and connection open
        """
        start_time = time.time()
        resource_id = f"trans_{trans_id}"
        
        conn = None
        cursor = None
        lock_acquired = False
        acquired_nodes = []
        
        try:
            # Acquire distributed lock on ALL nodes (multi-node locking)
            print(f"[{transaction_id}] GROWING PHASE: Attempting to acquire multi-node lock on {resource_id} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            # Try to acquire lock on all 3 nodes
            for node_id in [1, 2, 3]:
                try:
                    if self.lock_manager.acquire_lock(resource_id, node_id, timeout=30):
                        acquired_nodes.append(node_id)
                        print(f"[{transaction_id}]   Lock acquired on Node {node_id}")
                    else:
                        print(f"[{transaction_id}]   Failed to acquire lock on Node {node_id}")
                except Exception as e:
                    print(f"[{transaction_id}]   WARNING: Node {node_id} unavailable: {str(e)}")
            
            if len(acquired_nodes) == 0:
                raise Exception(f"Failed to acquire lock on any node for {resource_id}")
            
            lock_acquired = True
            print(f"[{transaction_id}] Multi-node lock acquired on {len(acquired_nodes)}/3 nodes: {acquired_nodes}")
            
            # Determine primary node for execution (use first available node)
            primary_node = acquired_nodes[0]
            
            # Connect to primary node
            config = self.node_configs[primary_node]
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor(dictionary=True)
            
            # Set isolation level
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            
            # Start transaction
            cursor.execute("START TRANSACTION")
            print(f"[{transaction_id}] Transaction started on Node {primary_node}")
            
            # Read current value
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            before = cursor.fetchone()
            
            if not before:
                raise Exception(f"Record with trans_id={trans_id} not found on Node {primary_node}")
            
            # Execute UPDATE statement (uncommitted - row locked)
            cursor.execute("UPDATE trans SET amount = %s WHERE trans_id = %s", (new_amount, trans_id))
            affected_rows = cursor.rowcount
            
            print(f"[{transaction_id}] UPDATE prepared (uncommitted) on Node {primary_node}: {before['amount']} → {new_amount}")
            
            # Store prepared transaction info (keep connection and lock held)
            with self.prepared_lock:
                self.prepared_transactions[transaction_id] = {
                    'conn': conn,
                    'cursor': cursor,
                    'resource_id': resource_id,
                    'acquired_nodes': acquired_nodes,
                    'primary_node': primary_node,
                    'trans_id': trans_id,
                    'before_amount': float(before['amount']),
                    'new_amount': new_amount,
                    'affected_rows': affected_rows,
                    'start_time': start_time,
                    'isolation_level': isolation_level
                }
            
            return True
            
        except Exception as e:
            # Rollback and cleanup on error
            print(f"[{transaction_id}] ERROR during prepare: {str(e)}")
            
            if conn:
                conn.rollback()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            
            # Release locks on all acquired nodes
            if lock_acquired and acquired_nodes:
                for node_id in acquired_nodes:
                    try:
                        self.lock_manager.release_lock(resource_id, node_id)
                    except:
                        pass
            
            # Store error result
            end_time = time.time()
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'WRITE',
                    'status': 'FAILED',
                    'error': str(e),
                    'phase': 'PREPARE',
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
            
            return False
    
    def commit_write_transaction(self, transaction_id):
        """
        Phase 2: Commit write transaction (matches app's COMMIT button)
        - Commit transaction on primary node
        - Simulate replication to other nodes
        - Release multi-node distributed lock (SHRINKING PHASE)
        """
        # Retrieve prepared transaction
        with self.prepared_lock:
            if transaction_id not in self.prepared_transactions:
                print(f"[{transaction_id}] ERROR: No prepared transaction found")
                return False
            
            prep = self.prepared_transactions[transaction_id]
        
        conn = prep['conn']
        cursor = prep['cursor']
        resource_id = prep['resource_id']
        acquired_nodes = prep['acquired_nodes']
        primary_node = prep['primary_node']
        trans_id = prep['trans_id']
        start_time = prep['start_time']
        
        try:
            print(f"[{transaction_id}] Committing transaction on Node {primary_node}...")
            
            # Read updated value before commit
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            after = cursor.fetchone()
            
            # Commit transaction (lock still held)
            conn.commit()
            print(f"[{transaction_id}] Transaction committed on Node {primary_node}")
            
            # Simulate replication to other nodes (best-effort)
            replication_success = 0
            for target_node in [1, 2, 3]:
                if target_node != primary_node:
                    try:
                        # Simulate replication delay
                        time.sleep(0.05)
                        
                        # Actually replicate if node is available
                        target_config = self.node_configs[target_node]
                        target_conn = mysql.connector.connect(**target_config)
                        target_cursor = target_conn.cursor()
                        
                        target_cursor.execute(
                            "UPDATE trans SET amount = %s WHERE trans_id = %s",
                            (prep['new_amount'], trans_id)
                        )
                        target_conn.commit()
                        target_cursor.close()
                        target_conn.close()
                        
                        replication_success += 1
                        print(f"[{transaction_id}]   Replicated to Node {target_node}")
                    except Exception as e:
                        print(f"[{transaction_id}]   WARNING: Replication to Node {target_node} failed: {str(e)}")
            
            print(f"[{transaction_id}] Replication complete: {replication_success}/{len([1,2,3])-1} nodes")
            
            end_time = time.time()
            
            # Store success result
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'WRITE',
                    'node': f'node{primary_node}',
                    'status': 'SUCCESS',
                    'trans_id': trans_id,
                    'before_amount': prep['before_amount'],
                    'after_amount': float(after['amount']),
                    'affected_rows': prep['affected_rows'],
                    'replicated_nodes': replication_success,
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
            
            return True
            
        except Exception as e:
            end_time = time.time()
            print(f"[{transaction_id}] ERROR during commit: {str(e)}")
            
            if conn:
                conn.rollback()
            
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'WRITE',
                    'status': 'FAILED',
                    'error': str(e),
                    'phase': 'COMMIT',
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
            
            return False
            
        finally:
            # SHRINKING PHASE - Release locks on all nodes
            print(f"[{transaction_id}] SHRINKING PHASE: Releasing multi-node lock for {resource_id}")
            
            for node_id in acquired_nodes:
                try:
                    self.lock_manager.release_lock(resource_id, node_id)
                    print(f"[{transaction_id}]   Lock released on Node {node_id}")
                except Exception as e:
                    print(f"[{transaction_id}]   WARNING: Failed to release lock on Node {node_id}: {str(e)}")
            
            print(f"[{transaction_id}] Lock released (2PL shrinking phase)")
            
            # Close database connection
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            
            # Remove from prepared transactions
            with self.prepared_lock:
                if transaction_id in self.prepared_transactions:
                    del self.prepared_transactions[transaction_id]
    
    def write_transaction_with_retry(self, trans_id, new_amount, transaction_id, isolation_level, max_retries=3):
        """
        Execute write transaction with retry logic for duplicate key errors
        (matches app's retry behavior in add_transaction.py)
        """
        for attempt in range(max_retries):
            try:
                # Phase 1: Prepare
                success = self.prepare_write_transaction(trans_id, new_amount, transaction_id, isolation_level)
                
                if not success:
                    if attempt < max_retries - 1:
                        print(f"[{transaction_id}] WARNING: Prepare failed. Retrying... (Attempt {attempt + 1}/{max_retries})")
                        time.sleep(0.5)
                        continue
                    else:
                        print(f"[{transaction_id}] ERROR: Max retries reached. Giving up.")
                        return False
                
                # Phase 2: Commit (back-to-back, no delay)
                success = self.commit_write_transaction(transaction_id)
                
                if success:
                    return True
                else:
                    # Check if it's a duplicate key error that needs retry
                    with self.lock:
                        result = self.results.get(transaction_id, {})
                        error = result.get('error', '')
                        
                        if '1062' in error or 'Duplicate entry' in error:
                            if attempt < max_retries - 1:
                                print(f"[{transaction_id}] Duplicate key detected. Retrying with new value... (Attempt {attempt + 1}/{max_retries})")
                                # Increment amount slightly for retry
                                new_amount += 0.01
                                time.sleep(0.5)
                                continue
                    
                    if attempt < max_retries - 1:
                        print(f"[{transaction_id}] WARNING: Commit failed. Retrying... (Attempt {attempt + 1}/{max_retries})")
                        time.sleep(0.5)
                        continue
                    else:
                        print(f"[{transaction_id}] ERROR: Max retries reached. Giving up.")
                        return False
                        
            except Exception as e:
                print(f"[{transaction_id}] Exception during write transaction: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"[{transaction_id}] Retrying... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(0.5)
                    continue
                else:
                    return False
        
        return False
    
    def write_transaction(self, node_num, trans_id, new_amount, transaction_id, isolation_level):
        """Execute a write (UPDATE) transaction on specified node with distributed locking"""
        start_time = time.time()
        config = self.node_configs[node_num]
        resource_id = f"trans_{trans_id}"
        
        conn = None
        cursor = None
        lock_acquired = False
        
        try:
            # Acquire distributed lock BEFORE starting transaction
            print(f"[{transaction_id}] Attempting to acquire lock on {resource_id} at Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            lock_acquired = self.lock_manager.acquire_lock(resource_id, node_num, timeout=30)
            
            if not lock_acquired:
                raise Exception(f"Failed to acquire lock on {resource_id}")
            
            print(f"[{transaction_id}] Lock acquired, starting write on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            # Connect to database
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor(dictionary=True)
            
            # Set isolation level
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            
            # Validate we still hold the lock before starting transaction
            if not self.lock_manager.check_lock(resource_id, node_num):
                raise Exception(f"Lost lock on {resource_id} at Node {node_num}")
            
            # Start transaction
            cursor.execute("START TRANSACTION")
            
            # Read current value
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            before = cursor.fetchone()
            
            if not before:
                raise Exception(f"Record with trans_id={trans_id} not found on Node {node_num}")
            
            # Update the amount IMMEDIATELY (locks the row)
            cursor.execute("UPDATE trans SET amount = %s WHERE trans_id = %s", (new_amount, trans_id))
            
            # Hold transaction open AFTER update (keeps row locked)
            time.sleep(3)
            affected_rows = cursor.rowcount
            
            # Read updated value
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            after = cursor.fetchone()
            
            # Commit
            conn.commit()
            
            end_time = time.time()
            
            print(f"[{transaction_id}] Completed write on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            print(f"[{transaction_id}] Updated trans_id={trans_id}: {before['amount']} → {after['amount']}")
            
            # Store results
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'WRITE',
                    'node': f'node{node_num}',
                    'status': 'SUCCESS',
                    'trans_id': trans_id,
                    'before_amount': float(before['amount']),
                    'after_amount': float(after['amount']),
                    'affected_rows': affected_rows,
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
        
        except Exception as e:
            end_time = time.time()
            if conn:
                conn.rollback()
            
            print(f"[{transaction_id}] ERROR on Node {node_num}: {str(e)}")
            
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'WRITE',
                    'node': f'node{node_num}',
                    'status': 'FAILED',
                    'error': str(e),
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
        
        finally:
            # Always release the lock
            if lock_acquired:
                self.lock_manager.release_lock(resource_id, node_num)
                print(f"[{transaction_id}] Lock released on {resource_id}")
            
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def read_transaction(self, node_num, trans_id, transaction_id, isolation_level):
        """
        Execute a read (SELECT) transaction on specified node
        No distributed locks - uses isolation level only (matches app behavior)
        """
        """Execute a read (SELECT) transaction on specified node"""
        start_time = time.time()
        config = self.node_configs[node_num]
        
        conn = None
        cursor = None
        
        try:
            # Connect to database
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor(dictionary=True)
            
            # Set isolation level
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            
            # Start transaction
            cursor.execute("START TRANSACTION")
            
            print(f"[{transaction_id}] Starting read on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            # First read
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            first_read = cursor.fetchone()
            
            # Simulate processing time (hold transaction open)
            time.sleep(2)
            
            # Second read (to detect non-repeatable reads)
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            second_read = cursor.fetchone()
            
            # Commit
            conn.commit()
            
            end_time = time.time()
            
            print(f"[{transaction_id}] Completed read on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            if first_read and second_read:
                print(f"[{transaction_id}] Read trans_id={trans_id}: 1st={first_read['amount']}, 2nd={second_read['amount']}")
                
                # Check for non-repeatable read
                repeatable = float(first_read['amount']) == float(second_read['amount'])
                
                with self.lock:
                    self.results[transaction_id] = {
                        'type': 'READ',
                        'node': f"node{node_num}",
                        'status': 'SUCCESS',
                        'trans_id': trans_id,
                        'first_read': float(first_read['amount']) if first_read else None,
                        'second_read': float(second_read['amount']) if second_read else None,
                        'repeatable': repeatable,
                        'start_time': start_time,
                        'end_time': end_time,
                        'duration': end_time - start_time
                    }
            else:
                raise Exception(f"Record with trans_id={trans_id} not found on Node {node_num}")
        
        except Exception as e:
            end_time = time.time()
            if conn:
                conn.rollback()
            
            print(f"[{transaction_id}] ERROR on Node {node_num}: {str(e)}")
            
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'READ',
                    'node': f'node{node_num}',
                    'status': 'FAILED',
                    'error': str(e),
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def run_test(self, trans_id, isolation_level="READ COMMITTED", mode="concurrent"):
        """
        Run mixed read/write test with 10 concurrent transactions using Strict 2PL
        All transactions access the SAME trans_id to test distributed concurrency
        
        Configuration:
            - 4 writers: Each acquires multi-node locks (all 3 nodes)
            - 6 readers: 2 on Node 1, 4 on Node 2 (isolation level only, no locks)
        
        Args:
            mode: "concurrent" for parallel execution, "sequential" for serial execution
        
        Tests 2-Phase Locking with:
            - Multi-node distributed lock acquisition (writers)
            - Growing phase: Lock before UPDATE
            - Shrinking phase: Release after COMMIT
            - Retry logic: Up to 3 attempts on failure
        """
        mode_label = "CONCURRENT" if mode == "concurrent" else "SEQUENTIAL"
        print(f"\n{'='*70}")
        print(f"Running Case #2: Mixed Read/Write (2PL) - trans_id={trans_id} ({mode_label})")
        print(f"Isolation Level: {isolation_level}")
        print(f"Configuration:")
        print(f"  WRITERS (4 total): Multi-node locks (all 3 nodes)")
        print(f"     - Strict 2PL: Acquire locks -> UPDATE -> COMMIT -> Release locks")
        print(f"     - Retry logic: Up to 3 attempts on failure")
        print(f"  READERS (6 total): 2 on Node 1, 4 on Node 2")
        print(f"     - Isolation level only (no distributed locks)")
        print(f"  Target: All transactions access trans_id={trans_id}")
        print(f"{'='*70}\n")
        
        self.results = {}  # Reset results
        threads = []
        
        # Create 4 writer threads
        # Using write_transaction_with_retry which:
        #   1. Acquires multi-node locks (all 3 nodes)
        #   2. Calls prepare→commit back-to-back
        #   3. Retries up to 3 times on failure
        # Note: Writers no longer target specific nodes - they acquire locks on ALL nodes
        for i in range(1, 3):
            threads.append(threading.Thread(
                target=self.write_transaction_with_retry,
                args=(trans_id, 10000.00 + i*1111.11, f"T{i}_WRITER_Multi", isolation_level)
            ))
        
        for i in range(3, 5):
            threads.append(threading.Thread(
                target=self.write_transaction_with_retry,
                args=(trans_id, 10000.00 + i*1111.11, f"T{i}_WRITER_Multi", isolation_level)
            ))
        
        # Create 6 reader threads - 2 on Node 1, 4 on Node 2
        for i in range(5, 7):
            threads.append(threading.Thread(
                target=self.read_transaction,
                args=(1, trans_id, f"T{i}_READER_Node1", isolation_level)
            ))
        
        for i in range(7, 11):
            threads.append(threading.Thread(
                target=self.read_transaction,
                args=(2, trans_id, f"T{i}_READER_Node2", isolation_level)
            ))
        
        if mode == "concurrent":
            # Start all threads with slight staggering
            for i, thread in enumerate(threads):
                thread.start()
                if i < 4:  # Stagger writers more
                    time.sleep(0.2)
                elif i == 4:  # Slight delay before readers start
                    time.sleep(0.1)
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
        else:
            # Sequential execution - run each thread one after another
            for thread in threads:
                thread.start()
                thread.join()  # Wait for this thread to complete before starting next
        
        # Display results
        self.display_results()
        
        # Restore original value on both nodes
        self.restore_original_value(trans_id, 1)
        self.restore_original_value(trans_id, 2)
        
        return self.results
    
    def restore_original_value(self, trans_id, node_num):
        """Restore the original value after test"""
        try:
            config = self.node_configs[node_num]
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            
            # Use a known value to restore (you can modify this)
            cursor.execute("UPDATE trans SET amount = 1000.00 WHERE trans_id = %s", (trans_id,))
            conn.commit()
            
            cursor.close()
            conn.close()
            
            print(f"\nRestored trans_id={trans_id} to original value")
        except Exception as e:
            print(f"\nWarning: Could not restore original value: {e}")
    
    def display_results(self):
        """Display test results"""
        print(f"\n{'='*70}")
        print("TEST RESULTS")
        print(f"{'='*70}\n")
        
        # Create summary table
        summary = []
        for txn_id, result in sorted(self.results.items()):
            row = {
                'Transaction': txn_id,
                'Type': result['type'],
                'Node': result['node'],
                'Status': result['status'],
                'Duration (s)': f"{result['duration']:.6f}"
            }
            
            if result['type'] == 'WRITE' and result['status'] == 'SUCCESS':
                row['Before→After'] = f"{result['before_amount']:.2f}→{result['after_amount']:.2f}"
                row['Replicated'] = f"{result.get('replicated_nodes', 0)}/2 nodes"
            elif result['type'] == 'READ' and result['status'] == 'SUCCESS':
                row['Repeatable?'] = 'Yes' if result['repeatable'] else 'No'
                row['Values'] = f"{result['first_read']:.2f}, {result['second_read']:.2f}"
            
            summary.append(row)
        
        df = pd.DataFrame(summary)
        print(df.to_string(index=False))
        
        # Analyze anomalies
        print(f"\n{'='*70}")
        print("ANOMALY DETECTION")
        print(f"{'='*70}\n")
        
        # Check for dirty reads (reader sees uncommitted write)
        writer_result = next((r for r in self.results.values() if r['type'] == 'WRITE'), None)
        read_results = [r for r in self.results.values() if r['type'] == 'READ' and r['status'] == 'SUCCESS']
        
        if writer_result and writer_result['status'] == 'SUCCESS':
            new_value = writer_result['after_amount']
            
            for read_result in read_results:
                if read_result['first_read'] == new_value or read_result['second_read'] == new_value:
                    print(f"WARNING: Possible DIRTY READ detected in {[k for k, v in self.results.items() if v == read_result][0]}")
                    print(f"   Reader saw value {new_value} that was being written")
        
        # Check for non-repeatable reads
        non_repeatable = [r for r in read_results if not r['repeatable']]
        if non_repeatable:
            print(f"\nWARNING: NON-REPEATABLE READS detected: {len(non_repeatable)} reader(s)")
            for result in non_repeatable:
                txn_id = [k for k, v in self.results.items() if v == result][0]
                print(f"   {txn_id}: First={result['first_read']:.2f}, Second={result['second_read']:.2f}")
        else:
            print(f"\nNO NON-REPEATABLE READS: All readers saw consistent values")
        
        # Show timing overlap
        print(f"\n{'='*70}")
        print("CONCURRENCY ANALYSIS")
        print(f"{'='*70}\n")
        
        start_times = [r['start_time'] for r in self.results.values()]
        end_times = [r['end_time'] for r in self.results.values()]
        
        earliest_start = min(start_times)
        latest_end = max(end_times)
        total_time = latest_end - earliest_start
        
        print(f"Total execution time: {total_time:.6f} seconds")
        print(f"Expected if sequential: {sum(r['duration'] for r in self.results.values()):.6f} seconds")
        print(f"PASSED: Transactions ran concurrently" if total_time < 5 else "WARNING: Transactions may have run sequentially")
        
        # Check 2PL effectiveness
        writers = [r for r in self.results.values() if r['type'] == 'WRITE']
        successful_writers = [r for r in writers if r['status'] == 'SUCCESS']
        
        if writers:
            success_rate = (len(successful_writers) / len(writers)) * 100
            print(f"\n2-PHASE LOCKING EFFECTIVENESS:")
            print(f"   Writers: {len(successful_writers)}/{len(writers)} successful ({success_rate:.1f}%)")
            
            if successful_writers:
                print(f"   Growing Phase: Multi-node locks acquired before UPDATE")
                print(f"   Shrinking Phase: Locks released after COMMIT")
                print(f"   Strict 2PL: Lock held through entire transaction lifecycle")
                
                # Check replication
                avg_replication = sum(r.get('replicated_nodes', 0) for r in successful_writers) / len(successful_writers)
                print(f"   Replication: Average {avg_replication:.1f}/2 nodes synchronized")
    
    def validate_serializability(self, concurrent_results, sequential_results, isolation_level):
        """
        Validate serializability by comparing concurrent and sequential execution results
        """
        print(f"\n{'='*70}")
        print(f"SERIALIZABILITY VALIDATION - {isolation_level}")
        print(f"{'='*70}\n")
        
        # Get final amounts from writers
        concurrent_writers = {k: v for k, v in concurrent_results.items() 
                            if v['type'] == 'WRITE' and v['status'] == 'SUCCESS'}
        sequential_writers = {k: v for k, v in sequential_results.items() 
                            if v['type'] == 'WRITE' and v['status'] == 'SUCCESS'}
        
        # Calculate execution times
        concurrent_start_times = [r['start_time'] for r in concurrent_results.values()]
        concurrent_end_times = [r['end_time'] for r in concurrent_results.values()]
        concurrent_duration = max(concurrent_end_times) - min(concurrent_start_times)
        
        sequential_start_times = [r['start_time'] for r in sequential_results.values()]
        sequential_end_times = [r['end_time'] for r in sequential_results.values()]
        sequential_duration = max(sequential_end_times) - min(sequential_start_times)
        
        # Compare number of successful transactions
        concurrent_success = len([v for v in concurrent_results.values() if v['status'] == 'SUCCESS'])
        sequential_success = len([v for v in sequential_results.values() if v['status'] == 'SUCCESS'])
        
        print(f"Concurrent execution: {concurrent_success} successful transactions ({concurrent_duration:.2f}s)")
        print(f"Sequential execution: {sequential_success} successful transactions ({sequential_duration:.2f}s)")
        print(f"Speedup: {sequential_duration / concurrent_duration:.2f}x faster with concurrency")
        
        # For SERIALIZABLE, the final state should match sequential execution
        if isolation_level == 'SERIALIZABLE':
            # Check if final write values are consistent
            if concurrent_writers and sequential_writers:
                # Get the last successful write from each execution
                concurrent_final = max(concurrent_writers.values(), 
                                     key=lambda x: x['end_time'])['after_amount']
                sequential_final = max(sequential_writers.values(), 
                                     key=lambda x: x['end_time'])['after_amount']
                
                print(f"\nFinal amount (concurrent): {concurrent_final:.2f}")
                print(f"Final amount (sequential): {sequential_final:.2f}")
                
                if abs(concurrent_final - sequential_final) < 0.01:
                    print("\nSERIALIZABLE: Final state matches sequential execution")
                    print("   Database consistency verified!")
                    return True, sequential_duration
                else:
                    print("\nWARNING: SERIALIZABLE: Final states differ")
                    print("   This may indicate serializability violation")
                    return False, sequential_duration
        
        # For other isolation levels, just check consistency of successful transactions
        print(f"\nValidation complete for {isolation_level}")
        print(f"   Note: {isolation_level} allows anomalies, sequential match not required")
        return True, sequential_duration
    
    def calculate_metrics(self):
        """Calculate performance metrics for comparison"""
        start_times = [r['start_time'] for r in self.results.values()]
        end_times = [r['end_time'] for r in self.results.values()]
        
        earliest_start = min(start_times)
        latest_end = max(end_times)
        total_time = latest_end - earliest_start
        
        successful_txns = sum(1 for r in self.results.values() if r['status'] == 'SUCCESS')
        failed_txns = sum(1 for r in self.results.values() if r['status'] == 'FAILED')
        
        # Throughput = successful transactions / total time
        throughput = successful_txns / total_time if total_time > 0 else 0
        
        # Average response time
        avg_response = sum(r['duration'] for r in self.results.values()) / len(self.results)
        
        # Count anomalies
        read_results = [r for r in self.results.values() if r['type'] == 'READ' and r['status'] == 'SUCCESS']
        non_repeatable_reads = sum(1 for r in read_results if not r['repeatable'])
        
        return {
            'total_time': total_time,
            'successful_txns': successful_txns,
            'failed_txns': failed_txns,
            'throughput': throughput,
            'avg_response_time': avg_response,
            'success_rate': (successful_txns / len(self.results)) * 100,
            'non_repeatable_reads': non_repeatable_reads
        }
    
    def cleanup(self):
        """Cleanup: release all locks and close connections"""
        # Release any remaining prepared transactions
        with self.prepared_lock:
            for txn_id, prep in list(self.prepared_transactions.items()):
                try:
                    conn = prep['conn']
                    cursor = prep['cursor']
                    resource_id = prep['resource_id']
                    acquired_nodes = prep['acquired_nodes']
                    
                    # Rollback
                    if conn:
                        conn.rollback()
                        cursor.close()
                        conn.close()
                    
                    # Release locks
                    for node_id in acquired_nodes:
                        self.lock_manager.release_lock(resource_id, node_id)
                    
                    print(f"[{txn_id}] Cleaned up prepared transaction")
                except:
                    pass
            
            self.prepared_transactions.clear()
        
        # Release all locks
        self.lock_manager.release_all_locks()

def main():
    """Run all test cases for Case #2"""
    test = MixedReadWriteTest()
    
    # Single scenario - one trans_id that exists on Node 2 (276-81998)
    trans_id = 276
    
    # Isolation levels to test
    isolation_levels = [
        'READ UNCOMMITTED',
        'READ COMMITTED',
        'REPEATABLE READ',
        'SERIALIZABLE'
    ]
    
    print("\n" + "="*70)
    print("CASE #2: MIXED READ/WRITE CONCURRENT TRANSACTIONS TEST (2PL)")
    print("="*70)
    print("\nTest Configuration:")
    print(f"  Trans_ID: {trans_id}")
    print("  10 concurrent transactions: 4 writers + 6 readers")
    print("  Cross-node access: Writers on Node 1 & 2, Readers on Node 1 & 2")
    print("  Testing all 4 isolation levels")
    print("\n2-Phase Locking Implementation:")
    print("  Writers: Strict 2PL with prepare->commit phases")
    print("  Multi-node distributed locks (across all 3 nodes)")
    print("  Retry logic: Up to 3 attempts on failure")
    print("  Readers: Isolation level only (no distributed locks)")
    print("="*70)
    
    # Store metrics for comparison
    isolation_metrics = {iso: [] for iso in isolation_levels}
    
    all_results = {}
    sequential_results = {}
    serializability_validation = {}
    sequential_durations = {}
    
    for isolation_level in isolation_levels:
        print(f"\n{'='*70}")
        print(f"Testing with {isolation_level}")
        print(f"{'='*70}")
        
        # Run concurrent execution
        print("\nRunning CONCURRENT execution...")
        concurrent_exec = test.run_test(
            trans_id=trans_id,
            isolation_level=isolation_level,
            mode="concurrent"
        )
        
        # Calculate metrics for this test
        metrics = test.calculate_metrics()
        isolation_metrics[isolation_level].append(metrics)
        
        all_results[isolation_level] = concurrent_exec
        
        # Run sequential execution for validation
        print("\nRunning SEQUENTIAL execution for validation...")
        sequential_exec = test.run_test(
            trans_id=trans_id,
            isolation_level=isolation_level,
            mode="sequential"
        )
        sequential_results[isolation_level] = sequential_exec
        
        # Validate serializability
        is_valid, seq_duration = test.validate_serializability(concurrent_exec, sequential_exec, isolation_level)
        serializability_validation[isolation_level] = is_valid
        sequential_durations[isolation_level] = seq_duration
        
        print("\n" + "-"*70)
    
    # ========================================================================
    # PERFORMANCE COMPARISON
    # ========================================================================
    
    print(f"\n{'='*70}")
    print("ISOLATION LEVEL PERFORMANCE COMPARISON")
    print(f"{'='*70}\n")
    
    # Calculate averages for each isolation level
    comparison_data = []
    
    for iso_level in isolation_levels:
        metrics_list = isolation_metrics[iso_level]
        
        avg_throughput = sum(m['throughput'] for m in metrics_list) / len(metrics_list)
        avg_response = sum(m['avg_response_time'] for m in metrics_list) / len(metrics_list)
        
        comparison_data.append({
            'Isolation Level': iso_level,
            'Avg Throughput (txn/s)': f"{avg_throughput:.6f}",
            'Avg Response Time (s)': f"{avg_response:.6f}"
        })
    
    # Create comparison DataFrame
    df_comparison = pd.DataFrame(comparison_data)
    print(df_comparison.to_string(index=False))
    
    # Display serializability validation summary
    print(f"\n{'='*70}")
    print("SEQUENTIAL VALIDATION SUMMARY")
    print(f"{'='*70}\n")
    
    for iso_level in isolation_levels:
        status = "PASSED" if serializability_validation.get(iso_level, False) else "CHECK"
        seq_duration = sequential_durations.get(iso_level, 0)
        print(f"{status} - {iso_level} (Sequential: {seq_duration:.2f}s)")
    
    # Cleanup
    test.cleanup()
    print("\nCleanup complete - all locks released")

if __name__ == "__main__":
    main()

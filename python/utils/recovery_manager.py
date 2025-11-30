"""
Global Failure and Recovery System
Handles the 4 case studies for distributed database recovery
"""

import hashlib
import time
from datetime import datetime
from typing import List, Dict, Optional
import mysql.connector
from mysql.connector import Error


class RecoveryManager:
    """Manages recovery logs and operations for distributed database system"""
    
    def __init__(self, db_config: Dict, current_node_id: int, max_retries: int = 3):
        """
        Initialize Recovery Manager
        
        Args:
            db_config: Database configuration dictionary
            current_node_id: Current node ID (1, 2, or 3)
            max_retries: Maximum retry attempts for recovery operations
        """
        self.db_config = db_config
        self.current_node_id = current_node_id
        self.max_retries = max_retries
    
    def generate_transaction_hash(self, target_node: int, source_node: int, sql_statement: str) -> str:
        """Generate unique hash to prevent duplicate recovery logs"""
        unique_string = f"{target_node}_{source_node}_{sql_statement}_{datetime.now().strftime('%Y%m%d')}"
        return hashlib.sha256(unique_string.encode()).hexdigest()
    
    def get_db_connection(self):
        """Get database connection for current node"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            return connection
        except Error as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def log_backup(self, target_node: int, source_node: int, sql_statement: str) -> bool:
        """
        Log failed replication transaction for recovery
        
        Args:
            target_node: ID of the failed node that needs this transaction
            source_node: ID of the node that generated the transaction  
            sql_statement: The SQL query that failed to replicate
            
        Returns:
            bool: True if logged successfully, False otherwise
        """
        connection = None
        cursor = None
        try:
            # Generate hash to prevent duplicates
            transaction_hash = self.generate_transaction_hash(target_node, source_node, sql_statement)
            
            connection = self.get_db_connection()
            cursor = connection.cursor()
            
            # Check if this transaction is already logged
            check_sql = """
                SELECT COUNT(*) FROM recovery_log 
                WHERE transaction_hash = %s AND status IN ('PENDING', 'COMPLETED')
            """
            cursor.execute(check_sql, (transaction_hash,))
            
            if cursor.fetchone()[0] > 0:
                print(f"Transaction already logged (hash: {transaction_hash[:8]}...)")
                return True
            
            # Insert recovery log
            insert_sql = """
                INSERT INTO recovery_log 
                (target_node, source_node, sql_statement, transaction_hash)
                VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (target_node, source_node, sql_statement, transaction_hash))
            connection.commit()
            
            log_id = cursor.lastrowid
            print(f"Recovery log created: ID={log_id}, Target=Node{target_node}, Source=Node{source_node}")
            
            # Store in backup node as well (cross-backup)
            self._store_cross_backup(target_node, source_node, sql_statement, transaction_hash)
            
            return True
            
        except Error as e:
            print(f"Failed to log backup transaction: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def _store_cross_backup(self, target_node: int, source_node: int, sql_statement: str, transaction_hash: str):
        """Store backup log in another node to prevent single point of failure"""
        try:
            # Determine backup node (avoid source and target nodes, and current node)
            backup_node = None
            for node in [1, 2, 3]:
                if node != source_node and node != target_node and node != self.current_node_id:
                    backup_node = node
                    break
            
            if backup_node:
                print(f"Storing cross-backup in Node {backup_node} (original in Node {self.current_node_id})")
                
                # Get configuration for the backup node
                from python.db.db_config import get_node_config
                backup_config = get_node_config(backup_node)
                
                connection = None
                cursor = None
                try:
                    # Connect to the backup node
                    import mysql.connector
                    connection = mysql.connector.connect(**backup_config)
                    cursor = connection.cursor()
                    
                    insert_sql = """
                        INSERT INTO recovery_log 
                        (target_node, source_node, sql_statement, transaction_hash, error_message)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(insert_sql, (
                        target_node, 
                        source_node, 
                        sql_statement, 
                        transaction_hash,
                        f"CROSS_BACKUP_FROM_NODE_{self.current_node_id}"
                    ))
                    connection.commit()
                    print(f"Cross-backup successfully stored in Node {backup_node}")
                    
                except Exception as e:
                    print(f"Failed to store cross-backup in Node {backup_node}: {e}")
                finally:
                    if cursor:
                        cursor.close()
                    if connection:
                        connection.close()
            else:
                print(f"No available backup node found (source={source_node}, target={target_node}, current={self.current_node_id})")
                
        except Exception as e:
            print(f"Error in cross-backup process: {e}")
    
    def check_and_recover_pending_logs(self) -> Dict:
        """
        Check for pending recovery logs and attempt to recover them
        Called when node starts up or manually triggered
        Checks ALL available nodes for recovery logs targeting this node
        
        Returns:
            Dict: Recovery results summary
        """
        print(f"Node {self.current_node_id} checking for pending recovery logs across all nodes...")
        
        all_pending_logs = []
        recovery_results = {
            'total_logs': 0,
            'recovered': 0,
            'failed': 0,
            'skipped': 0,
            'nodes_checked': []
        }
        
        # Check all nodes (1, 2, 3) for recovery logs targeting this node
        for check_node_id in [1, 2, 3]:
            try:
                print(f"Checking Node {check_node_id} for recovery logs targeting Node {self.current_node_id}...")
                
                # Get node configuration
                from python.db.db_config import get_node_config
                node_config = get_node_config(check_node_id)
                
                connection = None
                cursor = None
                try:
                    connection = mysql.connector.connect(**node_config)
                    cursor = connection.cursor(dictionary=True)
                    
                    # Get pending recovery logs that target this node
                    select_sql = """
                        SELECT log_id, target_node, source_node, sql_statement, 
                               timestamp, retry_count, transaction_hash
                        FROM recovery_log 
                        WHERE status = 'PENDING' AND target_node = %s
                        ORDER BY timestamp ASC
                    """
                    
                    cursor.execute(select_sql, (self.current_node_id,))
                    pending_logs = cursor.fetchall()
                    
                    if pending_logs:
                        print(f"Found {len(pending_logs)} pending logs for Node {self.current_node_id} in Node {check_node_id}")
                        # Add the source node info to each log for reference
                        for log in pending_logs:
                            log['found_in_node'] = check_node_id
                        all_pending_logs.extend(pending_logs)
                    else:
                        print(f"No pending logs for Node {self.current_node_id} found in Node {check_node_id}")
                    
                    recovery_results['nodes_checked'].append(check_node_id)
                    
                except Error as e:
                    print(f"Could not connect to Node {check_node_id}: {e}")
                    # Node might be offline, continue checking other nodes
                    continue
                finally:
                    if cursor:
                        cursor.close()
                    if connection:
                        connection.close()
                        
            except Exception as e:
                print(f"Error checking Node {check_node_id}: {e}")
                continue
        
        # Sort all logs by timestamp to maintain chronological order
        all_pending_logs.sort(key=lambda x: x['timestamp'])
        
        # Deduplicate logs by transaction_hash to avoid processing the same transaction multiple times
        unique_logs = {}
        for log in all_pending_logs:
            tx_hash = log.get('transaction_hash', '')
            if tx_hash and tx_hash in unique_logs:
                print(f"Skipping duplicate log {log['log_id']} from Node {log['found_in_node']} (same hash as log {unique_logs[tx_hash]['log_id']} from Node {unique_logs[tx_hash]['found_in_node']})")
                # Mark the duplicate as completed to prevent re-processing
                self._mark_recovery_status_in_node(log['found_in_node'], log['log_id'], 'COMPLETED', "Duplicate transaction - skipped during deduplication")
                recovery_results['skipped'] += 1
            else:
                unique_logs[tx_hash] = log
        
        deduplicated_logs = list(unique_logs.values())
        recovery_results['total_logs'] = len(all_pending_logs)
        recovery_results['unique_logs'] = len(deduplicated_logs)
        
        if not deduplicated_logs:
            print(f"No unique pending recovery logs found for Node {self.current_node_id} after deduplication.")
            return recovery_results
        
        print(f"Found {len(all_pending_logs)} total logs, {len(deduplicated_logs)} unique after deduplication. Starting recovery...")
        
        # Process unique recovery logs
        for log in deduplicated_logs:
            print(f"Processing unique log from Node {log['found_in_node']}: {log['sql_statement'][:50]}...")
            result = self._attempt_recovery_cross_node(log)
            recovery_results[result] += 1
            
            # Small delay between recovery attempts
            time.sleep(0.1)
        
        print(f"Recovery completed: {recovery_results}")
        return recovery_results
    
    def _attempt_recovery(self, log: Dict) -> str:
        """
        Attempt to recover a single transaction log from current node
        
        Args:
            log: Recovery log record
            
        Returns:
            str: Recovery result ('recovered', 'failed', 'skipped')
        """
        log_id = log['log_id']
        target_node = log['target_node']
        sql_statement = log['sql_statement']
        retry_count = log['retry_count']
        transaction_hash = log.get('transaction_hash', '')
        
        connection = None
        cursor = None
        try:
            # Skip if max retries exceeded
            if retry_count >= self.max_retries:
                self._mark_recovery_status(log_id, 'FAILED', f"Max retries ({self.max_retries}) exceeded")
                return 'failed'
            
            # Skip if this is not the target node
            if target_node != self.current_node_id:
                return 'skipped'
            
            print(f"Attempting recovery for log {log_id}: {sql_statement[:50]}...")
            
            # Execute the recovery transaction
            connection = self.get_db_connection()
            cursor = connection.cursor()
            
            # Execute the failed SQL statement
            cursor.execute(sql_statement)
            connection.commit()
            
            # Mark as completed
            self._mark_recovery_status(log_id, 'COMPLETED', "Recovery successful")
            
            print(f"Successfully recovered log {log_id}")
            return 'recovered'
            
        except Error as e:
            # Check if this is a duplicate entry error (indicates transaction was already processed)
            if e.errno == 1062:  # MySQL duplicate entry error
                print(f"Transaction already exists in database - marking as completed (hash: {transaction_hash[:8]}...)")
                # Mark as completed since the data is already there
                self._mark_recovery_status(log_id, 'COMPLETED', "Transaction already exists - duplicate detected")
                return 'skipped'
            
            error_msg = f"Recovery attempt {retry_count + 1} failed: {str(e)}"
            
            # Increment retry count
            self._increment_retry_count(log_id, error_msg)
            
            print(f"Recovery failed for log {log_id}: {error_msg}")
            return 'failed'
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def _attempt_recovery_cross_node(self, log: Dict) -> str:
        """
        Attempt to recover a single transaction log found in another node
        
        Args:
            log: Recovery log record with 'found_in_node' field
            
        Returns:
            str: Recovery result ('recovered', 'failed', 'skipped')
        """
        log_id = log['log_id']
        target_node = log['target_node']
        source_node = log.get('found_in_node', self.current_node_id)
        sql_statement = log['sql_statement']
        retry_count = log['retry_count']
        transaction_hash = log.get('transaction_hash', '')
        
        connection = None
        cursor = None
        try:
            # Skip if max retries exceeded
            if retry_count >= self.max_retries:
                self._mark_recovery_status_in_node(source_node, log_id, 'FAILED', f"Max retries ({self.max_retries}) exceeded")
                return 'failed'
            
            # Skip if this is not the target node
            if target_node != self.current_node_id:
                return 'skipped'
            
            # Check if this transaction was already completed by checking transaction hash
            connection = self.get_db_connection()
            cursor = connection.cursor()
            
            # Check if we already have this transaction completed locally
            if transaction_hash:
                check_sql = """
                    SELECT COUNT(*) FROM recovery_log 
                    WHERE transaction_hash = %s AND status = 'COMPLETED'
                """
                cursor.execute(check_sql, (transaction_hash,))
                
                if cursor.fetchone()[0] > 0:
                    print(f"Transaction hash {transaction_hash[:8]}... already completed - marking as skipped")
                    # Mark the duplicate log as completed to avoid re-processing
                    self._mark_recovery_status_in_node(source_node, log_id, 'COMPLETED', "Duplicate transaction - already processed")
                    return 'skipped'
            
            print(f"Attempting cross-node recovery for log {log_id} from Node {source_node}: {sql_statement[:50]}...")
            
            # Execute the failed SQL statement
            cursor.execute(sql_statement)
            connection.commit()
            
            # Mark as completed in the source node where the log was found
            self._mark_recovery_status_in_node(source_node, log_id, 'COMPLETED', "Cross-node recovery successful")
            
            print(f"Successfully recovered cross-node log {log_id} from Node {source_node}")
            return 'recovered'
            
        except Error as e:
            # Check if this is a duplicate entry error (indicates transaction was already processed)
            if e.errno == 1062:  # MySQL duplicate entry error
                print(f"Transaction already exists in database - marking as completed (hash: {transaction_hash[:8]}...)")
                # Mark as completed since the data is already there
                self._mark_recovery_status_in_node(source_node, log_id, 'COMPLETED', "Transaction already exists - duplicate detected")
                return 'skipped'
            
            error_msg = f"Cross-node recovery attempt {retry_count + 1} failed: {str(e)}"
            
            # Increment retry count in the source node
            self._increment_retry_count_in_node(source_node, log_id, error_msg)
            
            print(f"Cross-node recovery failed for log {log_id} from Node {source_node}: {error_msg}")
            return 'failed'
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def _mark_recovery_status(self, log_id: int, status: str, error_message: str = None):
        """Mark recovery log with final status in current node"""
        connection = None
        cursor = None
        try:
            connection = self.get_db_connection()
            cursor = connection.cursor()
            
            update_sql = """
                UPDATE recovery_log 
                SET status = %s, error_message = %s
                WHERE log_id = %s
            """
            
            cursor.execute(update_sql, (status, error_message, log_id))
            connection.commit()
            
        except Error as e:
            print(f"Failed to update recovery status: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def _mark_recovery_status_in_node(self, node_id: int, log_id: int, status: str, error_message: str = None):
        """Mark recovery log with final status in specified node"""
        connection = None
        cursor = None
        try:
            # Get configuration for the specified node
            from python.db.db_config import get_node_config
            node_config = get_node_config(node_id)
            
            connection = mysql.connector.connect(**node_config)
            cursor = connection.cursor()
            
            update_sql = """
                UPDATE recovery_log 
                SET status = %s, error_message = %s
                WHERE log_id = %s
            """
            
            cursor.execute(update_sql, (status, error_message, log_id))
            connection.commit()
            
        except Error as e:
            print(f"Failed to update recovery status in Node {node_id}: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def _increment_retry_count(self, log_id: int, error_message: str):
        """Increment retry count for recovery log in current node"""
        connection = None
        cursor = None
        try:
            connection = self.get_db_connection()
            cursor = connection.cursor()
            
            update_sql = """
                UPDATE recovery_log 
                SET retry_count = retry_count + 1, error_message = %s
                WHERE log_id = %s
            """
            
            cursor.execute(update_sql, (error_message, log_id))
            connection.commit()
            
        except Error as e:
            print(f"Failed to increment retry count: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def _increment_retry_count_in_node(self, node_id: int, log_id: int, error_message: str):
        """Increment retry count for recovery log in specified node"""
        connection = None
        cursor = None
        try:
            # Get configuration for the specified node
            from python.db.db_config import get_node_config
            node_config = get_node_config(node_id)
            
            connection = mysql.connector.connect(**node_config)
            cursor = connection.cursor()
            
            update_sql = """
                UPDATE recovery_log 
                SET retry_count = retry_count + 1, error_message = %s
                WHERE log_id = %s
            """
            
            cursor.execute(update_sql, (error_message, log_id))
            connection.commit()
            
        except Error as e:
            print(f"Failed to increment retry count in Node {node_id}: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def get_recovery_status(self) -> Dict:
        """Get recovery logs status summary from current node"""
        connection = None
        cursor = None
        try:
            connection = self.get_db_connection()
            cursor = connection.cursor()
            
            status_sql = """
                SELECT status, COUNT(*) as count
                FROM recovery_log 
                GROUP BY status
            """
            
            cursor.execute(status_sql)
            results = cursor.fetchall()
            
            status_summary = {'PENDING': 0, 'COMPLETED': 0, 'FAILED': 0}
            for status, count in results:
                status_summary[status] = count
                
            return status_summary
            
        except Error as e:
            print(f"Error getting recovery status: {e}")
            return {}
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def get_global_recovery_status(self) -> Dict:
        """Get recovery logs status summary across all nodes"""
        global_status = {
            'nodes': {},
            'total': {'PENDING': 0, 'COMPLETED': 0, 'FAILED': 0}
        }
        
        # Check all nodes
        for node_id in [1, 2, 3]:
            try:
                from python.db.db_config import get_node_config
                node_config = get_node_config(node_id)
                
                connection = None
                cursor = None
                try:
                    connection = mysql.connector.connect(**node_config)
                    cursor = connection.cursor()
                    
                    status_sql = """
                        SELECT status, COUNT(*) as count
                        FROM recovery_log 
                        GROUP BY status
                    """
                    
                    cursor.execute(status_sql)
                    results = cursor.fetchall()
                    
                    node_status = {'PENDING': 0, 'COMPLETED': 0, 'FAILED': 0}
                    for status, count in results:
                        node_status[status] = count
                        global_status['total'][status] += count
                    
                    global_status['nodes'][f'node_{node_id}'] = node_status
                    
                except Error as e:
                    global_status['nodes'][f'node_{node_id}'] = {'error': f'Could not connect: {str(e)}'}
                finally:
                    if cursor:
                        cursor.close()
                    if connection:
                        connection.close()
                        
            except Exception as e:
                global_status['nodes'][f'node_{node_id}'] = {'error': f'Configuration error: {str(e)}'}
        
        return global_status


# Utility Functions for Integration

def execute_sql_on_local_db(sql_statement: str, db_config: Dict) -> bool:
    """
    Execute SQL statement on local database
    Used by log_backup function
    """
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        connection.commit()
        return True
    except Error as e:
        print(f"SQL execution failed: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def simulate_replication_failure(target_node_id: int, source_node_id: int, sql_query: str, 
                               recovery_manager: RecoveryManager) -> Dict:
    """
    Simulate replication failure and log for recovery
    
    Returns:
        Dict: Status of the replication attempt
    """
    try:
        # Simulate attempting to replicate to target node
        # This is where you would normally try to execute on target node
        # For simulation, we'll assume it fails
        
        print(f"Simulating replication failure: Node {source_node_id} → Node {target_node_id}")
        print(f"Failed SQL: {sql_query}")
        
        # Log the failure for recovery
        success = recovery_manager.log_backup(target_node_id, source_node_id, sql_query)
        
        if success:
            return {
                'status': 'error',
                'message': f'Replication to Node {target_node_id} failed. Transaction logged for recovery.',
                'logged': True
            }
        else:
            return {
                'status': 'error', 
                'message': f'Replication to Node {target_node_id} failed. Failed to log for recovery!',
                'logged': False
            }
            
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Replication error: {str(e)}',
            'logged': False
        }


def replicate_transaction(query: str, source_node: int, target_node: int, isolation_level: str = "READ COMMITTED") -> Dict:
    """
    Replicate a transaction from source node to target node with recovery logging
    
    Args:
        query: SQL statement to replicate
        source_node: Node that originated the transaction
        target_node: Node to replicate to
        isolation_level: Transaction isolation level
        
    Returns:
        Dict: Replication result with status and message
    """
    from python.db.db_config import get_node_config, create_dedicated_connection
    
    try:
        print(f"Attempting replication: Node {source_node} -> Node {target_node}")
        
        # Create connection to target node
        conn = create_dedicated_connection(target_node, isolation_level)
        cursor = conn.cursor()
        
        # Execute the replication query
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Replication successful: Node {source_node} -> Node {target_node}")
        return {
            'status': 'success',
            'message': f'Successfully replicated to Node {target_node}',
            'logged': False
        }
        
    except Exception as e:
        print(f"Replication failed: Node {source_node} -> Node {target_node}: {str(e)}")
        
        # Log the failure for recovery
        try:
            # Get source node config for recovery manager
            source_config = get_node_config(source_node)
            recovery_manager = RecoveryManager(source_config, source_node)
            
            success = recovery_manager.log_backup(target_node, source_node, query)
            
            return {
                'status': 'error',
                'message': f'Replication to Node {target_node} failed: {str(e)}',
                'error_details': str(e),
                'logged': success,
                'recovery_action': f'Transaction logged for recovery on Node {target_node}' if success else 'Failed to log for recovery'
            }
            
        except Exception as log_error:
            return {
                'status': 'error', 
                'message': f'Replication to Node {target_node} failed: {str(e)}',
                'error_details': str(e),
                'logged': False,
                'recovery_action': f'Recovery logging also failed: {str(log_error)}'
            }


# Example usage for the 4 case studies
if __name__ == "__main__":
    # Example database config
    db_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'user', 
        'password': 'rootpass',
        'database': 'node1_db'
    }
    
    # Initialize recovery manager for Node 1
    recovery_manager = RecoveryManager(db_config, current_node_id=1)
    
    # Case 1 & 3: Simulate replication failures
    print("=== Case 1: Node 2 → Node 1 replication failure ===")
    simulate_replication_failure(
        target_node_id=1, 
        source_node_id=2, 
        sql_query="INSERT INTO movies (title, year) VALUES ('Test Movie', 2025)",
        recovery_manager=recovery_manager
    )
    
    # Case 2 & 4: Check and recover when node comes online
    print("\n=== Case 2: Node 1 recovery check ===")
    recovery_results = recovery_manager.check_and_recover_pending_logs()
    print(f"Recovery results: {recovery_results}")
    
    # Check status
    print("\n=== Recovery Status Summary ===")
    status = recovery_manager.get_recovery_status()
    print(f"Status summary: {status}")
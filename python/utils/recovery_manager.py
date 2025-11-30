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
        connection = None
        cursor = None
        try:
            # Determine backup node (avoid source and target nodes)
            backup_node = None
            for node in [1, 2, 3]:
                if node != source_node and node != target_node:
                    backup_node = node
                    break
            
            if backup_node:
                # Note: This would require connection to backup node
                # For now, just log locally with a flag indicating it's a cross-backup
                connection = self.get_db_connection()
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
                
        except Error as e:
            print(f"Failed to store cross-backup: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def check_and_recover_pending_logs(self) -> Dict:
        """
        Check for pending recovery logs and attempt to recover them
        Called when node starts up or manually triggered
        
        Returns:
            Dict: Recovery results summary
        """
        print(f"Node {self.current_node_id} checking for pending recovery logs...")
        
        connection = None
        cursor = None
        try:
            connection = self.get_db_connection()
            cursor = connection.cursor(dictionary=True)
            
            # Get all pending recovery logs in chronological order
            select_sql = """
                SELECT log_id, target_node, source_node, sql_statement, 
                       timestamp, retry_count, transaction_hash
                FROM recovery_log 
                WHERE status = 'PENDING' 
                ORDER BY timestamp ASC
            """
            
            cursor.execute(select_sql)
            pending_logs = cursor.fetchall()
            
            recovery_results = {
                'total_logs': len(pending_logs),
                'recovered': 0,
                'failed': 0,
                'skipped': 0
            }
            
            if not pending_logs:
                print("No pending recovery logs found.")
                return recovery_results
            
            print(f"Found {len(pending_logs)} pending recovery logs. Starting recovery...")
            
            for log in pending_logs:
                result = self._attempt_recovery(log)
                recovery_results[result] += 1
                
                # Small delay between recovery attempts
                time.sleep(0.1)
            
            print(f"Recovery completed: {recovery_results}")
            return recovery_results
            
        except Error as e:
            print(f"Error during recovery check: {e}")
            return {'error': str(e)}
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def _attempt_recovery(self, log: Dict) -> str:
        """
        Attempt to recover a single transaction log
        
        Args:
            log: Recovery log record
            
        Returns:
            str: Recovery result ('recovered', 'failed', 'skipped')
        """
        log_id = log['log_id']
        target_node = log['target_node']
        sql_statement = log['sql_statement']
        retry_count = log['retry_count']
        
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
    
    def _mark_recovery_status(self, log_id: int, status: str, error_message: str = None):
        """Mark recovery log with final status"""
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
    
    def _increment_retry_count(self, log_id: int, error_message: str):
        """Increment retry count for recovery log"""
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
    
    def get_recovery_status(self) -> Dict:
        """Get recovery logs status summary"""
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
"""
MySQL-Based Distributed Lock Manager

This module provides distributed locking using MySQL tables across multiple database nodes.
Unlike Redis-based locking, this approach uses the existing MySQL infrastructure,
making it ideal for Google Cloud deployments without additional services.

Each MySQL node has a 'distributed_lock' table that tracks lock ownership.
The lock manager coordinates locks across all three nodes to ensure consistency.
"""

import mysql.connector
import time
from datetime import datetime
from typing import Optional, Dict, Any


class DistributedLockManager:
    """
    MySQL-based distributed lock manager for coordinating transactions across nodes.
    
    Uses a dedicated 'distributed_lock' table in each MySQL node to track lock ownership.
    This approach works seamlessly when deployed on Google Cloud SQL instances.
    """
    
    def __init__(self, node_configs: Dict[int, Dict[str, Any]], current_node_id: str = "app"):
        """
        Initialize the distributed lock manager.
        
        Args:
            node_configs: Dictionary mapping node numbers to their connection configs
                         Format: {1: {...}, 2: {...}, 3: {...}}
            current_node_id: Identifier for this application instance (e.g., "app", "node1_app")
        """
        self.node_configs = node_configs
        self.current_node_id = current_node_id
        self.available = True
        self._active_locks = {}  # Track locks we currently hold: {resource_id: [node_list]}
        
        # Verify lock tables exist on all nodes
        self._initialize_lock_tables()
    
    def _initialize_lock_tables(self):
        """
        Ensure distributed_lock table exists on all nodes.
        Creates the table if it doesn't exist.
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS distributed_lock (
            lock_name VARCHAR(255) PRIMARY KEY,
            locked_by VARCHAR(255) NOT NULL,
            lock_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_locked_by (locked_by),
            INDEX idx_lock_time (lock_time)
        ) ENGINE=InnoDB;
        """
        
        for node_num, config in self.node_configs.items():
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                conn.commit()
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"Warning: Could not initialize lock table on Node {node_num}: {e}")
    
    def _get_connection(self, node: int) -> mysql.connector.connection.MySQLConnection:
        """
        Get a connection to a specific node.
        
        Args:
            node: Node number (1, 2, or 3)
            
        Returns:
            MySQL connection object
            
        Raises:
            ValueError: If node number is invalid
            Exception: If connection fails
        """
        if node not in self.node_configs:
            raise ValueError(f"Invalid node number: {node}")
        
        try:
            return mysql.connector.connect(**self.node_configs[node])
        except Exception as e:
            raise Exception(f"Failed to connect to Node {node}: {e}")
    
    def acquire_lock(self, resource_id: str, node: int, timeout: int = 30) -> bool:
        """
        Acquire a lock on a specific resource at a specific node.
        
        This method attempts to insert a lock record into the node's distributed_lock table.
        If the lock already exists and is older than the timeout, it's considered stale and replaced.
        
        Args:
            resource_id: Unique identifier for the resource (e.g., "trans_123", "account_456")
            node: Node number where the lock should be acquired
            timeout: Maximum seconds to wait for lock acquisition (default: 30)
            
        Returns:
            bool: True if lock acquired successfully, False otherwise
        """
        lock_name = f"lock_{resource_id}"
        start_time = time.time()
        
        select_lock_sql = """
        SELECT locked_by, lock_time 
        FROM distributed_lock 
        WHERE lock_name = %s
        """
        
        insert_lock_sql = """
        INSERT INTO distributed_lock (lock_name, locked_by) 
        VALUES (%s, %s)
        """
        
        update_lock_sql = """
        UPDATE distributed_lock 
        SET locked_by = %s, lock_time = NOW() 
        WHERE lock_name = %s
        """
        
        delete_stale_lock_sql = """
        DELETE FROM distributed_lock 
        WHERE lock_name = %s AND locked_by = %s
        """
        
        conn = None
        cursor = None
        
        try:
            conn = self._get_connection(node)
            cursor = conn.cursor(dictionary=True)
            
            # Loop until we acquire the lock or timeout
            while True:
                elapsed = time.time() - start_time
                
                if elapsed > timeout:
                    print(f"[{self.current_node_id}] Lock acquisition timeout for {resource_id} on Node {node}")
                    return False
                
                try:
                    # Check if lock exists
                    cursor.execute(select_lock_sql, (lock_name,))
                    result = cursor.fetchone()
                    
                    if result is None:
                        # Lock doesn't exist - try to create it
                        try:
                            cursor.execute(insert_lock_sql, (lock_name, self.current_node_id))
                            conn.commit()
                            
                            # Track this lock
                            if resource_id not in self._active_locks:
                                self._active_locks[resource_id] = []
                            self._active_locks[resource_id].append(node)
                            
                            print(f"[{self.current_node_id}] ✓ Acquired lock on {resource_id} at Node {node}")
                            return True
                            
                        except mysql.connector.IntegrityError:
                            # Another transaction inserted the lock between our SELECT and INSERT
                            # Loop and try again
                            time.sleep(0.1)
                            continue
                    
                    else:
                        # Lock exists - check if it's ours
                        if result['locked_by'] == self.current_node_id:
                            print(f"[{self.current_node_id}] ✓ Already hold lock on {resource_id} at Node {node}")
                            
                            # Track this lock if not already tracked
                            if resource_id not in self._active_locks:
                                self._active_locks[resource_id] = []
                            if node not in self._active_locks[resource_id]:
                                self._active_locks[resource_id].append(node)
                            
                            return True
                        
                        # Lock is held by another transaction
                        lock_age = (datetime.now() - result['lock_time']).total_seconds()
                        
                        # If lock is stale (older than timeout), we can take it over
                        if lock_age > timeout:
                            print(f"[{self.current_node_id}] Removing stale lock on {resource_id} at Node {node} (age: {lock_age:.2f}s)")
                            cursor.execute(delete_stale_lock_sql, (lock_name, result['locked_by']))
                            conn.commit()
                            # Loop and try to acquire again
                            continue
                        
                        # Wait and retry
                        print(f"[{self.current_node_id}] Waiting for lock on {resource_id} at Node {node} (held by {result['locked_by']})")
                        time.sleep(0.5)
                        continue
                
                except Exception as e:
                    print(f"[{self.current_node_id}] Error acquiring lock on Node {node}: {e}")
                    conn.rollback()
                    return False
        
        except Exception as e:
            print(f"[{self.current_node_id}] Failed to connect to Node {node} for lock acquisition: {e}")
            return False
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def release_lock(self, resource_id: str, node: int) -> bool:
        """
        Release a lock on a specific resource at a specific node.
        
        Args:
            resource_id: Unique identifier for the resource
            node: Node number where the lock should be released
            
        Returns:
            bool: True if lock released successfully, False otherwise
        """
        lock_name = f"lock_{resource_id}"
        
        delete_lock_sql = """
        DELETE FROM distributed_lock 
        WHERE lock_name = %s AND locked_by = %s
        """
        
        conn = None
        cursor = None
        
        try:
            conn = self._get_connection(node)
            cursor = conn.cursor()
            
            cursor.execute(delete_lock_sql, (lock_name, self.current_node_id))
            conn.commit()
            
            # Remove from tracking
            if resource_id in self._active_locks and node in self._active_locks[resource_id]:
                self._active_locks[resource_id].remove(node)
                if not self._active_locks[resource_id]:
                    del self._active_locks[resource_id]
            
            print(f"[{self.current_node_id}] ✓ Released lock on {resource_id} at Node {node}")
            return True
        
        except Exception as e:
            print(f"[{self.current_node_id}] Error releasing lock on Node {node}: {e}")
            return False
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def acquire_multi_node_lock(self, resource_id: str, nodes: list, timeout: int = 30) -> bool:
        """
        Acquire locks on the same resource across multiple nodes atomically.
        
        This is crucial for distributed transactions that need to update the same
        record on multiple nodes simultaneously. If any lock acquisition fails,
        all previously acquired locks are released.
        
        Args:
            resource_id: Unique identifier for the resource
            nodes: List of node numbers where locks should be acquired
            timeout: Maximum seconds to wait for all lock acquisitions
            
        Returns:
            bool: True if all locks acquired successfully, False otherwise
        """
        start_time = time.time()
        acquired_nodes = []
        
        try:
            for node in nodes:
                remaining_timeout = timeout - (time.time() - start_time)
                
                if remaining_timeout <= 0:
                    print(f"[{self.current_node_id}] Multi-node lock timeout for {resource_id}")
                    raise Exception("Timeout during multi-node lock acquisition")
                
                if not self.acquire_lock(resource_id, node, int(remaining_timeout)):
                    raise Exception(f"Failed to acquire lock on Node {node}")
                
                acquired_nodes.append(node)
            
            print(f"[{self.current_node_id}] ✓ Acquired multi-node lock on {resource_id} across nodes {nodes}")
            return True
        
        except Exception as e:
            # Release all acquired locks
            print(f"[{self.current_node_id}] Multi-node lock failed for {resource_id}: {e}")
            for node in acquired_nodes:
                self.release_lock(resource_id, node)
            return False
    
    def release_multi_node_lock(self, resource_id: str, nodes: list) -> bool:
        """
        Release locks on a resource across multiple nodes.
        
        Args:
            resource_id: Unique identifier for the resource
            nodes: List of node numbers where locks should be released
            
        Returns:
            bool: True if all locks released successfully
        """
        success = True
        for node in nodes:
            if not self.release_lock(resource_id, node):
                success = False
        
        if success:
            print(f"[{self.current_node_id}] ✓ Released multi-node lock on {resource_id} across nodes {nodes}")
        
        return success
    
    def check_lock(self, resource_id: str, node: int) -> Optional[Dict[str, Any]]:
        """
        Check if a lock exists on a resource at a specific node.
        
        Args:
            resource_id: Unique identifier for the resource
            node: Node number to check
            
        Returns:
            dict with lock info if lock exists, None otherwise
            Format: {'locked_by': str, 'lock_time': datetime}
        """
        lock_name = f"lock_{resource_id}"
        
        select_lock_sql = """
        SELECT locked_by, lock_time 
        FROM distributed_lock 
        WHERE lock_name = %s
        """
        
        conn = None
        cursor = None
        
        try:
            conn = self._get_connection(node)
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute(select_lock_sql, (lock_name,))
            result = cursor.fetchone()
            
            return result
        
        except Exception as e:
            print(f"[{self.current_node_id}] Error checking lock on Node {node}: {e}")
            return None
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def release_all_locks(self):
        """
        Release all locks held by this application instance across all nodes.
        
        This should be called during cleanup or recovery scenarios.
        """
        delete_all_sql = """
        DELETE FROM distributed_lock 
        WHERE locked_by = %s
        """
        
        total_released = 0
        
        for node_num in self.node_configs.keys():
            conn = None
            cursor = None
            
            try:
                conn = self._get_connection(node_num)
                cursor = conn.cursor()
                
                cursor.execute(delete_all_sql, (self.current_node_id,))
                released = cursor.rowcount
                conn.commit()
                
                if released > 0:
                    print(f"[{self.current_node_id}] Released {released} locks on Node {node_num}")
                    total_released += released
            
            except Exception as e:
                print(f"[{self.current_node_id}] Error releasing locks on Node {node_num}: {e}")
            
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        
        # Clear tracking
        self._active_locks = {}
        
        if total_released > 0:
            print(f"[{self.current_node_id}] ✓ Released {total_released} total locks across all nodes")
    
    def get_active_locks(self) -> Dict[str, list]:
        """
        Get all locks currently held by this application instance.
        
        Returns:
            dict: Mapping of resource_id to list of nodes where locks are held
        """
        return self._active_locks.copy()
    
    def is_available(self) -> bool:
        """
        Check if the lock manager is available.
        
        Returns:
            bool: True if available, False otherwise
        """
        return self.available
    
    def close(self):
        """
        Cleanup: release all locks and close connections.
        Should be called before shutting down the application.
        """
        print(f"[{self.current_node_id}] Closing lock manager and releasing all locks...")
        self.release_all_locks()


def get_lock_manager(node_configs: Dict[int, Dict[str, Any]], current_node_id: str = "app") -> DistributedLockManager:
    """
    Factory function to create a DistributedLockManager instance.
    
    Args:
        node_configs: Dictionary mapping node numbers to connection configs
        current_node_id: Identifier for this application instance
        
    Returns:
        DistributedLockManager instance
    """
    return DistributedLockManager(node_configs, current_node_id)

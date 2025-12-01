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
        
        initialized_nodes = []
        failed_nodes = []
        
        for node_num, config in self.node_configs.items():
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                conn.commit()
                cursor.close()
                conn.close()
                initialized_nodes.append(node_num)
                print(f"✓ Lock table initialized on Node {node_num}")
            except Exception as e:
                failed_nodes.append(node_num)
                print(f"✗ Failed to initialize lock table on Node {node_num}: {e}")
        
        if not initialized_nodes:
            raise Exception("CRITICAL: Failed to initialize lock tables on ANY node. System cannot operate.")
        
        if failed_nodes:
            print(f"WARNING: Lock tables only available on nodes {initialized_nodes}. Nodes {failed_nodes} unavailable.")
        else:
            print(f"✓ Lock tables successfully initialized on all nodes: {initialized_nodes}")
    
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
        FOR UPDATE  -- Row-level lock to prevent race conditions
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
                    # Start transaction for atomic lock check-and-acquire
                    cursor.execute("START TRANSACTION")
                    
                    # Check if lock exists with row-level lock (FOR UPDATE prevents race conditions)
                    cursor.execute(select_lock_sql, (lock_name,))
                    result = cursor.fetchone()
                    
                    if result is None:
                        # Lock doesn't exist - create it atomically
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
                            # Should not happen with FOR UPDATE, but handle gracefully
                            conn.rollback()
                            time.sleep(0.1)
                            continue
                    
                    else:
                        # Lock exists - check if it's ours
                        if result['locked_by'] == self.current_node_id:
                            conn.commit()  # Release the row lock
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
                        
                        # Lock is held by another active session - rollback and wait
                        conn.rollback()
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
    
    def check_lock(self, resource_id: str, node: int) -> bool:
        """
        Check if we currently hold a valid lock on the resource.
        Similar to check_us_lock() in reference implementation.
        
        Args:
            resource_id: The resource to check
            node: Node number to check lock on
            
        Returns:
            bool: True if we hold a valid lock, False otherwise
        """
        lock_name = f"lock_{resource_id}"
        
        select_lock_sql = """
        SELECT locked_by, lock_time 
        FROM distributed_lock 
        WHERE lock_name = %s
        """
        
        try:
            conn = self._get_connection(node)
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute(select_lock_sql, (lock_name,))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result is None:
                print(f"[{self.current_node_id}] No lock exists on {resource_id} at Node {node}")
                return False
            
            if result['locked_by'] == self.current_node_id:
                return True
            else:
                print(f"[{self.current_node_id}] ✗ Lock on {resource_id} at Node {node} held by {result['locked_by']}")
                return False
                
        except Exception as e:
            print(f"[{self.current_node_id}] Error checking lock on {resource_id} at Node {node}: {e}")
            return False
    
    def acquire_multi_node_lock(self, resource_id: str, nodes: list, timeout: int = 30) -> bool:
        """
        Acquire locks on the same resource across multiple available nodes.
        
        This implements the GROWING PHASE of 2-Phase Locking (2PL) with FAULT TOLERANCE.
        Locks are acquired on any available nodes, even if some nodes are down.
        This ensures high availability - operations succeed as long as at least 1 node is up.
        
        Args:
            resource_id: Unique identifier for the resource
            nodes: List of node numbers where locks should be attempted
            timeout: Maximum seconds to wait for lock acquisitions
            
        Returns:
            bool: True if locks acquired on at least 1 node, False if all nodes failed
        """
        start_time = time.time()
        acquired_nodes = []
        failed_nodes = []
        
        print(f"[{self.current_node_id}] 📈 2PL GROWING PHASE: Acquiring locks on {resource_id} across nodes {nodes}")
        
        # FAULT TOLERANT: Try each node, continue even if some fail
        for node in nodes:
            remaining_timeout = timeout - (time.time() - start_time)
            
            if remaining_timeout <= 0:
                print(f"[{self.current_node_id}] ⏱️ Lock acquisition timeout reached")
                break
            
            try:
                if self.acquire_lock(resource_id, node, int(remaining_timeout)):
                    acquired_nodes.append(node)
                    print(f"[{self.current_node_id}]   ✓ Lock acquired on Node {node}")
                else:
                    failed_nodes.append(node)
                    print(f"[{self.current_node_id}]   ⚠️ Could not acquire lock on Node {node} (may be held)")
            except Exception as e:
                failed_nodes.append(node)
                print(f"[{self.current_node_id}]   ⚠️ Node {node} unavailable: {str(e)[:100]}")
        
        # HIGH AVAILABILITY: Succeed if ANY nodes acquired locks
        if acquired_nodes:
            # Track which specific nodes have locks
            self._active_locks[resource_id] = acquired_nodes
            
            if failed_nodes:
                print(f"[{self.current_node_id}] ✅ 2PL GROWING PHASE COMPLETE: Locks acquired on {len(acquired_nodes)}/{len(nodes)} nodes {acquired_nodes}")
                print(f"[{self.current_node_id}]    ℹ️ Nodes unavailable: {failed_nodes} (high availability mode)")
            else:
                print(f"[{self.current_node_id}] ✅ 2PL GROWING PHASE COMPLETE: All locks acquired on {resource_id}")
            
            # ACTIVE SYNC: Sync existing locks to recovered nodes
            self._sync_locks_to_recovered_nodes(resource_id, acquired_nodes, failed_nodes)
            
            return True
        else:
            print(f"[{self.current_node_id}] ❌ 2PL GROWING PHASE FAILED: No nodes available for {resource_id}")
            return False
    
    def release_multi_node_lock(self, resource_id: str, nodes: list) -> bool:
        """
        Release locks on a resource across all nodes (including recovered ones).
        
        This implements the SHRINKING PHASE of 2-Phase Locking (2PL) with FAULT TOLERANCE.
        Attempts to release on ALL specified nodes, not just where locks were acquired.
        This provides self-healing: if a node was down during acquisition and comes back
        during the transaction, we'll clean it up during release.
        
        Args:
            resource_id: Unique identifier for the resource
            nodes: List of all node numbers where locks should be released
            
        Returns:
            bool: True if at least one lock released successfully
        """
        # Get list of nodes where we actually acquired locks
        acquired_nodes = self._active_locks.get(resource_id, [])
        
        print(f"[{self.current_node_id}] 📉 2PL SHRINKING PHASE: Releasing locks on {resource_id}")
        print(f"[{self.current_node_id}]    Locks were on: {acquired_nodes}, attempting release on all: {nodes}")
        
        released_count = 0
        failed_nodes = []
        
        # FAULT TOLERANT: Try to release on ALL nodes (self-healing)
        for node in nodes:
            try:
                if self.release_lock(resource_id, node):
                    released_count += 1
                    print(f"[{self.current_node_id}]   ✓ Lock released on Node {node}")
                else:
                    # Lock might not exist on this node (was down during acquisition)
                    pass
            except Exception as e:
                failed_nodes.append(node)
                print(f"[{self.current_node_id}]   ⚠️ Could not reach Node {node}: {str(e)[:50]}")
        
        # Remove from active locks tracking
        if resource_id in self._active_locks:
            del self._active_locks[resource_id]
        
        if released_count > 0:
            print(f"[{self.current_node_id}] ✅ 2PL SHRINKING PHASE COMPLETE: Released locks on {released_count}/{len(nodes)} nodes")
            return True
        else:
            print(f"[{self.current_node_id}] ⚠️ 2PL SHRINKING PHASE: No locks could be released")
            return False
    
    def _sync_locks_to_recovered_nodes(self, resource_id: str, healthy_nodes: list, failed_nodes: list):
        """
        Actively sync existing locks to nodes that have recovered.
        
        On-demand active sync: When a transaction starts, if some nodes were unavailable
        during initial lock acquisition but have locks on other nodes, we detect recovered
        nodes and sync the lock records to them.
        
        Args:
            resource_id: The resource being locked
            healthy_nodes: Nodes where locks were successfully acquired
            failed_nodes: Nodes that failed during lock acquisition
        """
        if not failed_nodes or not healthy_nodes:
            return  # Nothing to sync
        
        # Get a lock record from one of the healthy nodes
        source_node = healthy_nodes[0]
        lock_name = f"lock_{resource_id}"
        
        try:
            # Read lock from source node
            source_conn = self._get_connection(source_node)
            source_cursor = source_conn.cursor(dictionary=True)
            source_cursor.execute(
                "SELECT locked_by, lock_time FROM distributed_lock WHERE lock_name = %s",
                (lock_name,)
            )
            lock_record = source_cursor.fetchone()
            source_cursor.close()
            source_conn.close()
            
            if not lock_record:
                return  # No lock to sync
            
            # Try to sync to each failed node (they might have recovered)
            for target_node in failed_nodes:
                try:
                    target_conn = self._get_connection(target_node)
                    target_cursor = target_conn.cursor()
                    
                    # Insert or replace lock on recovered node
                    target_cursor.execute("""
                        INSERT INTO distributed_lock (lock_name, locked_by, lock_time)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE locked_by = VALUES(locked_by), lock_time = VALUES(lock_time)
                    """, (lock_name, lock_record['locked_by'], lock_record['lock_time']))
                    
                    target_conn.commit()
                    target_cursor.close()
                    target_conn.close()
                    
                    print(f"[{self.current_node_id}]   🔄 Synced lock to recovered Node {target_node}")
                    
                    # Update tracking to include this node
                    if resource_id in self._active_locks and target_node not in self._active_locks[resource_id]:
                        self._active_locks[resource_id].append(target_node)
                    
                except Exception as e:
                    # Node still down or sync failed, ignore
                    pass
                    
        except Exception as e:
            # Source node issue, can't sync
            pass
    
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

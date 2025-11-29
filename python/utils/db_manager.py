"""
Database Manager with Distributed Lock Support

Handles database operations across multiple nodes with proper distributed locking
to ensure consistency in concurrent transactions. Works locally and on Google Cloud.
"""

import mysql.connector
from typing import Dict, Any, Optional, List
from python.utils.lock_manager import DistributedLockManager
import time
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    """
    Manages database operations across 3-node distributed system with locking.
    
    Features:
    - Multi-node connection management
    - Distributed lock coordination via MySQL tables
    - Transaction management with proper isolation levels
    - Replication strategy (Node 1 <-> Nodes 2/3)
    - Works on local Docker and Google Cloud SQL
    """
    
    def __init__(self, use_cloud_sql: bool = False, current_node_id: str = "app"):
        """
        Initialize database manager.
        
        Args:
            use_cloud_sql: If True, use Google Cloud SQL configs; else use local Docker
            current_node_id: Identifier for this application instance
        """
        self.use_cloud_sql = use_cloud_sql
        self.current_node_id = current_node_id
        
        # Node configurations
        if use_cloud_sql:
            self.node_configs = {
                1: {
                    'host': os.getenv('CLOUD_DB_HOST'),
                    'port': int(os.getenv('CLOUD_DB_PORT', 3306)),
                    'user': os.getenv('CLOUD_DB_USER'),
                    'password': os.getenv('CLOUD_DB_PASSWORD'),
                    'database': os.getenv('CLOUD_DB_NAME', 'node1_db')
                },
                2: {
                    'host': os.getenv('CLOUD_DB_HOST_NODE2', os.getenv('CLOUD_DB_HOST')),
                    'port': int(os.getenv('CLOUD_DB_PORT_NODE2', 3306)),
                    'user': os.getenv('CLOUD_DB_USER_NODE2', os.getenv('CLOUD_DB_USER')),
                    'password': os.getenv('CLOUD_DB_PASSWORD_NODE2', os.getenv('CLOUD_DB_PASSWORD')),
                    'database': os.getenv('CLOUD_DB_NAME_NODE2', 'node2_db')
                },
                3: {
                    'host': os.getenv('CLOUD_DB_HOST_NODE3', os.getenv('CLOUD_DB_HOST')),
                    'port': int(os.getenv('CLOUD_DB_PORT_NODE3', 3306)),
                    'user': os.getenv('CLOUD_DB_USER_NODE3', os.getenv('CLOUD_DB_USER')),
                    'password': os.getenv('CLOUD_DB_PASSWORD_NODE3', os.getenv('CLOUD_DB_PASSWORD')),
                    'database': os.getenv('CLOUD_DB_NAME_NODE3', 'node3_db')
                }
            }
        else:
            # Local Docker configuration
            self.node_configs = {
                1: {
                    'host': 'localhost',
                    'port': 3306,
                    'user': 'user',
                    'password': 'rootpass',
                    'database': 'node1_db'
                },
                2: {
                    'host': 'localhost',
                    'port': 3307,
                    'user': 'user',
                    'password': 'rootpass',
                    'database': 'node2_db'
                },
                3: {
                    'host': 'localhost',
                    'port': 3308,
                    'user': 'user',
                    'password': 'rootpass',
                    'database': 'node3_db'
                }
            }
        
        # Initialize distributed lock manager
        self.lock_manager = DistributedLockManager(self.node_configs, current_node_id)
    
    def get_connection(self, node: int) -> mysql.connector.connection.MySQLConnection:
        """
        Get a connection to a specific node.
        
        Args:
            node: Node number (1, 2, or 3)
            
        Returns:
            MySQL connection object
        """
        if node not in self.node_configs:
            raise ValueError(f"Invalid node number: {node}")
        
        try:
            return mysql.connector.connect(**self.node_configs[node])
        except Exception as e:
            config = self.node_configs[node]
            raise Exception(f"Failed to connect to Node {node} ({config['host']}:{config['port']}): {e}")
    
    def create_dedicated_connection(self, node: int, isolation_level: str = "REPEATABLE READ") -> mysql.connector.connection.MySQLConnection:
        """
        Create a dedicated connection with specific isolation level.
        Use this for concurrent transaction testing.
        
        Args:
            node: Node number (1, 2, or 3)
            isolation_level: Transaction isolation level
            
        Returns:
            MySQL connection with isolation level set
        """
        conn = self.get_connection(node)
        cursor = conn.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
        cursor.close()
        return conn
    
    def execute_with_lock(self, query: str, params: tuple, resource_id: str, 
                         target_node: int, isolation_level: str = "READ COMMITTED",
                         timeout: int = 30) -> Dict[str, Any]:
        """
        Execute a write query with distributed locking.
        
        This method:
        1. Acquires lock on the resource
        2. Executes the query
        3. Commits the transaction
        4. Releases the lock
        
        Args:
            query: SQL query to execute (INSERT, UPDATE, DELETE)
            params: Query parameters (tuple)
            resource_id: Unique identifier for the resource being modified
            target_node: Node where the query should execute
            isolation_level: Transaction isolation level
            timeout: Lock acquisition timeout in seconds
            
        Returns:
            dict with status, affected_rows, and message
        """
        conn = None
        cursor = None
        
        try:
            # Acquire lock on the resource
            if not self.lock_manager.acquire_lock(resource_id, target_node, timeout):
                return {
                    'status': 'failed',
                    'error': f'Failed to acquire lock on {resource_id} at Node {target_node}',
                    'affected_rows': 0
                }
            
            # Execute the query
            conn = self.create_dedicated_connection(target_node, isolation_level)
            cursor = conn.cursor()
            
            cursor.execute("START TRANSACTION")
            cursor.execute(query, params)
            affected_rows = cursor.rowcount
            conn.commit()
            
            return {
                'status': 'success',
                'affected_rows': affected_rows,
                'message': f'Query executed successfully on Node {target_node}'
            }
        
        except Exception as e:
            if conn:
                conn.rollback()
            return {
                'status': 'failed',
                'error': str(e),
                'affected_rows': 0
            }
        
        finally:
            # Always release the lock
            self.lock_manager.release_lock(resource_id, target_node)
            
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def execute_multi_node_write(self, query: str, params: tuple, resource_id: str,
                                 nodes: List[int], isolation_level: str = "READ COMMITTED",
                                 timeout: int = 30) -> Dict[str, Any]:
        """
        Execute the same write query across multiple nodes with distributed locking.
        
        This is for replication scenarios where the same update must happen on
        multiple nodes atomically.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            resource_id: Unique identifier for the resource
            nodes: List of nodes where query should execute
            isolation_level: Transaction isolation level
            timeout: Lock acquisition timeout
            
        Returns:
            dict with status and results per node
        """
        results = {}
        
        try:
            # Acquire locks on all nodes
            if not self.lock_manager.acquire_multi_node_lock(resource_id, nodes, timeout):
                return {
                    'status': 'failed',
                    'error': 'Failed to acquire locks on all nodes',
                    'results': {}
                }
            
            # Execute on all nodes
            for node in nodes:
                conn = None
                cursor = None
                
                try:
                    conn = self.create_dedicated_connection(node, isolation_level)
                    cursor = conn.cursor()
                    
                    cursor.execute("START TRANSACTION")
                    cursor.execute(query, params)
                    affected_rows = cursor.rowcount
                    conn.commit()
                    
                    results[node] = {
                        'status': 'success',
                        'affected_rows': affected_rows
                    }
                
                except Exception as e:
                    if conn:
                        conn.rollback()
                    
                    results[node] = {
                        'status': 'failed',
                        'error': str(e)
                    }
                    
                    # If any node fails, mark overall status as failed
                    return {
                        'status': 'failed',
                        'error': f'Failed on Node {node}: {e}',
                        'results': results
                    }
                
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
            
            return {
                'status': 'success',
                'message': f'Query executed successfully on nodes {nodes}',
                'results': results
            }
        
        finally:
            # Always release locks on all nodes
            self.lock_manager.release_multi_node_lock(resource_id, nodes)
    
    def replicate_write(self, query: str, params: tuple, resource_id: str,
                       source_node: int, isolation_level: str = "READ COMMITTED") -> Dict[str, Any]:
        """
        Execute a write on source node and replicate to appropriate target nodes.
        
        Replication strategy:
        - Node 1 (Central) -> replicate to both Node 2 and Node 3
        - Node 2 -> replicate to Node 1 (and Node 1 will replicate to Node 3)
        - Node 3 -> replicate to Node 1 (and Node 1 will replicate to Node 2)
        
        Args:
            query: SQL query to execute
            params: Query parameters
            resource_id: Unique identifier for the resource
            source_node: Node where write originates
            isolation_level: Transaction isolation level
            
        Returns:
            dict with replication status
        """
        # Determine target nodes based on source
        if source_node == 1:
            # Central node -> replicate to both partitions
            target_nodes = [1, 2, 3]
        elif source_node == 2:
            # Node 2 -> update Node 2 and Central
            target_nodes = [2, 1]
        elif source_node == 3:
            # Node 3 -> update Node 3 and Central
            target_nodes = [3, 1]
        else:
            return {'status': 'failed', 'error': f'Invalid source node: {source_node}'}
        
        # Execute on all target nodes with distributed locking
        result = self.execute_multi_node_write(
            query=query,
            params=params,
            resource_id=resource_id,
            nodes=target_nodes,
            isolation_level=isolation_level
        )
        
        return result
    
    def check_connectivity(self) -> Dict[int, bool]:
        """
        Check connectivity to all nodes.
        
        Returns:
            dict mapping node numbers to connectivity status
        """
        status = {}
        
        for node in self.node_configs.keys():
            try:
                conn = self.get_connection(node)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                conn.close()
                status[node] = True
            except Exception as e:
                print(f"Node {node} connectivity check failed: {e}")
                status[node] = False
        
        return status
    
    def cleanup(self):
        """
        Cleanup: release all locks held by this manager.
        Call this before shutting down the application.
        """
        self.lock_manager.release_all_locks()


# Example usage
if __name__ == "__main__":
    # Create database manager (local Docker)
    db_manager = DatabaseManager(use_cloud_sql=False, current_node_id="test_app")
    
    # Check connectivity
    print("Checking connectivity...")
    connectivity = db_manager.check_connectivity()
    for node, status in connectivity.items():
        print(f"Node {node}: {'✓ Connected' if status else '✗ Failed'}")
    
    # Example: Update with distributed locking
    print("\n" + "="*60)
    print("Testing distributed lock write...")
    print("="*60)
    
    result = db_manager.execute_with_lock(
        query="UPDATE trans SET amount = %s WHERE trans_id = %s",
        params=(5000.0, 1),
        resource_id="trans_1",
        target_node=1,
        isolation_level="READ COMMITTED"
    )
    
    print(f"Result: {result}")
    
    # Example: Multi-node replication
    print("\n" + "="*60)
    print("Testing multi-node replication...")
    print("="*60)
    
    result = db_manager.replicate_write(
        query="UPDATE trans SET amount = %s WHERE trans_id = %s",
        params=(6000.0, 2),
        resource_id="trans_2",
        source_node=1,
        isolation_level="READ COMMITTED"
    )
    
    print(f"Replication result: {result}")
    
    # Cleanup
    db_manager.cleanup()
    print("\n✓ Cleanup complete")

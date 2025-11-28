"""
Enhanced Database Manager for Multi-Node Transaction Testing
Supports explicit transaction control and isolation level management
"""

import mysql.connector
import time
from datetime import datetime
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Tuple
import threading

class DatabaseManager:
    def __init__(self):
        self.node_configs = {
            1: {
                'host': 'localhost',
                'port': 3306,
                'user': 'user',
                'password': 'rootpass',
                'database': 'node1_db',
                'description': 'Central Node - All Data'
            },
            2: {
                'host': 'localhost',
                'port': 3307,
                'user': 'user',
                'password': 'rootpass',
                'database': 'node2_db',
                'description': 'Partition Node - Lower Half'
            },
            3: {
                'host': 'localhost',
                'port': 3308,
                'user': 'user',
                'password': 'rootpass',
                'database': 'node3_db', 
                'description': 'Partition Node - Upper Half'
            }
        }
        self.active_connections = {}  # Store persistent connections for transactions
        self.isolation_levels = {
            'READ_UNCOMMITTED': 'READ UNCOMMITTED',
            'READ_COMMITTED': 'READ COMMITTED', 
            'REPEATABLE_READ': 'REPEATABLE READ',
            'SERIALIZABLE': 'SERIALIZABLE'
        }
    
    @contextmanager
    def get_connection(self, node_id: int, persistent_key: str = None):
        """Get database connection with context manager"""
        conn = None
        try:
            # Use persistent connection if key provided
            if persistent_key and persistent_key in self.active_connections:
                yield self.active_connections[persistent_key]
                return
                
            config = self.node_configs[node_id]
            conn = mysql.connector.connect(**{k: v for k, v in config.items() if k != 'description'})
            conn.autocommit = False
            
            # Store persistent connection if key provided
            if persistent_key:
                self.active_connections[persistent_key] = conn
                
            yield conn
        except Exception as e:
            if conn and not persistent_key:
                conn.rollback()
            raise e
        finally:
            if conn and not persistent_key:
                conn.close()
    
    def close_persistent_connection(self, key: str):
        """Close and remove persistent connection"""
        if key in self.active_connections:
            try:
                self.active_connections[key].close()
            except:
                pass
            del self.active_connections[key]
    
    def set_global_isolation_level(self, node_id: int, level: str) -> bool:
        """Set global isolation level for a node"""
        try:
            with self.get_connection(node_id) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SET GLOBAL TRANSACTION ISOLATION LEVEL {level}")
                conn.commit()
                return True
        except Exception as e:
            print(f"ERROR: Error setting global isolation level on Node {node_id}: {e}")
            return False
    
    def set_session_isolation_level(self, node_id: int, level: str, connection_key: str = None) -> bool:
        """Set session isolation level for a node"""
        try:
            with self.get_connection(node_id, connection_key) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
                return True
        except Exception as e:
            print(f"ERROR: Error setting session isolation level on Node {node_id}: {e}")
            return False
    
    def execute_query(self, node_id: int, query: str, params: tuple = None, 
                     fetch_results: bool = True, connection_key: str = None,
                     auto_commit: bool = True) -> Dict[str, Any]:
        """Execute query on specified node with timing and logging"""
        start_time = time.time()
        
        try:
            with self.get_connection(node_id, connection_key) as conn:
                cursor = conn.cursor(dictionary=True)
                
                # Execute query
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get results if needed
                results = []
                if fetch_results and cursor.description:
                    results = cursor.fetchall()
                
                # Commit if it's a write operation and auto_commit is True
                if (auto_commit and 
                    query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER'))):
                    conn.commit()
                
                end_time = time.time()
                
                return {
                    'success': True,
                    'results': results,
                    'row_count': cursor.rowcount,
                    'duration': round(end_time - start_time, 4),
                    'timestamp': datetime.now().strftime('%H:%M:%S.%f')[:-3],
                    'node_id': node_id,
                    'query': query[:100] + '...' if len(query) > 100 else query
                }
                
        except Exception as e:
            end_time = time.time()
            return {
                'success': False,
                'error': str(e),
                'duration': round(end_time - start_time, 4),
                'timestamp': datetime.now().strftime('%H:%M:%S.%f')[:-3],
                'node_id': node_id,
                'query': query[:100] + '...' if len(query) > 100 else query
            }
    
    def start_transaction(self, node_id: int, connection_key: str = None) -> Dict[str, Any]:
        """Start explicit transaction"""
        result = self.execute_query(node_id, "START TRANSACTION", 
                                  fetch_results=False, connection_key=connection_key,
                                  auto_commit=False)
        return result
    
    def commit_transaction(self, node_id: int, connection_key: str = None) -> Dict[str, Any]:
        """Commit transaction"""
        result = self.execute_query(node_id, "COMMIT", 
                                  fetch_results=False, connection_key=connection_key,
                                  auto_commit=False)
        return result
    
    def rollback_transaction(self, node_id: int, connection_key: str = None) -> Dict[str, Any]:
        """Rollback transaction"""
        result = self.execute_query(node_id, "ROLLBACK", 
                                  fetch_results=False, connection_key=connection_key,
                                  auto_commit=False)
        return result
    
    def get_transaction_record(self, node_id: int, trans_id: int, connection_key: str = None) -> Dict[str, Any]:
        """Get specific transaction record"""
        query = "SELECT * FROM trans WHERE trans_id = %s"
        return self.execute_query(node_id, query, params=(trans_id,), connection_key=connection_key)
    
    def get_transaction_record_with_lock(self, node_id: int, trans_id: int, connection_key: str = None) -> Dict[str, Any]:
        """Get specific transaction record with FOR UPDATE lock"""
        query = "SELECT * FROM trans WHERE trans_id = %s FOR UPDATE"
        return self.execute_query(node_id, query, params=(trans_id,), connection_key=connection_key)
    
    def get_transaction_record_with_delay(self, node_id: int, trans_id: int, delay_seconds: int = 2, connection_key: str = None) -> Dict[str, Any]:
        """Get transaction record with SQL SLEEP to simulate processing time"""
        query = f"SELECT *, SLEEP({delay_seconds}) as sleep_result FROM trans WHERE trans_id = %s"
        return self.execute_query(node_id, query, params=(trans_id,), connection_key=connection_key)
    
    def update_transaction_with_delay(self, node_id: int, trans_id: int, new_value: float, delay_seconds: int = 2, connection_key: str = None) -> Dict[str, Any]:
        """Update transaction with SQL SLEEP to hold locks longer"""
        # First sleep, then update
        query = f"UPDATE trans SET amount = %s WHERE trans_id = %s AND SLEEP({delay_seconds}) = 0"
        return self.execute_query(node_id, query, params=(new_value, trans_id), connection_key=connection_key, auto_commit=False)
    
    def execute_concurrent_test_setup(self, node_id: int, test_type: str = "read", delay_seconds: float = 1.0) -> Dict[str, Any]:
        """Execute SQL commands to set up concurrent test timing"""
        if test_type == "read":
            query = f"SELECT 'Concurrent Read Setup', SLEEP({delay_seconds}) as setup_delay"
        elif test_type == "write":
            query = f"SELECT 'Concurrent Write Setup', SLEEP({delay_seconds}) as setup_delay"
        elif test_type == "lock":
            query = f"SELECT 'Lock Test Setup', SLEEP({delay_seconds}) as setup_delay"
        else:
            query = f"SELECT 'General Test Setup', SLEEP({delay_seconds}) as setup_delay"
            
        return self.execute_query(node_id, query, fetch_results=True)
    
    def check_lock_status(self, node_id: int) -> Dict[str, Any]:
        """Check current lock status on the database"""
        query = """
        SELECT 
            r.trx_id,
            r.trx_mysql_thread_id,
            r.trx_query,
            r.trx_operation_state,
            r.trx_tables_locked,
            r.trx_lock_structs,
            r.trx_rows_locked
        FROM information_schema.innodb_trx r
        ORDER BY r.trx_started
        """
        return self.execute_query(node_id, query, fetch_results=True)
    
    def update_transaction_record(self, node_id: int, trans_id: int, new_value: float, 
                                connection_key: str = None) -> Dict[str, Any]:
        """Update transaction record amount"""
        query = "UPDATE trans SET amount = %s WHERE trans_id = %s"
        return self.execute_query(node_id, query, params=(new_value, trans_id), 
                                connection_key=connection_key, auto_commit=False)
    
    def test_connections(self) -> Dict[int, bool]:
        """Test connection to all nodes"""
        results = {}
        for node_id in [1, 2, 3]:
            try:
                result = self.execute_query(node_id, "SELECT 1 as test") 
                results[node_id] = result['success']
            except:
                results[node_id] = False
        return results
    
    def get_node_info(self, node_id: int) -> str:
        """Get node description"""
        return self.node_configs[node_id]['description']
    
    def cleanup_all_connections(self):
        """Clean up all persistent connections"""
        for key in list(self.active_connections.keys()):
            self.close_persistent_connection(key)
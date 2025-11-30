def _get_lock_manager(current_node_id: str = "app"):
    """
    Get or create the distributed lock manager instance.

    Args:
        current_node_id: Identifier for this application instance

    Returns:
        DistributedLockManager instance
    """
    # Lazy import to avoid Streamlit module caching issues
    from python.utils.lock_manager import DistributedLockManager

    global _lock_manager
    if _lock_manager is None:
        # Build node configs for lock manager
        lock_node_configs = {}
        for node_num in [1, 2, 3]:
            config = get_node_config(node_num)
            lock_node_configs[node_num] = config
        _lock_manager = DistributedLockManager(lock_node_configs, current_node_id)
    return _lock_manager

def execute_with_lock(query: str, params: tuple, resource_id: str,
                     target_node: int, isolation_level: str = "READ COMMITTED",
                     timeout: int = 30, current_node_id: str = "app") -> Dict[str, Any]:
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
        current_node_id: Identifier for this application instance

    Returns:
        dict with status, affected_rows, and message
    """
    lock_manager = _get_lock_manager(current_node_id)
    conn = None
    cursor = None

    try:
        # Acquire lock on the resource
        if not lock_manager.acquire_lock(resource_id, target_node, timeout):
            return {
                'status': 'failed',
                'error': f'Failed to acquire lock on {resource_id} at Node {target_node}',
                'affected_rows': 0
            }

        # Execute the query
        conn = create_dedicated_connection(target_node, isolation_level)
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
        lock_manager.release_lock(resource_id, target_node)

        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_multi_node_write(query: str, params: tuple, resource_id: str,
                             nodes: List[int], isolation_level: str = "READ COMMITTED",
                             timeout: int = 30, current_node_id: str = "app") -> Dict[str, Any]:
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
        current_node_id: Identifier for this application instance

    Returns:
        dict with status and results per node
    """
    lock_manager = _get_lock_manager(current_node_id)
    results = {}

    try:
        # Acquire locks on all nodes
        if not lock_manager.acquire_multi_node_lock(resource_id, nodes, timeout):
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
                conn = create_dedicated_connection(node, isolation_level)
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
        lock_manager.release_multi_node_lock(resource_id, nodes)


def replicate_write(query: str, params: tuple, resource_id: str,
                   source_node: int, isolation_level: str = "READ COMMITTED",
                   current_node_id: str = "app") -> Dict[str, Any]:
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
        current_node_id: Identifier for this application instance

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
    result = execute_multi_node_write(
        query=query,
        params=params,
        resource_id=resource_id,
        nodes=target_nodes,
        isolation_level=isolation_level,
        current_node_id=current_node_id
    )

    return result

def cleanup_locks(current_node_id: str = "app"):
    """
    Cleanup: release all locks held by this manager.
    Call this before shutting down the application.

    Args:
        current_node_id: Identifier for this application instance
    """
    lock_manager = _get_lock_manager(current_node_id)
    lock_manager.release_all_locks()
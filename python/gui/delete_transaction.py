"""
Delete Transaction Module - Old Logic
This module contains the original logic for deleting transactions.
"""

import streamlit as st
import pandas as pd
import time
from python.db.db_config import fetch_data, create_dedicated_connection


def render(get_node_for_account, log_transaction):
    """
    Render the Delete Transaction page with the old logic.

    Args:
        get_node_for_account: Function to determine which node to use based on account_id
        log_transaction: Function to log transactions
    """
    st.title("Delete Transaction (Write Operation)")

    st.markdown("""
    Remove a transaction record from the database. Deletions are applied to the target node.
    """)

    # Check if we just completed a deletion
    if 'last_deleted_id' in st.session_state:
        st.success(f"Transaction {st.session_state.last_deleted_id} was successfully deleted!")
        st.info("You can now delete another transaction.")
        del st.session_state.last_deleted_id

    st.subheader("Delete Transaction")

    col1, col2 = st.columns(2)

    with col1:
        trans_id = st.number_input("Transaction ID", min_value=1, value=1, key='delete_trans_id')

    with col2:
        isolation_level = st.selectbox(
            "Isolation Level",
            ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
            index=1,
            key='delete_isolation'
        )

    # Show transaction info button
    if st.button("Preview Transaction"):
        try:
            # Check node status using server pinger
            node_status = st.session_state.node_pinger.get_status()
            
            # Search for transaction with Node 1 priority, fallback to other nodes
            found_data = None
            search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
            search_results = []
            
            with st.spinner(f"Searching for transaction {trans_id} across all available nodes (fresh data)..."):
                # Try Node 1 first with fresh data (ttl=0 to bypass cache)
                if node_status.get(1, False):
                    try:
                        found_data = fetch_data(search_query, node=1, ttl=0)  # Force fresh data
                        if not found_data.empty:
                            search_results.append(("Node 1 (Central)", found_data))
                            st.success("Transaction found on Node 1 (central)")
                        else:
                            st.info("Transaction not found on Node 1")
                    except Exception as e:
                        st.warning(f"Could not search Node 1: {str(e)}")
                else:
                    st.warning("Node 1 is offline - cannot search central database")
                
                # Search other nodes regardless to check for inconsistencies
                for node in [2, 3]:
                    if node_status.get(node, False):
                        try:
                            node_data = fetch_data(search_query, node=node, ttl=0)  # Force fresh data
                            if not node_data.empty:
                                search_results.append((f"Node {node}", node_data))
                                if found_data is None or found_data.empty:
                                    found_data = node_data
                                    st.info(f"Transaction found on Node {node}")
                        except Exception as e:
                            st.warning(f"Could not search Node {node}: {str(e)}")
                    else:
                        st.warning(f"Node {node} is offline")

            # Clear any cached preview data
            if 'preview_trans_id' in st.session_state:
                del st.session_state.preview_trans_id
            
            # Check if this transaction was recently deleted in this session
            recently_deleted = []
            if 'deleted_transactions' not in st.session_state:
                st.session_state.deleted_transactions = set()
            
            if trans_id in st.session_state.deleted_transactions:
                recently_deleted.append(trans_id)
            
            # Show results
            if not search_results:
                if recently_deleted:
                    st.success(f"Transaction ID {trans_id} was successfully deleted in this session")
                    st.info("The transaction has been removed from all nodes.")
                else:
                    st.error(f"Transaction ID {trans_id} not found on any available node")
                    st.info("This transaction may have already been deleted or never existed.")
                
                # Show which nodes were checked
                checked_nodes = [f"Node {i}" for i in [1, 2, 3] if node_status.get(i, False)]
                if checked_nodes:
                    st.caption(f"Fresh search performed on: {', '.join(checked_nodes)}")
                else:
                    st.error("No nodes were available for search")
                    
            elif len(search_results) == 1:
                # Transaction found on only one node - normal case
                node_name, data = search_results[0]
                
                if recently_deleted:
                    st.warning(f"Transaction found on {node_name} but was marked as deleted in this session")
                    st.error("Data inconsistency detected - transaction may not have been properly deleted from all nodes")
                    st.info("This transaction should not exist. Consider manual cleanup or recovery.")
                else:
                    st.success(f"Transaction found on {node_name}")
                    # Store that we found this transaction
                    st.session_state.preview_trans_id = trans_id
                    
                st.dataframe(data)
                
            else:
                # Transaction found on multiple nodes - check for consistency
                if recently_deleted:
                    st.error(f"Transaction found on {len(search_results)} node(s) but was marked as deleted in this session")
                    st.error("Critical data inconsistency detected - transaction exists after deletion")
                    st.warning("The delete operation may have failed on some nodes. Check recovery logs.")
                else:
                    st.success(f"Transaction found on {len(search_results)} node(s)")
                    # Store that we found this transaction
                    st.session_state.preview_trans_id = trans_id
                
                # Check if data is consistent across nodes
                base_data = search_results[0][1]
                is_consistent = True
                
                for i, (node_name, node_data) in enumerate(search_results[1:], 1):
                    if not base_data.equals(node_data):
                        is_consistent = False
                        break
                
                if is_consistent:
                    st.success("Data is consistent across all nodes")
                    st.dataframe(base_data)
                else:
                    st.warning("Data inconsistency detected between nodes!")
                    
                    # Show data from each node
                    for node_name, data in search_results:
                        with st.expander(f"Data from {node_name}"):
                            st.dataframe(data)
                
                # Show summary of where transaction was found
                nodes_found = [result[0] for result in search_results]
                st.caption(f"Fresh data from: {', '.join(nodes_found)}")

        except Exception as e:
            st.error(f"Error during search: {str(e)}")

    st.markdown("---")
    st.warning("This action cannot be undone!")

    # Delete button with custom styling
    st.markdown("""
    <style>
    div.stButton > button {
        background-color: #4B5C4B;
        color: white;
        border-color: #4B5C4B;
    }
    div.stButton > button:hover {
        background-color: #3A4A3A;
        border-color: #3A4A3A;
    }
    /* Rollback button styling */
    button[data-testid="baseButton-secondary"]:has(p:contains("Rollback")) {
        background-color: #692727 !important;
        border-color: #692727 !important;
    }
    button[data-testid="baseButton-secondary"]:has(p:contains("Rollback")):hover {
        background-color: #531F1F !important;
        border-color: #531F1F !important;
    }
    </style>
    """, unsafe_allow_html=True)

    btn_col1, btn_col2, btn_col3 = st.columns(3)
    with btn_col1:
        delete_button = st.button("Delete Transaction", type="primary", use_container_width=True)
    with btn_col2:
        commit_button = st.button("Commit Transaction", type="secondary", use_container_width=True, key="commit_delete")
    with btn_col3:
        rollback_button = st.button("Rollback", type="secondary", use_container_width=True, key="rollback_delete")

    if commit_button:
        from python.utils.recovery_manager import replicate_transaction
        
        delete_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'delete']
        if delete_transactions:
            try:
                committed_count = 0
                indices_to_remove = []

                # Process transactions one by one
                for txn in delete_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]
                    
                    # Get transaction details
                    primary_node = txn['node']
                    account_id = txn['account_id']
                    query = txn['query']
                    isolation_level = txn['isolation_level']
                    trans_id = txn['trans_id']
                    
                    try:
                        # Commit on primary node first
                        with st.spinner(f"Deleting transaction on Node {primary_node}..."):
                            conn.commit()
                            cursor.close()
                            conn.close()
                        
                        st.info(f"Transaction deleted on Node {primary_node}")
                        
                        # Get current node status for replication decisions
                        current_node_status = st.session_state.node_pinger.get_status()
                        partition_node_for_account = get_node_for_account(account_id)
                        
                        # Determine replication targets based on primary node
                        replication_results = []
                        
                        if primary_node == 1:
                            # Primary is Node 1: replicate to partition node
                            target_node = partition_node_for_account
                            if target_node != 1:
                                with st.spinner(f"Replicating delete to Node {target_node} (partition node)..."):
                                    result = replicate_transaction(query, primary_node, target_node, isolation_level)
                                    replication_results.append((target_node, result))
                                
                        else:
                            # Primary is Node 2/3: replicate to Node 1 (and potentially other nodes)
                            # Always try to replicate to Node 1 (central)
                            with st.spinner(f"Replicating delete to Node 1 (central)..."):
                                result = replicate_transaction(query, primary_node, 1, isolation_level)
                                replication_results.append((1, result))
                            
                            # If primary is not the natural partition node, also replicate to partition node
                            if primary_node != partition_node_for_account and partition_node_for_account != 1:
                                with st.spinner(f"Replicating delete to Node {partition_node_for_account} (partition node)..."):
                                    result = replicate_transaction(query, primary_node, partition_node_for_account, isolation_level)
                                    replication_results.append((partition_node_for_account, result))
                        
                        # Display replication results
                        successful_replications = 0
                        failed_replications = 0
                        
                        for target_node, result in replication_results:
                            if result['status'] == 'error':
                                st.error(f"Delete replication to Node {target_node} failed: {result['message']}")
                                if result['logged']:
                                    st.warning(f"Recovery logged: {result['recovery_action']}")
                                else:
                                    st.error(f"Recovery logging failed: {result['recovery_action']}")
                                failed_replications += 1
                            else:
                                st.success(f"Successfully replicated delete to Node {target_node}")
                                successful_replications += 1
                        
                        # Show replication summary
                        if replication_results:
                            if failed_replications > 0:
                                st.info(f"Replication Summary: {successful_replications} successful, {failed_replications} failed (logged for recovery)")
                            else:
                                st.success(f"All delete replications successful ({successful_replications}/{len(replication_results)})")
                        
                        # Log successful transaction
                        duration = time.time() - txn['start_time']
                        log_transaction(
                            operation=txn['operation'],
                            query=txn['query'],
                            node=txn['node'],
                            isolation_level=txn['isolation_level'],
                            status='SUCCESS',
                            duration=duration
                        )
                        committed_count += 1
                        
                        # Store successful deletion for confirmation
                        st.session_state.last_deleted_id = trans_id
                        
                        # Add to deleted transactions set to track across session
                        if 'deleted_transactions' not in st.session_state:
                            st.session_state.deleted_transactions = set()
                        st.session_state.deleted_transactions.add(trans_id)
                        
                    except Exception as commit_error:
                        st.error(f"Delete commit failed on Node {primary_node}: {str(commit_error)}")
                        try:
                            conn.rollback()
                            cursor.close()
                            conn.close()
                        except:
                            pass

                # Remove processed transactions
                for idx in sorted(indices_to_remove, reverse=True):
                    del st.session_state.active_transactions[idx]
                    del st.session_state.transaction_connections[idx]
                    del st.session_state.transaction_cursors[idx]

                if committed_count > 0:
                    st.success(f"{committed_count} delete transaction(s) committed successfully!")
                    st.toast(f"{committed_count} transaction(s) deleted successfully")
                    
            except Exception as e:
                st.error(f"Delete commit process failed: {str(e)}")
        else:
            st.warning("No active DELETE transaction to commit")

    if rollback_button:
        delete_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'delete']
        if delete_transactions:
            try:
                rolled_back_count = 0
                indices_to_remove = []

                # Collect indices and rollback transactions
                for txn in delete_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    rolled_back_count += 1

                # Remove in reverse order to maintain correct indices
                for idx in sorted(indices_to_remove, reverse=True):
                    del st.session_state.active_transactions[idx]
                    del st.session_state.transaction_connections[idx]
                    del st.session_state.transaction_cursors[idx]

                st.info(f"{rolled_back_count} delete transaction(s) rolled back - data not deleted, no changes logged")
                st.toast(f"{rolled_back_count} transaction(s) rolled back")
            except Exception as e:
                st.error(f"Rollback failed: {str(e)}")
        else:
            st.warning("No active DELETE transaction to rollback")

    if delete_button:
        # Clear any stale session data
        if 'preview_trans_id' in st.session_state:
            del st.session_state.preview_trans_id
        if 'last_deleted_id' in st.session_state and st.session_state.last_deleted_id == trans_id:
            st.warning(f"Transaction {trans_id} was already deleted in this session. Please refresh or choose a different transaction ID.")
            st.stop()
            
        start_time = time.time()
        delete_query = None
        lock_acquired = False
        resource_id = f"trans_{trans_id}"  # Lock specific to this transaction

        try:
            # Check node status using server pinger
            node_status = st.session_state.node_pinger.get_status()
            
            # Acquire distributed lock before deleting
            with st.spinner(f"Acquiring distributed lock on transaction {trans_id}..."):
                # Try to acquire lock on available nodes
                lock_node = 1 if node_status.get(1, False) else None
                if not lock_node:
                    # Find any available node for locking
                    for node in [1, 2, 3]:
                        if node_status.get(node, False):
                            lock_node = node
                            break
                
                if not lock_node:
                    st.error("No nodes available for locking. Please check node connectivity.")
                    st.stop()
                
                lock_acquired = st.session_state.lock_manager.acquire_lock(
                    resource_id, node=lock_node, timeout=30
                )

                if not lock_acquired:
                    st.error(f"Failed to acquire lock on transaction {trans_id}. Another user may be modifying it. Please try again.")
                    st.stop()

            # Search for transaction with Node 1 priority, fallback to other nodes
            found_data = None
            account_id = None
            search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
            
            with st.spinner(f"Searching for transaction {trans_id} (fresh data)..."):
                # Try Node 1 first with fresh data
                if node_status.get(1, False):
                    try:
                        found_data = fetch_data(search_query, node=1, ttl=0)  # Force fresh data
                        if not found_data.empty:
                            st.info("Transaction found on Node 1 (central)")
                            account_id = int(found_data.iloc[0]['account_id'])
                    except Exception as e:
                        st.warning(f"Could not search Node 1: {str(e)}")
                        # Add recovery log for search failure
                        st.error("Error searching Node 1 - adding to recovery log")
                
                # If not found on Node 1 or Node 1 is offline, search other nodes
                if found_data is None or found_data.empty:
                    for node in [2, 3]:
                        if node_status.get(node, False):
                            try:
                                found_data = fetch_data(search_query, node=node, ttl=0)  # Force fresh data
                                if not found_data.empty:
                                    account_id = int(found_data.iloc[0]['account_id'])
                                    # Check if this creates data inconsistency
                                    if node_status.get(1, False):  # Node 1 is online but transaction not found there
                                        st.error(f"Data inconsistency detected: Transaction found on Node {node} but not on Node 1")
                                        st.warning("Proceeding with delete but will add recovery log")
                                    else:
                                        st.info(f"Transaction found on Node {node}")
                                    break
                            except Exception as e:
                                st.warning(f"Could not search Node {node}: {str(e)}")
                
                if found_data is None or found_data.empty:
                    st.error(f"Transaction ID {trans_id} not found on any available node")
                    st.warning("This transaction may have already been deleted or never existed.")
                    st.info("Use the Preview button to check if a transaction exists before deleting.")
                    st.stop()

            # Show current node status
            partition_node = get_node_for_account(account_id)
            
            # Determine primary node (Node 1 priority, fallback logic)
            if node_status.get(1, False):  # Node 1 is online
                primary_node = 1
                st.info("Using Node 1 (Central) as primary node")
            elif node_status.get(partition_node, False):  # Partition node is online
                primary_node = partition_node
                st.warning(f"Node 1 offline - Using Node {partition_node} (partition node) as primary")
            else:
                # Find any available node as last resort
                primary_node = None
                for node in [1, 2, 3]:
                    if node_status.get(node, False):
                        primary_node = node
                        break
                
                if primary_node is None:
                    st.error("All nodes are offline. Cannot proceed with delete.")
                    st.stop()
                else:
                    st.error(f"Both Node 1 and Node {partition_node} are offline - Using Node {primary_node} as emergency primary")
            
            # Show node status
            with st.expander("Current Node Status"):
                status_data = []
                for node in [1, 2, 3]:
                    is_online = node_status.get(node, False)
                    if node == primary_node:
                        role = "Primary (Active)"
                    elif node == 1 and not is_online:
                        role = "Central (Offline - will recover)"
                    elif node == partition_node and node != primary_node:
                        if is_online:
                            role = "Partition (Standby)"
                        else:
                            role = "Partition (Offline - will recover)"
                    else:
                        role = "Replica" if is_online else "Offline (will recover)"
                    
                    status_data.append({
                        'Node': f"Node {node}",
                        'Status': 'Online' if is_online else 'Offline',
                        'Role': role
                    })
                st.dataframe(pd.DataFrame(status_data))

            # Build DELETE query
            delete_query = f"DELETE FROM trans WHERE trans_id = {trans_id}"

            with st.spinner(f"Preparing delete transaction on Node {primary_node}..."):
                # Create dedicated connection to primary node only
                conn = create_dedicated_connection(primary_node, isolation_level)
                cursor = conn.cursor(dictionary=True)

                # Set isolation level and start transaction
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                cursor.execute("START TRANSACTION")

                # Execute delete but don't commit yet
                cursor.execute(delete_query)

                # Store single transaction for commit/rollback
                st.session_state.transaction_connections.append(conn)
                st.session_state.transaction_cursors.append(cursor)
                st.session_state.active_transactions.append({
                    'page': 'delete',
                    'node': primary_node,
                    'operation': 'DELETE',
                    'trans_id': trans_id,
                    'account_id': account_id,
                    'query': delete_query,
                    'isolation_level': isolation_level,
                    'start_time': start_time
                })

            duration = time.time() - start_time

            st.success(f"Delete transaction prepared on Node {primary_node} in {duration:.3f}s")
            st.warning("Transaction active - Click 'Commit' to finalize deletion or 'Rollback' to cancel")

            # Show confirmation
            with st.expander("Pending Deletion"):
                st.write(f"Transaction ID {trans_id} is marked for deletion")
                st.dataframe(found_data)
                
                if primary_node == 1:
                    st.caption(f"Delete prepared on Node 1 (central) - will replicate to Node {partition_node} on commit")
                elif primary_node == partition_node:
                    st.caption(f"Delete prepared on Node {primary_node} (natural partition for {'even' if partition_node == 2 else 'odd'} accounts) - will replicate to Node 1 (central) on commit")
                else:
                    st.caption(f"Delete prepared on Node {primary_node} (emergency primary) - will replicate to Node 1 (central) and Node {partition_node} (natural partition) on commit")

        except Exception as e:
            # Don't log failed transactions - only log successful commits
            st.error(f"Error: {str(e)}")

        finally:
            # Always release the lock
            if lock_acquired:
                st.session_state.lock_manager.release_lock(resource_id, node=lock_node)
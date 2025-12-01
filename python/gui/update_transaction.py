"""
Update Transaction Module - Old Logic
This module contains the original logic for updating transactions.
"""

import streamlit as st
import pandas as pd
import time
from python.db.db_config import fetch_data, create_dedicated_connection


def render(get_node_for_account, log_transaction):
    """
    Render the Update Transaction page with the old logic.

    Args:
        get_node_for_account: Function to determine which node to use based on account_id
        log_transaction: Function to log transactions
    """
    st.title("Update Transaction (Write Operation)")

    st.markdown("""
    Modify an existing transaction record. Updates are applied to the target node.
    """)

    st.subheader("Update Transaction")

    col1, col2 = st.columns(2)

    with col1:
        trans_id = st.number_input("Transaction ID", min_value=1, value=1)
        new_amount = st.number_input("New Amount", min_value=0.0, value=1000.0, step=100.0)
        new_type = st.selectbox("New Type", ["Credit", "Debit"])

    with col2:
        new_operation = st.text_input("New Operation", placeholder="e.g., Credit in Cash")
        isolation_level = st.selectbox(
            "Isolation Level",
            ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
            index=1,
            key='update_isolation'
        )

    # Show transaction info button
    if st.button("Preview Transaction"):
        try:
            # Search for transaction on Node 1 (central node)
            search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
            found_data = fetch_data(search_query, node=1)

            if found_data.empty:
                st.warning(f"Transaction ID {trans_id} not found")
            else:
                st.success(f"Found transaction")
                st.dataframe(found_data)

        except Exception as e:
            st.error(f"Error searching: {str(e)}")

    # Update button with custom styling
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
        update_button = st.button("Update Transaction", type="primary", use_container_width=True)
    with btn_col2:
        commit_button = st.button("Commit Transaction", type="secondary", use_container_width=True, key="commit_update")
    with btn_col3:
        rollback_button = st.button("Rollback", type="secondary", use_container_width=True, key="rollback_update")

    if commit_button:
        from python.utils.recovery_manager import replicate_transaction
        
        update_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'update']
        if update_transactions:
            try:
                committed_count = 0
                indices_to_remove = []

                # Process transactions one by one
                for txn in update_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]
                    
                    # Get transaction details and lock state
                    primary_node = txn['node']
                    account_id = txn['account_id']
                    query = txn['query']
                    isolation_level = txn['isolation_level']
                    trans_id = txn['trans_id']
                    lock_acquired = txn.get('lock_acquired', False)
                    resource_id = txn.get('resource_id', f"trans_{trans_id}")
                    
                    try:
                        # Lock already held from UPDATE phase - just commit and replicate (2PL growing phase)
                        # Commit on primary node first
                        with st.spinner(f"Committing update on Node {primary_node}..."):
                            conn.commit()
                            cursor.close()
                            conn.close()
                        
                        st.info(f"Transaction updated on Node {primary_node}")
                        
                        # Get current node status for replication decisions
                        current_node_status = st.session_state.node_pinger.get_status()
                        partition_node_for_account = get_node_for_account(account_id)
                        
                        # Determine replication targets based on primary node
                        replication_results = []
                        
                        if primary_node == 1:
                            # Primary is Node 1: replicate to partition node
                            target_node = partition_node_for_account
                            if target_node != 1:
                                with st.spinner(f"Replicating to Node {target_node} (partition node)..."):
                                    result = replicate_transaction(query, primary_node, target_node, isolation_level)
                                    replication_results.append((target_node, result))
                                
                        else:
                            # Primary is Node 2/3: replicate to Node 1 (and potentially other nodes)
                            # Always try to replicate to Node 1 (central)
                            with st.spinner(f"Replicating to Node 1 (central)..."):
                                result = replicate_transaction(query, primary_node, 1, isolation_level)
                                replication_results.append((1, result))
                            
                            # If primary is not the natural partition node, also replicate to partition node
                            if primary_node != partition_node_for_account and partition_node_for_account != 1:
                                with st.spinner(f"Replicating to Node {partition_node_for_account} (partition node)..."):
                                    result = replicate_transaction(query, primary_node, partition_node_for_account, isolation_level)
                                    replication_results.append((partition_node_for_account, result))
                        
                        # Display replication results
                        successful_replications = 0
                        failed_replications = 0
                        
                        for target_node, result in replication_results:
                            if result['status'] == 'error':
                                st.error(f"Replication to Node {target_node} failed: {result['message']}")
                                if result['logged']:
                                    st.warning(f"Recovery logged: {result['recovery_action']}")
                                else:
                                    st.error(f"Recovery logging failed: {result['recovery_action']}")
                                failed_replications += 1
                            else:
                                st.success(f"Successfully replicated to Node {target_node}")
                                successful_replications += 1
                        
                        # Show replication summary
                        if replication_results:
                            if failed_replications > 0:
                                st.info(f"Replication Summary: {successful_replications} successful, {failed_replications} failed (logged for recovery)")
                            else:
                                st.success(f"All replications successful ({successful_replications}/{len(replication_results)})")
                        
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
                        
                    except Exception as commit_error:
                        st.error(f"Update commit failed on Node {primary_node}: {str(commit_error)}")
                        try:
                            conn.rollback()
                            cursor.close()
                            conn.close()
                        except:
                            pass
                    finally:
                        # 2PL SHRINKING PHASE: Release lock after commit and replication complete
                        if lock_acquired:
                            st.session_state.lock_manager.release_multi_node_lock(resource_id, nodes=[1, 2, 3])
                            st.info("🔓 Lock released (2PL shrinking phase)")

                # Remove processed transactions
                for idx in sorted(indices_to_remove, reverse=True):
                    del st.session_state.active_transactions[idx]
                    del st.session_state.transaction_connections[idx]
                    del st.session_state.transaction_cursors[idx]

                if committed_count > 0:
                    st.success(f"{committed_count} update transaction(s) committed successfully!")
                    st.toast(f"{committed_count} transaction(s) committed successfully")
                    
            except Exception as e:
                st.error(f"Update commit process failed: {str(e)}")
        else:
            st.warning("No active UPDATE transaction to commit")

    if rollback_button:
        update_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'update']
        if update_transactions:
            try:
                rolled_back_count = 0
                indices_to_remove = []

                # Collect indices and rollback transactions
                for txn in update_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    
                    # Release lock on rollback (2PL abort - release all locks)
                    if txn.get('lock_acquired', False):
                        resource_id = txn.get('resource_id', f"trans_{txn.get('trans_id')}")
                        st.session_state.lock_manager.release_multi_node_lock(resource_id, nodes=[1, 2, 3])
                    
                    rolled_back_count += 1

                # Remove in reverse order to maintain correct indices
                for idx in sorted(indices_to_remove, reverse=True):
                    del st.session_state.active_transactions[idx]
                    del st.session_state.transaction_connections[idx]
                    del st.session_state.transaction_cursors[idx]

                st.info(f"{rolled_back_count} update transaction(s) rolled back - no changes made or logged")
                st.toast(f"{rolled_back_count} transaction(s) rolled back")
            except Exception as e:
                st.error(f"Rollback failed: {str(e)}")
        else:
            st.warning("No active UPDATE transaction to rollback")

    if update_button:
        start_time = time.time()
        lock_acquired = False
        resource_id = f"trans_{trans_id}"  # Lock specific to this transaction

        try:
            # Check node status using server pinger
            node_status = st.session_state.node_pinger.get_status()
            
            # Acquire distributed lock across all available nodes before updating
            with st.spinner(f"Acquiring distributed lock on transaction {trans_id}..."):
                lock_acquired = st.session_state.lock_manager.acquire_multi_node_lock(
                    resource_id, nodes=[1, 2, 3], timeout=30
                )

                if not lock_acquired:
                    st.error(f"Failed to acquire lock on transaction {trans_id}. Another user may be modifying it. Please try again.")
                    st.stop()

            # Search for transaction with Node 1 priority, fallback to other nodes
            found_data = None
            account_id = None
            search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
            
            with st.spinner(f"Searching for transaction {trans_id}..."):
                # Try Node 1 first
                if node_status.get(1, False):
                    try:
                        found_data = fetch_data(search_query, node=1)
                        if not found_data.empty:
                            st.info("Transaction found on Node 1 (central)")
                            account_id = int(found_data.iloc[0]['account_id'])
                    except Exception as e:
                        st.warning(f"Could not search Node 1: {str(e)}")
                
                # If not found on Node 1 or Node 1 is offline, search other nodes
                if found_data is None or found_data.empty:
                    for node in [2, 3]:
                        if node_status.get(node, False):
                            try:
                                found_data = fetch_data(search_query, node=node)
                                if not found_data.empty:
                                    st.info(f"Transaction found on Node {node}")
                                    account_id = int(found_data.iloc[0]['account_id'])
                                    break
                            except Exception as e:
                                st.warning(f"Could not search Node {node}: {str(e)}")
                
                if found_data is None or found_data.empty:
                    st.error(f"Transaction ID {trans_id} not found on any available node")
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
                    st.error("All nodes are offline. Cannot proceed with update.")
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

            # Build UPDATE query
            update_query = f"""
            UPDATE trans 
            SET amount = {new_amount}, 
                type = '{new_type}', 
                operation = '{new_operation}'
            WHERE trans_id = {trans_id}
            """

            with st.spinner(f"Preparing update transaction on Node {primary_node}..."):
                # Create dedicated connection to primary node only
                conn = create_dedicated_connection(primary_node, isolation_level)
                cursor = conn.cursor(dictionary=True)

                # Set isolation level and start transaction
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                cursor.execute("START TRANSACTION")

                # Execute update but don't commit yet
                cursor.execute(update_query)

                # Store single transaction for commit/rollback
                st.session_state.transaction_connections.append(conn)
                st.session_state.transaction_cursors.append(cursor)
                st.session_state.active_transactions.append({
                    'page': 'update',
                    'node': primary_node,
                    'operation': 'UPDATE',
                    'trans_id': trans_id,
                    'account_id': account_id,
                    'query': update_query,
                    'isolation_level': isolation_level,
                    'start_time': start_time,
                    'lock_acquired': lock_acquired,  # Track lock state for 2PL
                    'resource_id': resource_id  # Store resource_id for lock release
                })

            duration = time.time() - start_time

            st.success(f"Update transaction prepared on Node {primary_node} in {duration:.3f}s")
            st.warning("Transaction active - Click 'Commit' to finalize update or 'Rollback' to cancel")

            # Show preview of change
            with st.expander("Pending Update"):
                st.write("**Before:**")
                st.dataframe(found_data)
                st.write("**After (pending commit):**")
                updated_preview = found_data.copy()
                updated_preview['amount'] = new_amount
                updated_preview['type'] = new_type
                updated_preview['operation'] = new_operation
                st.dataframe(updated_preview)
                
                if primary_node == 1:
                    st.caption(f"Update prepared on Node 1 (central) - will replicate to Node {partition_node} on commit")
                elif primary_node == partition_node:
                    st.caption(f"Update prepared on Node {primary_node} (natural partition for {'even' if partition_node == 2 else 'odd'} accounts) - will replicate to Node 1 (central) on commit")
                else:
                    st.caption(f"Update prepared on Node {primary_node} (emergency primary) - will replicate to Node 1 (central) and Node {partition_node} (natural partition) on commit")

        except Exception as e:
            # Don't log failed transactions - only log successful commits
            st.error(f"Error: {str(e)}")
            # On error, release lock immediately since transaction won't proceed
            if lock_acquired:
                st.session_state.lock_manager.release_multi_node_lock(resource_id, nodes=[1, 2, 3])
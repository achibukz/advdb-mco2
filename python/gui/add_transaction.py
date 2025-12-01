"""
Add Transaction Module - Old Logic
This module contains the original logic for adding transactions.
"""

import streamlit as st
import pandas as pd
import time
import sys
import os

# Add parent directory to path for imports (fixes Streamlit Cloud deployment)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from python.db.db_config import create_dedicated_connection, get_max_trans_id_multi_node


def render(get_node_for_account, log_transaction):
    """
    Render the Add Transaction page with the old logic.

    Args:
        get_node_for_account: Function to determine which node to use based on account_id
        log_transaction: Function to log transactions
    """
    st.title("Add New Transaction (Write Operation)")

    st.markdown("""
    Insert a new transaction record. The system queries all available nodes to find the 
    highest trans_id and automatically routes the insert to the appropriate node.
    """)

    # Configuration
    isolation_level = st.selectbox(
        "Isolation Level",
        ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
        index=1,
        key='insert_isolation',
        help="Controls transaction isolation level"
    )

    # Transaction form
    st.subheader("Transaction Details")

    col1, col2 = st.columns(2)

    with col1:
        account_id = st.number_input("Account ID", min_value=1, value=1)
        trans_date = st.date_input("Transaction Date")
        trans_type = st.selectbox("Type", ["Credit", "Debit"])

    with col2:
        operation = st.text_input("Operation", placeholder="e.g., Credit in Cash")
        amount = st.number_input("Amount", min_value=0.0, value=1000.0, step=100.0)
        k_symbol = st.text_input("K Symbol", value="")

    # Show next trans_id that will be used
    st.info("ℹ️ The next available trans_id will be automatically fetched and assigned")

    # Insert button with custom styling
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
        insert_button = st.button("💾 Insert Transaction", type="primary", use_container_width=True)
    with btn_col2:
        commit_button = st.button("✅ Commit Transaction", type="secondary", use_container_width=True, key="commit_insert")
    with btn_col3:
        rollback_button = st.button("↩️ Rollback", type="secondary", use_container_width=True, key="rollback_insert")

    if commit_button:
        from python.utils.recovery_manager import replicate_transaction
        
        add_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'add']
        if add_transactions:
            try:
                committed_count = 0
                indices_to_remove = []
                processed_trans_ids = set()  # Track which trans_ids have been processed

                # Process transactions one by one
                for txn in add_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]
                    
                    # Get transaction details
                    primary_node = txn['node']
                    account_id = txn['account_id']
                    trans_id = txn['trans_id']
                    query = txn['query']
                    isolation_level = txn['isolation_level']
                    
                    # Only commit for the first transaction with this trans_id
                    if trans_id not in processed_trans_ids:
                        # Get lock state from transaction (already acquired during INSERT button - 2PL growing phase)
                        lock_acquired = txn.get('lock_acquired', False)
                        resource_id = txn.get('resource_id', 'insert_trans')
                        
                        retry_count = 0
                        max_retries = 3
                        commit_successful = False
                        
                        while retry_count < max_retries and not commit_successful:
                            try:
                                # Lock already held from INSERT phase - just commit and replicate
                                # Commit on primary node first
                                with st.spinner(f"Committing on Node {primary_node}..."):
                                    conn.commit()
                                    commit_successful = True
                                
                                st.info(f"Transaction committed on Node {primary_node}")
                                
                            except Exception as commit_error:
                                error_str = str(commit_error)
                                
                                # Check if it's a duplicate key error (1062)
                                if "1062" in error_str and "Duplicate entry" in error_str:
                                    retry_count += 1
                                    
                                    if retry_count < max_retries:
                                        st.warning(f"⚠️ Duplicate trans_id detected. Retrying with new ID... (Attempt {retry_count}/{max_retries})")
                                        
                                        try:
                                            # Rollback current transaction
                                            conn.rollback()
                                            
                                            # Re-read max_trans_id from all nodes
                                            max_result = get_max_trans_id_multi_node()
                                            
                                            if max_result['status'] == 'failed':
                                                st.error(f"Cannot retry: {max_result['error']}")
                                                raise commit_error
                                            
                                            # Get new trans_id
                                            new_trans_id = max_result['max_trans_id'] + 1
                                            st.info(f"🔄 Retrying with new trans_id: {new_trans_id} (was {trans_id})")
                                            
                                            # Rebuild INSERT query with new trans_id
                                            new_query = query.replace(f"VALUES ({trans_id},", f"VALUES ({new_trans_id},")
                                            
                                            # Re-execute INSERT with new ID
                                            cursor = conn.cursor(dictionary=True)
                                            cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                                            cursor.execute("START TRANSACTION")
                                            cursor.execute(new_query)
                                            
                                            # Update transaction metadata
                                            txn['trans_id'] = new_trans_id
                                            txn['query'] = new_query
                                            trans_id = new_trans_id
                                            query = new_query
                                            
                                            # Update cursor reference
                                            st.session_state.transaction_cursors[idx] = cursor
                                            
                                        except Exception as retry_error:
                                            st.error(f"Retry failed: {str(retry_error)}")
                                            raise commit_error
                                    else:
                                        st.error(f"❌ Max retries ({max_retries}) reached. Transaction failed.")
                                        raise commit_error
                                else:
                                    # Not a duplicate key error - raise immediately
                                    raise commit_error
                        
                        if not commit_successful:
                            st.error(f"❌ Transaction could not be committed after {max_retries} attempts")
                            # Release lock and skip to next transaction
                            if lock_acquired:
                                st.session_state.lock_manager.release_multi_node_lock(resource_id, nodes=[1, 2, 3])
                            continue
                        
                        try:
                            # Get current node status for replication decisions
                            current_node_status = st.session_state.node_pinger.get_status()
                            partition_node_for_account = get_node_for_account(account_id)
                            
                            # Determine replication targets based on primary node
                            replication_results = []
                            
                            if primary_node == 1:
                                # If primary is Node 1, replicate to partition node
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
                            processed_trans_ids.add(trans_id)
                            
                        except Exception as replication_error:
                            st.error(f"❌ Replication failed: {str(replication_error)}")
                            try:
                                conn.rollback()
                                cursor.close()
                                conn.close()
                            except:
                                pass
                        finally:
                            # Close cursor and connection
                            try:
                                cursor.close()
                                conn.close()
                            except:
                                pass
                            
                            # 2PL SHRINKING PHASE: Release lock after commit and replication complete
                            if lock_acquired:
                                st.session_state.lock_manager.release_multi_node_lock(resource_id, nodes=[1, 2, 3])
                                st.info("🔓 Lock released (2PL shrinking phase)")
                    else:
                        # This is a replica transaction (same trans_id already processed)
                        # Just commit without acquiring lock (lock already held by primary)
                        try:
                            conn.commit()
                            cursor.close()
                            conn.close()
                        except Exception as e:
                            st.error(f"Replica commit failed: {str(e)}")
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
                    st.success(f"{committed_count} transaction(s) committed successfully!")
                    st.toast(f"{committed_count} transaction(s) committed successfully")
                    
            except Exception as e:
                st.error(f"Commit process failed: {str(e)}")
        else:
            st.warning("No active INSERT transaction to commit")

    if rollback_button:
        add_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'add']
        if add_transactions:
            try:
                rolled_back_count = 0
                indices_to_remove = []

                # Collect indices and rollback transactions
                for txn in add_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    
                    # Release lock on rollback (2PL abort - release all locks)
                    if txn.get('lock_acquired', False):
                        resource_id = txn.get('resource_id', 'insert_trans')
                        st.session_state.lock_manager.release_multi_node_lock(resource_id, nodes=[1, 2, 3])
                    
                    rolled_back_count += 1

                # Remove in reverse order to maintain correct indices
                for idx in sorted(indices_to_remove, reverse=True):
                    del st.session_state.active_transactions[idx]
                    del st.session_state.transaction_connections[idx]
                    del st.session_state.transaction_cursors[idx]

                st.info(f"↩️ {rolled_back_count} insert transaction(s) rolled back - no changes made or logged")
                st.toast(f"{rolled_back_count} transaction(s) rolled back")
            except Exception as e:
                st.error(f"Rollback failed: {str(e)}")
        else:
            st.warning("No active INSERT transaction to rollback")

    if insert_button:
        start_time = time.time()
        lock_acquired = False
        resource_id = "insert_trans"  # Global lock for insert operations

        try:
            # Step 1: Execute global recovery with checkpoints
            with st.spinner("Processing pending recovery logs..."):
                from python.utils.recovery_manager import execute_global_recovery
                recovery_result = execute_global_recovery()
                
                if recovery_result.get('lock_acquired', False):
                    if recovery_result['total_logs'] > 0:
                        if recovery_result['recovered'] > 0:
                            st.success(f"Processed {recovery_result['recovered']} recovery logs successfully")
                        if recovery_result['failed'] > 0:
                            st.warning(f"{recovery_result['failed']} recovery logs failed - check system logs")
                        elif recovery_result['recovered'] == 0:
                            st.info("No recovery logs needed processing")
                    else:
                        st.info("No new recovery logs to process")
                else:
                    st.info("Recovery already running by another process")
            
            # Step 2: Acquire distributed lock BEFORE querying max_trans_id (prevents race condition)
            with st.spinner(f"Acquiring distributed lock across all nodes..."):
                lock_acquired = st.session_state.lock_manager.acquire_multi_node_lock(
                    resource_id, nodes=[1, 2, 3], timeout=30
                )

                if not lock_acquired:
                    st.error("Failed to acquire lock. Another user may be inserting. Please try again.")
                    st.stop()
                
                st.info(f"✅ Lock acquired successfully by session {st.session_state.lock_manager.current_node_id}")
            
            # Step 3: Check node status using server pinger
            node_status = st.session_state.node_pinger.get_status()
            
            # Determine primary node with robust fallback logic
            partition_node = get_node_for_account(account_id)
            
            # Priority: Node 1 > Partition Node > Any Available Node
            if node_status.get(1, False):  # Node 1 is online (highest priority)
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
                    st.error("❌ All nodes are offline. Cannot proceed with transaction.")
                    st.stop()
                else:
                    st.error(f"🚨 Both Node 1 and Node {partition_node} are offline - Using Node {primary_node} as emergency primary")
            
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

            # Step 4: Query max_trans_id AFTER acquiring lock (prevents concurrent ID collision)
            with st.spinner(f"Checking available nodes for highest trans_id..."):
                # Query all available nodes for MAX(trans_id) and get the highest value
                max_result = get_max_trans_id_multi_node()

                if max_result['status'] == 'failed':
                    st.error(f"Cannot proceed: {max_result['error']}")
                    st.warning("All servers are down. Please check node availability and try again.")
                    st.stop()

                # Get the next trans_id from the highest value across all nodes
                next_trans_id = max_result['max_trans_id'] + 1

                # Show which nodes were queried
                with st.expander("Multi-Node Query Results"):
                    st.info(f"Queried {len(max_result['available_nodes'])} available node(s)")
                    node_data = []
                    for node, value in max_result['node_values'].items():
                        node_data.append({
                            'Node': f"Node {node}",
                            'MAX(trans_id)': value if value is not None else 'N/A'
                        })
                    st.dataframe(pd.DataFrame(node_data))
                    st.success(f"Selected highest trans_id: {max_result['max_trans_id']} → Next: {next_trans_id}")

            with st.spinner(f"Preparing insert transaction on Node {primary_node}..."):
                # Build INSERT query with trans_id
                insert_query = f"""
                INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, k_symbol)
                VALUES ({next_trans_id}, {account_id}, '{trans_date}', '{trans_type}', '{operation}', {amount}, '{k_symbol}')
                """

                # Create dedicated connection to primary node only
                conn = create_dedicated_connection(primary_node, isolation_level)
                cursor = conn.cursor(dictionary=True)

                # Set isolation level and start transaction
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                cursor.execute("START TRANSACTION")

                # Execute insert but don't commit yet
                cursor.execute(insert_query)

                # Store single transaction for commit/rollback
                st.session_state.transaction_connections.append(conn)
                st.session_state.transaction_cursors.append(cursor)
                st.session_state.active_transactions.append({
                    'page': 'add',
                    'node': primary_node,
                    'operation': 'INSERT',
                    'query': insert_query,
                    'isolation_level': isolation_level,
                    'start_time': start_time,
                    'trans_id': next_trans_id,
                    'account_id': account_id,
                    'lock_acquired': lock_acquired,  # Track lock state for 2PL
                    'resource_id': resource_id  # Store resource_id for lock release
                })

            duration = time.time() - start_time

            st.success(f"Insert transaction prepared with trans_id={next_trans_id} on Node {primary_node} in {duration:.3f}s")
            st.warning("Transaction active - Click 'Commit' to finalize insertion or 'Rollback' to cancel")

            # Show preview
            with st.expander("Pending Insert"):
                preview_data = pd.DataFrame([{
                    'trans_id': next_trans_id,
                    'account_id': account_id,
                    'newdate': trans_date,
                    'type': trans_type,
                    'operation': operation,
                    'amount': amount,
                    'k_symbol': k_symbol
                }])
                st.dataframe(preview_data)
                
                # Show replication strategy based on primary node
                if primary_node == 1:
                    st.caption(f"Insert prepared on Node 1 (central) - will replicate to Node {partition_node} ({'even' if partition_node == 2 else 'odd'} accounts) on commit")
                elif primary_node == partition_node:
                    st.caption(f"Insert prepared on Node {primary_node} (natural partition for {'even' if partition_node == 2 else 'odd'} accounts) - will replicate to Node 1 (central) on commit")
                else:
                    st.caption(f"Insert prepared on Node {primary_node} (emergency primary) - will replicate to Node 1 (central) and Node {partition_node} (natural partition) on commit")

        except Exception as e:
            # Don't log failed transactions - only log successful commits
            st.error(f"❌ Error: {str(e)}")
            # On error, release lock immediately since transaction won't proceed
            if lock_acquired:
                st.session_state.lock_manager.release_multi_node_lock(resource_id, nodes=[1, 2, 3])
"""
Add Transaction Module - Old Logic
This module contains the original logic for adding transactions.
"""

import streamlit as st
import pandas as pd
import time
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
        add_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'add']
        if add_transactions:
            try:
                committed_count = 0
                indices_to_remove = []

                # Track which trans_ids we've already processed to avoid duplicate locking
                processed_trans_ids = set()

                # Collect indices and commit transactions
                for txn in add_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]

                    # Re-acquire lock for replication phase (only once per trans_id)
                    trans_id = txn.get('trans_id')
                    resource_id = "insert_trans"
                    lock_acquired = False

                    # Only acquire lock once per unique transaction
                    if trans_id not in processed_trans_ids:
                        try:
                            lock_acquired = st.session_state.lock_manager.acquire_lock(
                                resource_id, node=1, timeout=30
                            )

                            if not lock_acquired:
                                st.error(f"Failed to acquire replication lock for trans_id={trans_id}")
                                conn.rollback()
                                cursor.close()
                                conn.close()
                                continue

                            processed_trans_ids.add(trans_id)

                            # Commit the transaction on the target node (while holding lock)
                            conn.commit()
                            cursor.close()
                            conn.close()

                            # Log the transaction
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

                        finally:
                            # Release lock AFTER replication completes
                            if lock_acquired:
                                st.session_state.lock_manager.release_lock(resource_id, node=1)
                    else:
                        # This is a replica of an already-processed transaction
                        conn.commit()
                        cursor.close()
                        conn.close()
                        committed_count += 1

                # Remove in reverse order to maintain correct indices
                for idx in sorted(indices_to_remove, reverse=True):
                    del st.session_state.active_transactions[idx]
                    del st.session_state.transaction_connections[idx]
                    del st.session_state.transaction_cursors[idx]

                st.success(f"✅ {committed_count} transaction(s) committed successfully!")
                st.toast(f"{committed_count} transaction(s) committed successfully")
            except Exception as e:
                st.error(f"Commit failed: {str(e)}")
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
        # Determine target node based on account_id
        target_node = get_node_for_account(account_id)

        start_time = time.time()
        lock_acquired = False
        resource_id = "insert_trans"  # Global lock for insert operations

        try:
            # Acquire distributed lock before inserting
            with st.spinner(f"Acquiring distributed lock..."):
                lock_acquired = st.session_state.lock_manager.acquire_lock(
                    resource_id, node=1, timeout=30
                )

                if not lock_acquired:
                    st.error("Failed to acquire lock. Another user may be inserting. Please try again.")
                    st.stop()

            with st.spinner(f"Checking all nodes for highest trans_id..."):
                # Query all available nodes for MAX(trans_id) and get the highest value
                max_result = get_max_trans_id_multi_node()

                if max_result['status'] == 'failed':
                    st.error(f"❌ Cannot proceed: {max_result['error']}")
                    st.warning("⚠️ Servers are down. Please check node availability and try again.")

                    # Show node status
                    with st.expander("📊 Node Status"):
                        status_data = []
                        for node in [1, 2, 3]:
                            is_available = node in max_result['available_nodes']
                            status_data.append({
                                'Node': f"Node {node}",
                                'Status': '✅ Available' if is_available else '❌ Down'
                            })
                        st.dataframe(pd.DataFrame(status_data))
                    st.stop()

                # Get the next trans_id from the highest value across all nodes
                next_trans_id = max_result['max_trans_id'] + 1

                # Show which nodes were queried
                with st.expander("📊 Multi-Node Query Results"):
                    st.info(f"Queried {len(max_result['available_nodes'])} available node(s)")
                    node_data = []
                    for node, value in max_result['node_values'].items():
                        node_data.append({
                            'Node': f"Node {node}",
                            'MAX(trans_id)': value if value is not None else 'N/A'
                        })
                    st.dataframe(pd.DataFrame(node_data))
                    st.success(f"Selected highest trans_id: {max_result['max_trans_id']} → Next: {next_trans_id}")

            with st.spinner(f"Preparing insert transaction..."):
                # Create dedicated connection and start transaction
                conn = create_dedicated_connection(target_node, isolation_level)
                cursor = conn.cursor(dictionary=True)

                # Set isolation level and start transaction
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                cursor.execute("START TRANSACTION")

                # Build INSERT query with trans_id (already determined from multi-node query)
                insert_query = f"""
                INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, k_symbol)
                VALUES ({next_trans_id}, {account_id}, '{trans_date}', '{trans_type}', '{operation}', {amount}, '{k_symbol}')
                """

                # Prepare transactions on Node 1 (central) and target partition node (2 or 3 based on account_id)
                partition_node = get_node_for_account(account_id)
                nodes_to_write = [1, partition_node]

                for node in nodes_to_write:
                    conn_temp = create_dedicated_connection(node, isolation_level)
                    cursor_temp = conn_temp.cursor(dictionary=True)

                    # Set isolation level and start transaction
                    cursor_temp.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                    cursor_temp.execute("START TRANSACTION")

                    # Execute insert but don't commit yet
                    cursor_temp.execute(insert_query)

                    # Append connection and transaction to lists
                    st.session_state.transaction_connections.append(conn_temp)
                    st.session_state.transaction_cursors.append(cursor_temp)
                    st.session_state.active_transactions.append({
                        'page': 'add',
                        'node': node,
                        'operation': 'INSERT',
                        'query': insert_query,
                        'isolation_level': isolation_level,
                        'start_time': start_time,
                        'trans_id': next_trans_id,
                        'account_id': account_id
                    })

            duration = time.time() - start_time

            # DON'T log yet - will log when user commits

            st.success(f"✅ Insert transaction prepared with trans_id={next_trans_id} on Node 1 and Node {partition_node} in {duration:.3f}s")
            st.warning("⏳ Transaction active - Click 'Commit' to finalize insertion or 'Rollback' to cancel")

            # Show preview
            with st.expander("📝 Pending Insert"):
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
                st.caption(f"Insert prepared on Node 1 (central) and Node {partition_node} ({'even' if partition_node == 2 else 'odd'} accounts) - not yet committed")

        except Exception as e:
            # Don't log failed transactions - only log successful commits
            st.error(f"❌ Error: {str(e)}")

        finally:
            # Always release the lock
            if lock_acquired:
                st.session_state.lock_manager.release_lock(resource_id, node=1)
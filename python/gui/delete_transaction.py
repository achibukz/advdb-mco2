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
        st.success(f"✅ Transaction {st.session_state.last_deleted_id} was successfully deleted!")
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
    if st.button("🔍 Preview Transaction"):
        try:
            # Search for transaction on Node 1 (central node)
            search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
            found_data = fetch_data(search_query, node=1)

            if found_data.empty:
                st.error(f"❌ Transaction ID {trans_id} not found in the database")
                st.info("This transaction may have already been deleted or never existed.")
            else:
                st.success(f"✅ Found transaction")
                st.dataframe(found_data)
                # Store that we found this transaction
                st.session_state.preview_trans_id = trans_id

        except Exception as e:
            st.error(f"❌ Error searching: {str(e)}")

    st.markdown("---")
    st.warning("⚠️ This action cannot be undone!")

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
        delete_button = st.button("🗑️ Delete Transaction", type="primary", use_container_width=True)
    with btn_col2:
        commit_button = st.button("✅ Commit Transaction", type="secondary", use_container_width=True, key="commit_delete")
    with btn_col3:
        rollback_button = st.button("↩️ Rollback", type="secondary", use_container_width=True, key="rollback_delete")

    if commit_button:
        delete_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'delete']
        if delete_transactions:
            try:
                committed_count = 0
                indices_to_remove = []

                # Track which trans_ids we've already processed to avoid duplicate locking
                processed_trans_ids = set()

                # Collect indices and commit transactions
                for txn in delete_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]

                    # Re-acquire lock for replication phase (only once per trans_id)
                    trans_id = txn.get('trans_id')
                    resource_id = f"trans_{trans_id}"
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

                            # Commit the transaction (while holding lock)
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

                st.success(f"✅ {committed_count} delete transaction(s) committed successfully!")
                st.toast(f"{committed_count} transaction(s) committed successfully")
            except Exception as e:
                st.error(f"Commit failed: {str(e)}")
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

                st.info(f"↩️ {rolled_back_count} delete transaction(s) rolled back - data not deleted, no changes logged")
                st.toast(f"{rolled_back_count} transaction(s) rolled back")
            except Exception as e:
                st.error(f"Rollback failed: {str(e)}")
        else:
            st.warning("No active DELETE transaction to rollback")

    if delete_button:
        start_time = time.time()
        delete_query = None
        lock_acquired = False
        resource_id = f"trans_{trans_id}"  # Lock specific to this transaction

        try:
            # Acquire distributed lock before deleting
            with st.spinner(f"Acquiring distributed lock on transaction {trans_id}..."):
                lock_acquired = st.session_state.lock_manager.acquire_lock(
                    resource_id, node=1, timeout=30
                )

                if not lock_acquired:
                    st.error(f"Failed to acquire lock on transaction {trans_id}. Another user may be modifying it. Please try again.")
                    st.stop()

            with st.spinner(f"Verifying transaction exists..."):
                # First verify the transaction exists and get account_id
                search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
                found_data = fetch_data(search_query, node=1)

                if found_data.empty:
                    st.error(f"❌ Transaction ID {trans_id} not found in the database")
                    st.warning("This transaction may have already been deleted or never existed.")
                    st.info("💡 Use the Preview button to check if a transaction exists before deleting.")
                    st.stop()  # Stop execution here
                else:
                    # Get account_id to determine target partition node
                    account_id = int(found_data.iloc[0]['account_id'])

                    # Build DELETE query
                    delete_query = f"DELETE FROM trans WHERE trans_id = {trans_id}"

                    # Determine partition node based on account_id
                    partition_node = get_node_for_account(account_id)

                    with st.spinner(f"Starting transaction on Node 1 and Node {partition_node}..."):
                        # Prepare transactions on Node 1 (central) and target partition node (2 or 3 based on account_id)
                        nodes_to_write = [1, partition_node]

                        for node in nodes_to_write:
                            conn_temp = create_dedicated_connection(node, isolation_level)
                            cursor_temp = conn_temp.cursor(dictionary=True)

                            # Set isolation level and start transaction
                            cursor_temp.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                            cursor_temp.execute("START TRANSACTION")

                            # Execute delete but don't commit yet
                            cursor_temp.execute(delete_query)

                            # Append connection and transaction to lists
                            st.session_state.transaction_connections.append(conn_temp)
                            st.session_state.transaction_cursors.append(cursor_temp)
                            st.session_state.active_transactions.append({
                                'page': 'delete',
                                'node': node,
                                'operation': 'DELETE',
                                'trans_id': trans_id,
                                'account_id': account_id,
                                'query': delete_query,
                                'isolation_level': isolation_level,
                                'start_time': start_time
                            })

                    duration = time.time() - start_time

                    # DON'T log yet - will log when user commits

                    st.success(f"✅ Delete transaction prepared on Node 1 and Node {partition_node} in {duration:.3f}s")
                    st.warning("⏳ Transaction active - Click 'Commit' to finalize deletion or 'Rollback' to cancel")

                    # Show confirmation
                    with st.expander("📝 Pending Deletion"):
                        st.write(f"Transaction ID {trans_id} is marked for deletion")
                        st.dataframe(found_data)
                        st.caption(f"Delete prepared on Node 1 (central) and Node {partition_node} ({'even' if partition_node == 2 else 'odd'} accounts) - not yet committed")

        except Exception as e:
            # Don't log failed transactions - only log successful commits
            st.error(f"❌ Error: {str(e)}")

        finally:
            # Always release the lock
            if lock_acquired:
                st.session_state.lock_manager.release_lock(resource_id, node=1)
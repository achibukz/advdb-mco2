"""
STADVDB MCO2: Distributed Database Transaction Manager
Users perform CRUD operations; System logs concurrent transactions automatically
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
import json

from python.db.db_config import fetch_data, execute_query

st.set_page_config(
    page_title="Transaction Manager",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for transaction tracking
if 'transaction_log' not in st.session_state:
    st.session_state.transaction_log = []

# Sidebar
st.sidebar.title("Database Operations")
st.sidebar.text("STADVDB MCO2 Group 12")
st.sidebar.text("Balcita, Bukuhan, Cu, Dimaunahan")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Select Operation",
    [
        "Home",
        "View Transactions",
        "Add Transaction",
        "Update Transaction",
        "Delete Transaction",
        "Transaction Log",
        "Test Case #1"
    ]
)

# Helper function to determine node based on account_id
def get_node_for_account(account_id):
    """
    Determine which node to use based on account_id
    Node 1: Central (all data)
    Node 2: Even account_id
    Node 3: Odd account_id
    """
    if account_id % 2 == 0:
        return 2
    else:
        return 3

# Helper function to log transactions
def log_transaction(operation, query, node, isolation_level, status, duration):
    """Log transaction for later analysis"""
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'operation': operation,
        'query': query,
        'node': node,
        'isolation_level': isolation_level,
        'status': status,
        'duration': duration,
        'user_session': st.session_state.get('user_id', 'anonymous')
    }
    
    st.session_state.transaction_log.append(log_entry)
    
    # Also save to file for persistence
    with open('transaction_log.json', 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

def main():
    # ============================================================================
    # HOME PAGE
    # ============================================================================
    if page == "Home":
        st.title("Distributed Database Transaction Manager")

        st.markdown("""
        ## Welcome to the Transaction Manager
        
        INSERT INTRO HERE
                    
        """)

        # Show node status
        st.markdown("---")
        st.subheader("📊 Current Node Status")

        col1, col2, col3 = st.columns(3)

        try:
            node1_count = fetch_data("SELECT COUNT(*) as count FROM trans", node=1)['count'][0]
            node2_count = fetch_data("SELECT COUNT(*) as count FROM trans", node=2)['count'][0]
            node3_count = fetch_data("SELECT COUNT(*) as count FROM trans", node=3)['count'][0]

            with col1:
                st.metric("Node 1 (Central)", "✅ Active", f"{node1_count:,} rows")
            with col2:
                st.metric("Node 2 (Even Accounts)", "✅ Active", f"{node2_count:,} rows")
            with col3:
                st.metric("Node 3 (Odd Accounts)", "✅ Active", f"{node3_count:,} rows")
        except Exception as e:
            st.error(f"⚠️ Database connection issue: {str(e)}")

    # ============================================================================
    # VIEW TRANSACTIONS (READ OPERATION)
    # ============================================================================
    elif page == "View Transactions":
        st.title("View Transactions (Read Operation)")

        st.markdown("""
        Browse transaction records from the database. The system will automatically 
        query the appropriate node based on your search criteria.
        """)

        # Configuration
        col1, col2 = st.columns(2)

        with col1:
            isolation_level = st.selectbox(
                "Isolation Level",
                ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
                index=1,
                help="Controls how the transaction sees concurrent changes"
            )

        with col2:
            limit = st.number_input("Number of rows", min_value=10, max_value=1000, value=50)

        # Filter options
        st.subheader("Filter Options")

        col1, col2, col3 = st.columns(3)

        with col1:
            account_id = st.text_input("Account ID", placeholder="Leave empty for all")

        with col2:
            trans_type = st.selectbox("Transaction Type",
                                      ["All", "Credit", "Debit"])

        with col3:
            date_range = st.date_input("Date Range", value=None)

        # Build query based on filters
        base_query = "SELECT * FROM trans WHERE 1=1"

        # Determine which node to query
        if account_id:
            base_query += f" AND account_id = {account_id}"
            # Query specific partition node
            selected_node = get_node_for_account(int(account_id))
        else:
            # Query central node for all data
            selected_node = 1

        if trans_type != "All":
            base_query += f" AND type = '{trans_type}'"

        base_query += f" LIMIT {limit}"

        # Execute button
        if st.button("🔍 Fetch Data", type="primary"):
            start_time = time.time()

            try:
                with st.spinner(f"Querying database..."):
                    # Execute query (node is automatically selected)
                    data = fetch_data(base_query, node=selected_node)

                duration = time.time() - start_time

                # Log this READ operation
                log_transaction(
                    operation='READ',
                    query=base_query,
                    node=selected_node,
                    isolation_level=isolation_level,
                    status='SUCCESS',
                    duration=duration
                )

                if data.empty:
                    st.warning("⚠️ No data found matching your criteria")
                else:
                    st.success(f"✅ Retrieved {len(data)} rows in {duration:.3f}s")
                    st.dataframe(data, use_container_width=True)

                    # Show transaction info
                    with st.expander("ℹ️ Transaction Details"):
                        st.write(f"**Isolation Level**: {isolation_level}")
                        st.write(f"**Duration**: {duration:.3f}s")
                        st.write(f"**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        st.caption(f"System selected Node {selected_node} for this query")

            except Exception as e:
                duration = time.time() - start_time

                log_transaction(
                    operation='READ',
                    query=base_query,
                    node=selected_node,
                    isolation_level=isolation_level,
                    status='FAILED',
                    duration=duration
                )

                st.error(f"❌ Error: {str(e)}")

    # ============================================================================
    # ADD TRANSACTION (INSERT/WRITE OPERATION)
    # ============================================================================
    elif page == "Add Transaction":
        st.title("Add New Transaction (Write Operation)")

        st.markdown("""
        Insert a new transaction record. The system will automatically route it to the 
        appropriate node and handle replication.
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

        # Insert button
        if st.button("💾 Insert Transaction", type="primary"):

            # Determine target node based on account_id
            target_node = get_node_for_account(account_id)

            # Build INSERT query
            insert_query = f"""
            INSERT INTO trans (account_id, newdate, type, operation, amount, k_symbol)
            VALUES ({account_id}, '{trans_date}', '{trans_type}', '{operation}', {amount}, '{k_symbol}')
            """

            start_time = time.time()

            try:
                with st.spinner(f"Inserting transaction..."):
                    # Execute insert on partition node
                    result = execute_query(insert_query, node=target_node,
                                         isolation_level=isolation_level)

                    # Simulate replication to other nodes
                    st.info("🔄 Replicating to other nodes...")
                    time.sleep(0.5)  # Simulate replication delay

                    # Replicate to Node 1 (central node)
                    if target_node != 1:
                        execute_query(insert_query, node=1, isolation_level=isolation_level)

                    # If insert was on Node 1, replicate to partition node
                    if target_node == 1:
                        replica_node = get_node_for_account(account_id)
                        if replica_node != 1:
                            execute_query(insert_query, node=replica_node, isolation_level=isolation_level)

                duration = time.time() - start_time

                # Log the WRITE operation
                log_transaction(
                    operation='INSERT',
                    query=insert_query,
                    node=target_node,
                    isolation_level=isolation_level,
                    status='SUCCESS',
                    duration=duration
                )

                st.success(f"✅ Transaction inserted successfully in {duration:.3f}s")
                st.success("✅ Replicated to other nodes")

                # Show inserted data
                with st.expander("📝 View Inserted Record"):
                    verify_query = f"SELECT * FROM trans WHERE account_id = {account_id} ORDER BY trans_id DESC LIMIT 1"
                    inserted_data = fetch_data(verify_query, node=target_node)
                    st.dataframe(inserted_data)
                    st.caption(f"Data inserted into Node {target_node} and replicated to central node")

            except Exception as e:
                duration = time.time() - start_time

                log_transaction(
                    operation='INSERT',
                    query=insert_query,
                    node=target_node,
                    isolation_level=isolation_level,
                    status='FAILED',
                    duration=duration
                )

                st.error(f"❌ Error: {str(e)}")

    # ============================================================================
    # UPDATE TRANSACTION (WRITE OPERATION)
    # ============================================================================
    elif page == "Update Transaction":
        st.title("Update Transaction (Write Operation)")

        st.markdown("""
        Modify an existing transaction record. The system will automatically find 
        and update the record across all nodes.
        """)

        # Step 1: Find the transaction to update
        st.subheader("Step 1: Find Transaction to Update")

        col1, col2 = st.columns(2)

        with col1:
            trans_id = st.number_input("Transaction ID", min_value=1, value=1)

        with col2:
            st.info("System will search across all nodes automatically")

        if st.button("🔍 Search"):
            try:
                # Try to find transaction (search central node first)
                search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
                found_data = fetch_data(search_query, node=1)

                if found_data.empty:
                    st.warning(f"⚠️ Transaction ID {trans_id} not found")
                else:
                    st.success(f"✅ Found transaction")
                    st.dataframe(found_data)

                    # Store in session state for update form
                    st.session_state.current_trans = found_data.iloc[0].to_dict()
                    # Determine which node this account belongs to
                    st.session_state.update_node = get_node_for_account(
                        int(st.session_state.current_trans['account_id'])
                    )

            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

        # Step 2: Update form (only show if transaction found)
        if 'current_trans' in st.session_state:
            st.markdown("---")
            st.subheader("Step 2: Update Values")

            current = st.session_state.current_trans

            col1, col2 = st.columns(2)

            with col1:
                new_amount = st.number_input("New Amount",
                                            value=float(current['amount']),
                                            step=100.0)
                new_type = st.selectbox("New Type",
                                       ["Credit", "Debit"],
                                       index=0 if current['type'] == 'Credit' else 1)

            with col2:
                new_operation = st.text_input("New Operation",
                                             value=current['operation'])
                isolation_level = st.selectbox(
                    "Isolation Level",
                    ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
                    index=1,
                    key='update_isolation'
                )

            if st.button("💾 Update Transaction", type="primary"):
                update_query = f"""
                UPDATE trans 
                SET amount = {new_amount}, 
                    type = '{new_type}', 
                    operation = '{new_operation}'
                WHERE trans_id = {trans_id}
                """

                start_time = time.time()
                update_node = st.session_state.update_node

                try:
                    with st.spinner(f"Updating transaction on Node {update_node}..."):
                        # Update on partition node
                        result = execute_query(update_query, node=update_node,
                                    isolation_level=isolation_level)

                    with st.spinner("🔄 Replicating update to central node..."):
                        time.sleep(0.3)

                        # Update central node if different
                        if update_node != 1:
                            execute_query(update_query, node=1, isolation_level=isolation_level)

                    duration = time.time() - start_time

                    log_transaction(
                        operation='UPDATE',
                        query=update_query,
                        node=update_node,
                        isolation_level=isolation_level,
                        status='SUCCESS',
                        duration=duration
                    )

                    st.success(f"✅ Transaction updated successfully in {duration:.3f}s")
                    st.success("✅ Replicated to other nodes")

                    # Clear session state
                    del st.session_state.current_trans
                    del st.session_state.update_node

                except Exception as e:
                    duration = time.time() - start_time

                    log_transaction(
                        operation='UPDATE',
                        query=update_query,
                        node=update_node,
                        isolation_level=isolation_level,
                        status='FAILED',
                        duration=duration
                    )

                    st.error(f"❌ Error: {str(e)}")

    # ============================================================================
    # DELETE TRANSACTION (WRITE OPERATION)
    # ============================================================================
    elif page == "Delete Transaction":
        st.title("Delete Transaction (Write Operation)")

        st.markdown("""
        Remove a transaction record from the database. The system will automatically 
        delete from all nodes.
        """)

        st.subheader("Find Transaction to Delete")

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

        if st.button("🔍 Find Transaction"):
            try:
                search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
                found_data = fetch_data(search_query, node=1)

                if found_data.empty:
                    st.warning(f"⚠️ Transaction ID {trans_id} not found")
                else:
                    st.success(f"✅ Found transaction")
                    st.dataframe(found_data)
                    st.session_state.delete_trans = found_data.iloc[0].to_dict()
                    st.session_state.delete_node = get_node_for_account(
                        int(st.session_state.delete_trans['account_id'])
                    )
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

        # Delete button (only show if transaction found)
        if 'delete_trans' in st.session_state:
            st.markdown("---")
            st.warning("⚠️ This action cannot be undone!")

            if st.button("🗑️ Delete Transaction", type="primary"):
                delete_query = f"DELETE FROM trans WHERE trans_id = {trans_id}"
                start_time = time.time()
                delete_node = st.session_state.delete_node

                try:
                    with st.spinner("Deleting transaction..."):
                        # Delete from partition node
                        execute_query(delete_query, node=delete_node, isolation_level=isolation_level)

                        st.info("🔄 Replicating deletion to other nodes...")
                        time.sleep(0.5)

                        # Delete from central node
                        if delete_node != 1:
                            execute_query(delete_query, node=1, isolation_level=isolation_level)

                    duration = time.time() - start_time

                    log_transaction(
                        operation='DELETE',
                        query=delete_query,
                        node=delete_node,
                        isolation_level=isolation_level,
                        status='SUCCESS',
                        duration=duration
                    )

                    st.success(f"✅ Transaction deleted successfully in {duration:.3f}s")
                    st.success("✅ Deletion replicated to all nodes")

                    # Clear session state
                    del st.session_state.delete_trans
                    del st.session_state.delete_node

                except Exception as e:
                    duration = time.time() - start_time

                    log_transaction(
                        operation='DELETE',
                        query=delete_query,
                        node=delete_node,
                        isolation_level=isolation_level,
                        status='FAILED',
                        duration=duration
                    )

                    st.error(f"❌ Error: {str(e)}")

    # ============================================================================
    # TRANSACTION LOG (ANALYSIS)
    # ============================================================================
    elif page == "Transaction Log":
        st.title("Transaction Log & Concurrency Analysis")

        st.markdown("""
        View all operations performed and analyze concurrent transactions.
        **This shows which test cases occurred naturally during usage.**
        """)

        if not st.session_state.transaction_log:
            st.info("ℹ️ No transactions logged yet. Perform some operations first!")
        else:
            # Display log
            log_df = pd.DataFrame(st.session_state.transaction_log)

            st.subheader("All Transactions")
            st.dataframe(log_df, use_container_width=True)

            # Analyze concurrency
            st.markdown("---")
            st.subheader("🔍 Concurrency Analysis")

            # Find concurrent operations
            log_df['timestamp'] = pd.to_datetime(log_df['timestamp'])
            log_df = log_df.sort_values('timestamp')

            # Detect overlapping transactions (within 5 seconds = concurrent)
            st.markdown("### Detected Concurrent Operations")

            concurrent_found = False
            for i in range(len(log_df) - 1):
                time_diff = (log_df.iloc[i+1]['timestamp'] - log_df.iloc[i]['timestamp']).total_seconds()

                if time_diff < 5:  # Within 5 seconds = concurrent
                    concurrent_found = True
                    op1 = log_df.iloc[i]
                    op2 = log_df.iloc[i+1]

                    # Determine test case
                    if op1['operation'] == 'READ' and op2['operation'] == 'READ':
                        case = "📖 Case #1: Concurrent Reads"
                        color = "blue"
                    elif (op1['operation'] == 'READ' and op2['operation'] in ['INSERT', 'UPDATE', 'DELETE']) or \
                         (op2['operation'] == 'READ' and op1['operation'] in ['INSERT', 'UPDATE', 'DELETE']):
                        case = "🔄 Case #2: Read-Write Conflict"
                        color = "orange"
                    elif op1['operation'] in ['INSERT', 'UPDATE', 'DELETE'] and op2['operation'] in ['INSERT', 'UPDATE', 'DELETE']:
                        case = "✍️ Case #3: Write-Write Conflict"
                        color = "red"

                    with st.container(border=True):
                        st.markdown(f"**{case}**")
                        col1, col2 = st.columns(2)

                        with col1:
                            st.write(f"**Operation 1**: {op1['operation']}")
                            st.write(f"Node: {op1['node']}")
                            st.write(f"Time: {op1['timestamp']}")

                        with col2:
                            st.write(f"**Operation 2**: {op2['operation']}")
                            st.write(f"Node: {op2['node']}")
                            st.write(f"Time: {op2['timestamp']}")

                        st.write(f"⏱️ Time difference: {time_diff:.2f}s")

            if not concurrent_found:
                st.info("ℹ️ No concurrent operations detected yet. Try performing operations within 5 seconds of each other.")

    # ============================================================================
    # TEST CASE #1: CONCURRENT READS
    # ============================================================================
    elif page == "Test Case #1":
        st.title("📖 Test Case #1: Concurrent Read Transactions")

        st.markdown("""
        Run automated tests to simulate concurrent read transactions across multiple nodes.
        This demonstrates that multiple transactions can read the same data simultaneously.
        """)

        # Configuration
        st.header("⚙️ Test Configuration")

        col1, col2, col3 = st.columns(3)

        with col1:
            isolation_level = st.selectbox(
                "Isolation Level",
                ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
                index=1,
                help="Controls how transactions see concurrent changes"
            )

        with col2:
            num_transactions = st.slider(
                "Number of Concurrent Transactions",
                min_value=2,
                max_value=20,
                value=3,
                help="How many transactions to run simultaneously"
            )

        with col3:
            scenario = st.selectbox(
                "Test Scenario",
                [
                    "Raw Reading",
                    "Same Account Transactions",
                    "Credit Transactions",
                    "Date Range Query",
                    "Account Analytics",
                    "High-Value Transactions"
                ]
            )

        # Map scenarios to queries
        scenario_queries = {
            "Raw Reading": "SELECT * FROM trans LIMIT 15000",
            "Same Account Transactions": "SELECT * FROM trans WHERE account_id = 1 LIMIT 15000",
            "Credit Transactions": "SELECT * FROM trans WHERE type = 'Credit' LIMIT 15000",
            "Date Range Query": "SELECT * FROM trans WHERE newdate BETWEEN '1995-01-01' AND '1995-12-31' LIMIT 15000",
            "Account Analytics": "SELECT account_id, COUNT(*) as trans_count, SUM(amount) as total_amount FROM trans GROUP BY account_id LIMIT 15000",
            "High-Value Transactions": "SELECT * FROM trans WHERE amount > 10000 ORDER BY amount DESC LIMIT 15000"
        }

        query = scenario_queries[scenario]

        # Show query
        with st.expander("📝 View SQL Query"):
            st.code(query, language="sql")

        # Run test button
        if st.button("🚀 Run Test", type="primary", use_container_width=True):
            # Import test class
            try:
                from case1_test import SimpleConcurrentReadTest

                # Initialize test
                test = SimpleConcurrentReadTest()

                # Progress indicator
                progress_text = st.empty()
                progress_bar = st.progress(0)

                progress_text.text(f"Initializing {num_transactions} concurrent transactions...")
                progress_bar.progress(20)

                # Run test (suppress print statements)
                import io
                import sys
                old_stdout = sys.stdout
                sys.stdout = io.StringIO()

                try:
                    results = test.run_test(
                        query=query,
                        num_transactions=num_transactions,
                        isolation_level=isolation_level
                    )

                    # Calculate metrics
                    metrics = test.calculate_metrics()

                finally:
                    sys.stdout = old_stdout

                progress_bar.progress(100)
                progress_text.text("✅ Test completed!")

                # Display results in tabs
                tab1, tab2, tab3 = st.tabs(["📊 Summary", "⏱️ Timeline", "📈 Analysis"])

                with tab1:
                    st.subheader("Test Summary")

                    # Create summary table
                    summary_data = []
                    for txn_id, result in sorted(results.items()):
                        summary_data.append({
                            'Transaction': txn_id,
                            'Node': result['node'],
                            'Status': '✅ Success' if result['status'] == 'SUCCESS' else '❌ Failed',
                            'Rows Read': result.get('rows_read', 'N/A'),
                            'Duration (s)': f"{result['duration']:.6f}"
                        })

                    df = pd.DataFrame(summary_data)
                    st.dataframe(df, use_container_width=True, hide_index=True)

                    # Metrics
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("Success Rate", f"{metrics['success_rate']:.2f}%")
                    with col2:
                        st.metric("Throughput", f"{metrics['throughput']:.6f} txn/s")
                    with col3:
                        st.metric("Avg Response Time", f"{metrics['avg_response_time']:.6f}s")

                with tab2:
                    st.subheader("Transaction Timeline")

                    # Timeline visualization
                    timeline_data = []
                    for txn_id, result in sorted(results.items()):
                        timeline_data.append({
                            'Transaction': txn_id,
                            'Duration': result['duration']
                        })

                    df_timeline = pd.DataFrame(timeline_data)
                    st.bar_chart(df_timeline.set_index('Transaction')['Duration'])

                    st.info("""
                    **Interpretation**: 
                    - Similar bar lengths (~2s each) = Concurrent execution ✅
                    - One bar much longer = Sequential execution ❌
                    """)

                with tab3:
                    st.subheader("Concurrency Analysis")

                    col1, col2 = st.columns(2)

                    with col1:
                        st.metric("Total Execution Time", f"{metrics['total_time']:.6f}s")
                        sequential_time = sum(r['duration'] for r in results.values())
                        st.metric("If Run Sequentially", f"{sequential_time:.6f}s")

                    with col2:
                        speedup = sequential_time / metrics['total_time'] if metrics['total_time'] > 0 else 1
                        st.metric("Speedup Factor", f"{speedup:.2f}x")

                        if speedup > 2:
                            st.success("✅ Excellent concurrency!")
                        elif speedup > 1.5:
                            st.info("ℹ️ Good concurrency")
                        else:
                            st.warning("⚠️ Limited concurrency")

                    # Data consistency check
                    st.markdown("---")
                    st.subheader("Data Consistency Check")

                    successful_reads = [r for r in results.values() if r['status'] == 'SUCCESS']
                    if successful_reads:
                        row_counts = [r['rows_read'] for r in successful_reads]
                        if len(set(row_counts)) == 1:
                            st.success(f"✅ CONSISTENT: All transactions read {row_counts[0]} rows")
                        else:
                            st.warning(f"⚠️ Row counts vary: {set(row_counts)}")
                            st.info("Note: Different nodes may have different data partitions")

            except ImportError as e:
                st.error(f"❌ Error importing test module: {str(e)}")
                st.info("Make sure case1_test.py is in the same directory as app.py")
            except Exception as e:
                st.error(f"❌ Test failed: {str(e)}")
                st.exception(e)

if __name__ == "__main__":
    main()
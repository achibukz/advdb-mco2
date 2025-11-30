"""
STADVDB MCO2: Distributed Database Transaction Manager
Users perform CRUD operations; System logs concurrent transactions automatically
"""

import streamlit as st
from datetime import datetime
import json

from python.db.db_config import fetch_data, execute_query, get_node_config
from python.utils.lock_manager import DistributedLockManager
from python.utils.server_ping import NodePinger

# Import page modules
import python.gui.view_transactions as view_transactions
import python.gui.add_transaction as add_transaction
import python.gui.update_transaction as update_transaction
import python.gui.delete_transaction as delete_transaction
import python.gui.transaction_log as transaction_log
import python.gui.test_case1 as test_case1

st.set_page_config(
    page_title="Transaction Manager",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for transaction tracking
if 'transaction_log' not in st.session_state:
    st.session_state.transaction_log = []

# Initialize session state for active transactions (multiple pending transactions)
if 'active_transactions' not in st.session_state:
    st.session_state.active_transactions = []  # List of transaction metadata
if 'transaction_connections' not in st.session_state:
    st.session_state.transaction_connections = []  # Parallel list of MySQL connections
if 'transaction_cursors' not in st.session_state:
    st.session_state.transaction_cursors = []  # Parallel list of cursors

# Initialize distributed lock manager
if 'lock_manager' not in st.session_state:
    node_configs = {
        1: get_node_config(1),
        2: get_node_config(2),
        3: get_node_config(3)
    }
    st.session_state.lock_manager = DistributedLockManager(
        node_configs,
        current_node_id="streamlit_app"
    )

# Initialize and start node pinger
if 'node_pinger' not in st.session_state:
    st.session_state.node_pinger = NodePinger(interval=5)
    st.session_state.node_pinger.start()

# Check for recovery notifications on ALL pages (in sidebar)
if 'node_pinger' in st.session_state:
    recovery_notifications = st.session_state.node_pinger.get_recovery_notifications()
    
    # Show recovery notifications as real-time alerts
    for notification in recovery_notifications:
        node = notification['node']
        timestamp = notification['timestamp']
        
        # Get detailed recovery info if available
        recovery_details = st.session_state.node_pinger.get_recovery_details()
        if node in recovery_details:
            details = recovery_details[node]
            results = details['results']
            
            if results['recovered'] > 0:
                st.sidebar.success(f"Node {node} Recovery Complete!")
                st.sidebar.info(f"Recovered {results['recovered']} transaction(s) at {timestamp[:19]}")
                st.toast(f"Node {node} recovered {results['recovered']} pending transaction(s)!", icon="🔄")
            else:
                st.sidebar.info(f"Node {node} came back online - No pending transactions")
                st.toast(f"Node {node} is back online", icon="🟢")
        else:
            st.sidebar.info(f"Node {node} is back online")
            st.toast(f"Node {node} is back online", icon="🟢")

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
    # Check for recovery notifications from node pinger
    if 'node_pinger' in st.session_state:
        recovery_notifications = st.session_state.node_pinger.get_recovery_notifications()
        recovery_details = st.session_state.node_pinger.get_recovery_details()
        
        # Show recovery notifications as popups
        for notification in recovery_notifications:
            node = notification['node']
            timestamp = notification['timestamp']
            
            # Get detailed recovery info if available
            if node in recovery_details:
                details = recovery_details[node]
                results = details['results']
                
                if results['recovered'] > 0:
                    st.success(f"Node {node} Recovery Complete!")
                    st.info(f"Recovered {results['recovered']} pending transaction(s) at {timestamp}")
                    
                    # Show detailed recovery info in expander
                    with st.expander(f"Recovery Details - Node {node}"):
                        recovery_summary = {
                            'Total Logs Found': results['total_logs'],
                            'Successfully Recovered': results['recovered'],
                            'Failed Recoveries': results['failed'],
                            'Skipped (Duplicates)': results['skipped'],
                            'Nodes Checked': len(results.get('nodes_checked', []))
                        }
                        
                        for key, value in recovery_summary.items():
                            st.metric(key, value)
                        
                        st.caption(f"Recovery completed at {details['timestamp']}")
                else:
                    st.info(f"Node {node} came back online - No pending transactions to recover")
            else:
                st.info(f"Node {node} is back online")
    
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
                st.metric("Node 1 (Central)", "Active", f"{node1_count:,} rows")
            with col2:
                st.metric("Node 2 (Even Accounts)", "Active", f"{node2_count:,} rows")
            with col3:
                st.metric("Node 3 (Odd Accounts)", "Active", f"{node3_count:,} rows")
                
        except Exception as e:
            st.error(f"⚠️ Database connection issue: {str(e)}")
        
        # Show recovery status if available
        if 'node_pinger' in st.session_state:
            recovery_details = st.session_state.node_pinger.get_recovery_details()
            if recovery_details:
                st.markdown("---")
                st.subheader("Recent Recovery Activity")
                
                for node_id, details in recovery_details.items():
                    results = details['results']
                    timestamp = details['timestamp']
                    
                    with st.expander(f"Node {node_id} Recovery - {timestamp[:19]}"):
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("Recovered", results['recovered'])
                        with col2:
                            st.metric("Failed", results['failed'])
                        with col3:
                            st.metric("Skipped", results['skipped'])
                        with col4:
                            st.metric("Total Logs", results['total_logs'])
                        
                        if results['recovered'] > 0:
                            st.success(f"Successfully recovered {results['recovered']} pending transaction(s)")
                        elif results['total_logs'] == 0:
                            st.info(f"No pending transactions found for recovery")
                        else:
                            st.warning(f"Some transactions could not be recovered ({results['failed']} failed)")

    elif page == "View Transactions":
        view_transactions.render(get_node_for_account, log_transaction)

    elif page == "Add Transaction":
        add_transaction.render(get_node_for_account, log_transaction)

    elif page == "Update Transaction":
        update_transaction.render(get_node_for_account, log_transaction)

    elif page == "Delete Transaction":
        delete_transaction.render(get_node_for_account, log_transaction)

    elif page == "Transaction Log":
        transaction_log.render()

    elif page == "Test Case #1":
        test_case1.render()

if __name__ == "__main__":
    main()
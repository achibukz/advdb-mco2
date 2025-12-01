"""
STADVDB MCO2: Distributed Database Transaction Manager
Users perform CRUD operations; System logs concurrent transactions automatically
"""

import streamlit as st
from datetime import datetime
import json
import sys
import os

# Add parent directory to path for imports (fixes Streamlit Cloud deployment)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from python.db.db_config import fetch_data, execute_query, get_node_config, NODE_USE
from python.utils.lock_manager import DistributedLockManager
from python.utils.server_ping import NodePinger

# Import page modules
import python.gui.view_transactions as view_transactions
import python.gui.view_reports as view_reports
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
        current_node_id="app"
    )

# Initialize and start node pinger
if 'node_pinger' not in st.session_state:
    st.session_state.node_pinger = NodePinger(interval=5)
    st.session_state.node_pinger.start()

# No automatic recovery notifications

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
        "View Reports"
        # "Transaction Log",
        # "Test Case #1"
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
    # No automatic recovery notifications
    
    # ============================================================================
    # HOME PAGE
    # ============================================================================
    if page == "Home":
        st.title("Distributed Database Transaction Manager")

        st.markdown(f"""
        ## Welcome to the Transaction Manager
        
        Connected to Node {NODE_USE}
                    
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
        
        # Recovery system is now manual-only (triggered before each transaction)

    elif page == "View Transactions":
        view_transactions.render(get_node_for_account, log_transaction)

    elif page == "Add Transaction":
        add_transaction.render(get_node_for_account, log_transaction)

    elif page == "Update Transaction":
        update_transaction.render(get_node_for_account, log_transaction)

    elif page == "Delete Transaction":
        delete_transaction.render(get_node_for_account, log_transaction)

    elif page == "View Reports":
        view_reports.render()

    # elif page == "Transaction Log":
    #     transaction_log.render()

    # elif page == "Test Case #1":
    #     test_case1.render()

if __name__ == "__main__":
    main()
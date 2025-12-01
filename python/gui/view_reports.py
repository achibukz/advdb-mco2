import streamlit as st
import pandas as pd
from python.db.db_config import fetch_data

def render():
    """Render the View Reports page with aggregated summaries"""
    st.title("📊 Dataset Reports & Summaries")
    
    st.markdown("""
    View aggregated summaries and insights from the distributed transaction database.
    All data is aggregated from Node 1 (central node).
    """)
    
    try:
        # ============================================================================
        # 1. OVERALL STATISTICS
        # ============================================================================
        st.header("📈 Overall Statistics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        # Total transactions
        total_query = "SELECT COUNT(*) as total FROM trans"
        total_result = fetch_data(total_query, node=1)
        total_transactions = total_result['total'][0] if not total_result.empty else 0
        
        # Total accounts
        accounts_query = "SELECT COUNT(DISTINCT account_id) as total FROM trans"
        accounts_result = fetch_data(accounts_query, node=1)
        total_accounts = accounts_result['total'][0] if not accounts_result.empty else 0
        
        # Total amount
        amount_query = "SELECT SUM(amount) as total FROM trans"
        amount_result = fetch_data(amount_query, node=1)
        total_amount = amount_result['total'][0] if not amount_result.empty else 0
        
        # Average amount
        avg_query = "SELECT AVG(amount) as avg FROM trans"
        avg_result = fetch_data(avg_query, node=1)
        avg_amount = avg_result['avg'][0] if not avg_result.empty else 0
        
        with col1:
            st.metric("Total Transactions", f"{total_transactions:,}")
        with col2:
            st.metric("Unique Accounts", f"{total_accounts:,}")
        with col3:
            st.metric("Total Amount", f"${total_amount:,.2f}")
        with col4:
            st.metric("Average Amount", f"${avg_amount:,.2f}")
        
        st.markdown("---")
        
        # ============================================================================
        # 2. TRANSACTION TYPE BREAKDOWN
        # ============================================================================
        st.header("💳 Transaction Type Breakdown")
        
        type_query = """
        SELECT 
            type,
            COUNT(*) as count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM trans
        GROUP BY type
        """
        type_data = fetch_data(type_query, node=1)
        
        if not type_data.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Transaction Counts")
                st.dataframe(type_data[['type', 'count']], use_container_width=True, hide_index=True)
            
            with col2:
                st.subheader("Amount Statistics")
                type_data_formatted = type_data.copy()
                type_data_formatted['total_amount'] = type_data_formatted['total_amount'].apply(lambda x: f"${x:,.2f}")
                type_data_formatted['avg_amount'] = type_data_formatted['avg_amount'].apply(lambda x: f"${x:,.2f}")
                st.dataframe(type_data_formatted[['type', 'total_amount', 'avg_amount']], use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # ============================================================================
        # 3. TOP ACCOUNTS BY TRANSACTION COUNT
        # ============================================================================
        st.header("🔝 Top 10 Accounts by Transaction Count")
        
        top_accounts_query = """
        SELECT 
            account_id,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM trans
        GROUP BY account_id
        ORDER BY transaction_count DESC
        LIMIT 10
        """
        top_accounts = fetch_data(top_accounts_query, node=1)
        
        if not top_accounts.empty:
            top_accounts_formatted = top_accounts.copy()
            top_accounts_formatted['total_amount'] = top_accounts_formatted['total_amount'].apply(lambda x: f"${x:,.2f}")
            top_accounts_formatted['avg_amount'] = top_accounts_formatted['avg_amount'].apply(lambda x: f"${x:,.2f}")
            st.dataframe(top_accounts_formatted, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # ============================================================================
        # 4. AMOUNT DISTRIBUTION
        # ============================================================================
        st.header("💰 Amount Distribution")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Amount Ranges")
            ranges_query = """
            SELECT 
                CASE 
                    WHEN amount < 1000 THEN 'Under $1,000'
                    WHEN amount >= 1000 AND amount < 5000 THEN '$1,000 - $5,000'
                    WHEN amount >= 5000 AND amount < 10000 THEN '$5,000 - $10,000'
                    WHEN amount >= 10000 AND amount < 50000 THEN '$10,000 - $50,000'
                    ELSE 'Over $50,000'
                END as amount_range,
                COUNT(*) as count
            FROM trans
            GROUP BY amount_range
            ORDER BY MIN(amount)
            """
            ranges_data = fetch_data(ranges_query, node=1)
            if not ranges_data.empty:
                st.dataframe(ranges_data, use_container_width=True, hide_index=True)
        
        with col2:
            st.subheader("Min/Max Amounts")
            minmax_query = """
            SELECT 
                MIN(amount) as min_amount,
                MAX(amount) as max_amount
            FROM trans
            """
            minmax_data = fetch_data(minmax_query, node=1)
            if not minmax_data.empty:
                st.metric("Minimum Amount", f"${minmax_data['min_amount'][0]:,.2f}")
                st.metric("Maximum Amount", f"${minmax_data['max_amount'][0]:,.2f}")
        
        st.markdown("---")
        
        # ============================================================================
        # 5. YEARLY SUMMARY
        # ============================================================================
        st.header("📅 Transactions by Year")
        
        year_query = """
        SELECT 
            YEAR(newdate) as year,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount
        FROM trans
        GROUP BY YEAR(newdate)
        ORDER BY year DESC
        LIMIT 10
        """
        year_data = fetch_data(year_query, node=1)
        
        if not year_data.empty:
            year_data_formatted = year_data.copy()
            year_data_formatted['total_amount'] = year_data_formatted['total_amount'].apply(lambda x: f"${x:,.2f}")
            st.dataframe(year_data_formatted, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # ============================================================================
        # 6. NODE DISTRIBUTION
        # ============================================================================
        st.header("🖥️ Data Distribution Across Nodes")
        
        col1, col2, col3 = st.columns(3)
        
        # Node 1 (Central)
        node1_query = "SELECT COUNT(*) as count FROM trans"
        node1_data = fetch_data(node1_query, node=1)
        node1_count = node1_data['count'][0] if not node1_data.empty else 0
        
        # Node 2 (Even accounts)
        node2_query = "SELECT COUNT(*) as count FROM trans WHERE account_id % 2 = 0"
        node2_data = fetch_data(node2_query, node=1)
        node2_count = node2_data['count'][0] if not node2_data.empty else 0
        
        # Node 3 (Odd accounts)
        node3_query = "SELECT COUNT(*) as count FROM trans WHERE account_id % 2 = 1"
        node3_data = fetch_data(node3_query, node=1)
        node3_count = node3_data['count'][0] if not node3_data.empty else 0
        
        with col1:
            st.metric("Node 1 (Central)", f"{node1_count:,} rows", "All data")
        with col2:
            st.metric("Node 2 (Even)", f"{node2_count:,} rows", "Even account_ids")
        with col3:
            st.metric("Node 3 (Odd)", f"{node3_count:,} rows", "Odd account_ids")
        
        st.caption("Note: Node 1 contains all transactions. Nodes 2 & 3 contain partitioned subsets.")
        
    except Exception as e:
        st.error(f"❌ Error loading reports: {str(e)}")
        st.info("💡 Make sure the database is running and accessible.")

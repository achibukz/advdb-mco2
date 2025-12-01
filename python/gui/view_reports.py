import streamlit as st
import pandas as pd
from python.db.db_config import fetch_data

def render():
    """Render the View Reports page with aggregated summaries"""
    st.title("Dataset Reports & Summaries")
    
    st.markdown("""
    View aggregated summaries and insights from the distributed transaction database.
    All data is aggregated from Node 1 (central node).
    """)
    
    try:
        # ============================================================================
        # TRANSACTION TYPE BREAKDOWN
        # ============================================================================
        st.header("Transaction Type Breakdown")
        
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
        # AMOUNT DISTRIBUTION
        # ============================================================================
        st.header("Amount Distribution")
        
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
        # YEARLY SUMMARY
        # ============================================================================
        st.header("Transactions by Year")
        
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
        
        
    except Exception as e:
        st.error(f"❌ Error loading reports: {str(e)}")
        st.info("💡 Make sure the database is running and accessible.")
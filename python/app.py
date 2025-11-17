"""
Simplified Multi-Node Transaction Viewer
Displays transaction data from all 3 database nodes
"""

import streamlit as st
import pandas as pd

# Import database functions from separate config file
from db_config import fetch_data

# Set page config for wider layout
st.set_page_config(layout="wide", page_title="Transaction Viewer")

# Title
st.title("Transaction Data Viewer")

# Sidebar for node selection
st.sidebar.header("Database Node Selection")
selected_node = st.sidebar.selectbox(
    "Select Node to View:",
    [1, 2, 3],
    format_func=lambda x: f"Node {x}"
)

st.sidebar.markdown("---")
st.sidebar.markdown("Multi-Node Database System")
st.sidebar.markdown("STADVDB S17 | Group 12")

# Main content
st.write(f"### Viewing data from Node {selected_node}")
st.write("Query: `SELECT * FROM trans LIMIT 20`")

# Fetch and display data
try:
    data = fetch_data("SELECT * FROM trans LIMIT 20", node=selected_node)

    if data.empty:
        st.warning("⚠️ No data found in trans table")
    else:
        st.success(f"✅ Retrieved {len(data)} rows from Node {selected_node}")

        st.write("Detailed Data:")
        st.dataframe(data, use_container_width=True)

except Exception as e:
    st.error(f"❌ Error querying database: {str(e)}")
    st.info("Please check your database connection settings.")


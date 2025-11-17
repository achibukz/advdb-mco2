"""
Database Configuration and Connection Module - Multi-Node Support

This module handles all database connections for the Financial Reports Dashboard.
It supports both local MySQL connections and Google Cloud SQL connections for 3 nodes.
Works with both Streamlit secrets and .env files.
Uses st.connection() for better connection management when running in Streamlit.
"""

import mysql.connector
import pandas as pd
import hashlib
import pickle
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Cloud SQL connector will be initialized only when needed
_connector = None
_streamlit_connections = {}  # Cache for st.connection per node

def _is_running_in_streamlit():
    """
    Check if code is running inside a Streamlit app.

    Returns:
        bool: True if running in Streamlit, False otherwise
    """
    try:
        import streamlit as st
        return hasattr(st, 'secrets')
    except ImportError:
        return False


def _get_config_value(key, default=''):
    """
    Get configuration value from Streamlit secrets or environment variables.
    Streamlit secrets take precedence if available.

    Args:
        key (str): Configuration key name
        default: Default value if key not found

    Returns:
        Configuration value
    """
    try:
        # Try to import streamlit and use secrets
        import streamlit as st
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except (ImportError, FileNotFoundError, KeyError):
        pass

    # Fall back to environment variables
    return os.getenv(key, default)


# Query Cache Configuration
CACHE_ENABLED = str(_get_config_value('CACHE_ENABLED', 'True')).lower() == 'true'
CACHE_TTL_SECONDS = int(_get_config_value('CACHE_TTL_SECONDS', '3600'))
_query_cache = {}  # In-memory cache storage per node

# Database Configuration
# Choose connection method by setting USE_CLOUD_SQL environment variable
USE_CLOUD_SQL = str(_get_config_value('USE_CLOUD_SQL', 'False')).lower() == 'true'

# Node 1 Configuration
CLOUD_SQL_CONFIG_NODE1 = {
    "host": _get_config_value('CLOUD_DB_HOST'),
    "port": int(_get_config_value('CLOUD_DB_PORT', '3306')),
    "user": _get_config_value('CLOUD_DB_USER'),
    "password": _get_config_value('CLOUD_DB_PASSWORD'),
    "database": _get_config_value('CLOUD_DB_NAME')
}

LOCAL_CONFIG_NODE1 = {
    "host": _get_config_value('LOCAL_DB_HOST'),
    "port": int(_get_config_value('LOCAL_DB_PORT', '3306')),
    "user": _get_config_value('LOCAL_DB_USER'),
    "password": _get_config_value('LOCAL_DB_PASSWORD'),
    "database": _get_config_value('LOCAL_DB_NAME')
}

# Node 2 Configuration
CLOUD_SQL_CONFIG_NODE2 = {
    "host": _get_config_value('CLOUD_DB_HOST_NODE2', _get_config_value('CLOUD_DB_HOST')),
    "port": int(_get_config_value('CLOUD_DB_PORT_NODE2', '3306')),
    "user": _get_config_value('CLOUD_DB_USER_NODE2', _get_config_value('CLOUD_DB_USER')),
    "password": _get_config_value('CLOUD_DB_PASSWORD_NODE2', _get_config_value('CLOUD_DB_PASSWORD')),
    "database": _get_config_value('CLOUD_DB_NAME_NODE2', 'node2_db')
}

LOCAL_CONFIG_NODE2 = {
    "host": _get_config_value('LOCAL_DB_HOST_NODE2', 'localhost'),
    "port": int(_get_config_value('LOCAL_DB_PORT_NODE2', '3307')),
    "user": _get_config_value('LOCAL_DB_USER_NODE2', _get_config_value('LOCAL_DB_USER')),
    "password": _get_config_value('LOCAL_DB_PASSWORD_NODE2', _get_config_value('LOCAL_DB_PASSWORD')),
    "database": _get_config_value('LOCAL_DB_NAME_NODE2', 'node2_db')
}

# Node 3 Configuration
CLOUD_SQL_CONFIG_NODE3 = {
    "host": _get_config_value('CLOUD_DB_HOST_NODE3', _get_config_value('CLOUD_DB_HOST')),
    "port": int(_get_config_value('CLOUD_DB_PORT_NODE3', '3306')),
    "user": _get_config_value('CLOUD_DB_USER_NODE3', _get_config_value('CLOUD_DB_USER')),
    "password": _get_config_value('CLOUD_DB_PASSWORD_NODE3', _get_config_value('CLOUD_DB_PASSWORD')),
    "database": _get_config_value('CLOUD_DB_NAME_NODE3', 'node3_db')
}

LOCAL_CONFIG_NODE3 = {
    "host": _get_config_value('LOCAL_DB_HOST_NODE3', 'localhost'),
    "port": int(_get_config_value('LOCAL_DB_PORT_NODE3', '3308')),
    "user": _get_config_value('LOCAL_DB_USER_NODE3', _get_config_value('LOCAL_DB_USER')),
    "password": _get_config_value('LOCAL_DB_PASSWORD_NODE3', _get_config_value('LOCAL_DB_PASSWORD')),
    "database": _get_config_value('LOCAL_DB_NAME_NODE3', 'node3_db')
}

# Map node numbers to configurations
NODE_CONFIGS = {
    1: {"cloud": CLOUD_SQL_CONFIG_NODE1, "local": LOCAL_CONFIG_NODE1},
    2: {"cloud": CLOUD_SQL_CONFIG_NODE2, "local": LOCAL_CONFIG_NODE2},
    3: {"cloud": CLOUD_SQL_CONFIG_NODE3, "local": LOCAL_CONFIG_NODE3}
}

# Map node numbers to Streamlit connection names
STREAMLIT_CONN_NAMES = {
    1: {"cloud": "mysql", "local": "mysql_local"},
    2: {"cloud": "mysql_node2", "local": "mysql_local_node2"},
    3: {"cloud": "mysql_node3", "local": "mysql_local_node3"}
}


def _generate_cache_key(query, node=1):
    """
    Generate a unique cache key for a query and node combination.

    Args:
        query (str): SQL query string
        node (int): Node number (1, 2, or 3)

    Returns:
        str: MD5 hash of the query and node
    """
    # Normalize query: strip whitespace and convert to lowercase
    normalized_query = ' '.join(query.strip().lower().split())
    # Include node number in cache key
    cache_input = f"node{node}:{normalized_query}"
    return hashlib.md5(cache_input.encode()).hexdigest()


def _is_cache_valid(cache_entry):
    """
    Check if a cache entry is still valid based on TTL.

    Args:
        cache_entry (dict): Cache entry with 'timestamp' and 'data' keys

    Returns:
        bool: True if cache is valid, False if expired
    """
    if not CACHE_ENABLED:
        return False

    timestamp = cache_entry.get('timestamp')
    if not timestamp:
        return False

    age = (datetime.now() - timestamp).total_seconds()
    return age < CACHE_TTL_SECONDS


def get_node_config(node=1):
    """
    Get the configuration for a specific node.

    Args:
        node (int): Node number (1, 2, or 3)

    Returns:
        dict: Configuration dictionary for the specified node

    Raises:
        ValueError: If node number is invalid
    """
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")

    config_type = "cloud" if USE_CLOUD_SQL else "local"
    return NODE_CONFIGS[node][config_type]


def get_db_connection(node=1):
    """
    Establish and return a database connection for a specific node.

    Args:
        node (int): Node number (1, 2, or 3)

    Returns:
        mysql.connector.connection: Database connection object

    Raises:
        Exception: If connection fails
    """
    config = get_node_config(node)
    config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"

    try:
        conn = mysql.connector.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            connect_timeout=10  # Add timeout to prevent hanging
        )
        return conn
    except mysql.connector.Error as db_err:
        error_code = db_err.errno if hasattr(db_err, 'errno') else 'Unknown'
        error_msg = (
            f"Failed to connect to {config_type} database (Node {node})\n"
            f"Host: {config['host']}:{config['port']}\n"
            f"Database: {config['database']}\n"
            f"User: {config['user']}\n"
            f"Error Code: {error_code}\n"
            f"Error: {str(db_err)}\n\n"
            f"Common solutions:\n"
            f"1. Check if the database server is running\n"
            f"2. Verify host/port are correct\n"
            f"3. Ensure user has proper permissions\n"
            f"4. Check firewall/network settings\n"
            f"5. For Cloud SQL: verify IP whitelist and public IP access"
        )
        raise Exception(error_msg)
    except Exception as e:
        raise Exception(f"Failed to connect to {config_type} database (Node {node}) at {config['host']}:{config['port']}\n"
                      f"Error: {str(e)}")


def fetch_data(query, node, ttl=9999):
    """
    Execute a SQL query and return results as a pandas DataFrame from a specific node.
    Uses st.connection() when running in Streamlit for better caching and connection management.
    Falls back to direct MySQL connection when not in Streamlit.

    Args:
        query (str): SQL query to execute
        node (int): Node number (1, 2, or 3) to query from
        ttl (int): Time-to-live for cached results in seconds (default: 9999)

    Returns:
        pandas.DataFrame: Query results

    Raises:
        Exception: If query execution fails
    """
    # Validate node number
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")

    # Try to use Streamlit connection if available
    if _is_running_in_streamlit():
        try:
            import streamlit as st
            # Get the appropriate connection name for this node
            config_type = "cloud" if USE_CLOUD_SQL else "local"
            conn_name = STREAMLIT_CONN_NAMES[node][config_type]

            # Use st.connection for automatic caching and connection management
            conn = st.connection(conn_name, type='sql')
            # Execute query with built-in caching (ttl in seconds)
            return conn.query(query, ttl=ttl)
        except Exception as e:
            config = get_node_config(node)
            config_type_name = "Cloud SQL" if USE_CLOUD_SQL else "Local"
            error_msg = (
                f"Streamlit connection failed for {config_type_name} (Node {node}) "
                f"({config['host']}:{config['port']}/{config['database']}): {str(e)}\n"
                f"Error type: {type(e).__name__}\n\n"
                f"Make sure your secrets.toml has [connections.{conn_name}] configured correctly.\n"
                f"Current USE_CLOUD_SQL setting: {USE_CLOUD_SQL}"
            )
            raise Exception(error_msg)

    # Not in Streamlit - use manual connection with custom caching
    cache_key = _generate_cache_key(query, node)

    # Check if valid cached result
    if CACHE_ENABLED and cache_key in _query_cache:
        cache_entry = _query_cache[cache_key]
        if _is_cache_valid(cache_entry):
            return cache_entry['data'].copy()
        else:
            del _query_cache[cache_key]

    # Cache miss or expired - fetch from database
    conn = None
    cursor = None

    try:
        conn = get_db_connection(node)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        data = cursor.fetchall()
        result_df = pd.DataFrame(data)

        # Store in cache
        if CACHE_ENABLED:
            _query_cache[cache_key] = {
                'timestamp': datetime.now(),
                'data': result_df.copy(),
                'query': query[:100],
                'node': node
            }

        return result_df

    except mysql.connector.Error as db_err:
        config = get_node_config(node)
        config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"
        error_msg = (
            f"Database error while fetching data from {config_type} (Node {node}) "
            f"({config['host']}:{config['port']}/{config['database']}): {str(db_err)}\n"
            f"Query: {query[:200]}..."
        )
        raise Exception(error_msg)

    except Exception as e:
        config = get_node_config(node)
        config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"
        error_msg = (
            f"Failed to fetch data from {config_type} (Node {node}) "
            f"({config['host']}:{config['port']}/{config['database']}): {str(e)}\n"
            f"Error type: {type(e).__name__}"
        )
        raise Exception(error_msg)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def execute_multi_statement_query(query, node=1, ttl=3600):
    """
    Execute a multi-statement SQL query and return the final SELECT results from a specific node.
    Useful for queries that create temporary tables before selecting data.
    Results are cached to avoid repeated database queries.

    IMPORTANT: This function bypasses Streamlit's connection pooling to maintain
    a single connection across all statements (required for temporary tables).

    Args:
        query (str): Multi-statement SQL query (statements separated by semicolons)
        node (int): Node number (1, 2, or 3) to execute on
        ttl (int): Time-to-live for cached results in seconds (default: 3600)

    Returns:
        pandas.DataFrame: Results from the final SELECT statement (from cache or fresh)

    Raises:
        Exception: If query execution fails
    """
    # Validate node number
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")

    # Generate cache key
    cache_key = _generate_cache_key(query, node)

    # Check if we have a valid cached result
    if CACHE_ENABLED and cache_key in _query_cache:
        cache_entry = _query_cache[cache_key]
        if _is_cache_valid(cache_entry):
            # Return cached data (create a copy to prevent modifications)
            return cache_entry['data'].copy()
        else:
            # Remove expired cache entry
            del _query_cache[cache_key]

    # Cache miss or expired - fetch from database
    conn = None
    cursor = None
    config = get_node_config(node)
    config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"

    try:
        # Establish direct connection (not using st.connection to maintain single session)
        conn = mysql.connector.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            connect_timeout=30,  # Longer timeout for cloud connections
            autocommit=True,  # Important for temporary tables
            allow_local_infile=False,  # Security setting
            consume_results=True  # Automatically consume unread results
        )

        cursor = conn.cursor(dictionary=True, buffered=True)

        # Split the query into individual statements
        statements = [s.strip() for s in query.split(';') if s.strip()]

        if len(statements) == 0:
            raise Exception("No valid SQL statements found in query")

        # Execute all statements in sequence on the same connection
        # This ensures temporary tables persist across statements
        for i, statement in enumerate(statements[:-1]):
            try:
                cursor.execute(statement)
                # Try to consume any results
                try:
                    cursor.fetchall()
                except mysql.connector.errors.InterfaceError:
                    pass  # No results to fetch (e.g., CREATE, DROP statements)
            except mysql.connector.Error as stmt_err:
                raise Exception(f"Failed to execute statement {i+1}/{len(statements)} on Node {node}: {str(stmt_err)}\nStatement: {statement[:200]}")

        # Execute the final SELECT statement and fetch results
        try:
            cursor.execute(statements[-1])
            data = cursor.fetchall()
            result_df = pd.DataFrame(data)
        except mysql.connector.Error as stmt_err:
            raise Exception(f"Failed to execute final SELECT statement on Node {node}: {str(stmt_err)}\nStatement: {statements[-1][:200]}")

        # Store in cache
        if CACHE_ENABLED:
            _query_cache[cache_key] = {
                'timestamp': datetime.now(),
                'data': result_df.copy(),
                'query': query[:100],  # Store first 100 chars for debugging
                'node': node
            }

        return result_df

    except mysql.connector.Error as db_err:
        error_code = db_err.errno if hasattr(db_err, 'errno') else 'Unknown'
        error_msg = (
            f"Database error in multi-statement query ({config_type}, Node {node})\n"
            f"Host: {config['host']}:{config['port']}\n"
            f"Database: {config['database']}\n"
            f"Error Code: {error_code}\n"
            f"Error: {str(db_err)}\n\n"
            f"Query preview: {query[:300]}..."
        )
        raise Exception(error_msg)

    except Exception as e:
        error_msg = (
            f"Failed to execute multi-statement query on {config_type} (Node {node})\n"
            f"Host: {config['host']}:{config['port']}\n"
            f"Error type: {type(e).__name__}\n"
            f"Error: {str(e)}"
        )
        raise Exception(error_msg)

    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass


def test_connection(node=1):
    """
    Test the database connection for a specific node.

    Args:
        node (int): Node number (1, 2, or 3) to test

    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        conn = get_db_connection(node)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"Node {node} database connection successful!")
        return True
    except Exception as e:
        print(f"Node {node} database connection failed: {str(e)}")
        return False
"""
Node Failure Simulation Script

This script simulates node failures by revoking database privileges from the 'user' account.
It can be used to test the system's behavior during node failures and recovery.

Usage:
    python fail_start.py <node_number> [<node_number> ...]

Examples:
    python fail_start.py 1          # Fail node 1
    python fail_start.py 2          # Fail node 2
    python fail_start.py 2 3        # Fail nodes 2 and 3
    python fail_start.py 1 2 3      # Fail all nodes

Test Cases:
    Case 1: Fail node 1 (from node2/3 to node1 fail write transaction)
        python fail_start.py 1

    Case 2: Recover node 1 (node1 comes back online)
        Input 'Y' when prompted to grant privileges back

    Case 3: Fail nodes 2 and 3 (from node1 to node2/3 fail write transaction)
        python fail_start.py 2 3

    Case 4: Recover nodes 2 and 3 (node2/3 comes back online)
        Input 'Y' when prompted to grant privileges back
"""

import sys
import mysql.connector
from python.db.db_config import get_node_config, USE_CLOUD_SQL

# Node database names mapping
NODE_DATABASES = {
    1: 'node1_db',
    2: 'node2_db',
    3: 'node3_db'
}

def get_root_connection(node):
    """
    Get a root/admin connection to perform privilege management.
    Uses 'tester' user (admin user with all privileges) to revoke/grant privileges.

    Args:
        node (int): Node number (1, 2, or 3)

    Returns:
        mysql.connector.connection: Database connection with admin privileges
    """
    config = get_node_config(node)

    # Use tester credentials for privilege management
    # Tester user has all privileges and works in both local and cloud environments
    admin_config = config.copy()

    if not USE_CLOUD_SQL:
        # For local environment, use tester user
        admin_config['user'] = 'tester'
        admin_config['password'] = 'testpass'
    else:
        # For cloud environment, use tester user
        admin_config['user'] = 'tester'
        admin_config['password'] = 'Testpass123!'

    try:
        conn = mysql.connector.connect(
            host=admin_config["host"],
            port=admin_config["port"],
            user=admin_config["user"],
            password=admin_config["password"],
            database=admin_config["database"],
            connect_timeout=10
        )
        return conn
    except mysql.connector.Error as e:
        raise Exception(f"Failed to connect to Node {node} as admin: {str(e)}")


def kill_user_sessions(node, username='user'):
    """
    Kill all active connections for a specific user on a node.
    This is only required for LOCAL environments.

    Args:
        node (int): Node number (1, 2, or 3)
        username (str): Username to kill sessions for (default: 'user')
    """
    if USE_CLOUD_SQL:
        print(f"  [Node {node}] Skipping session kill (Cloud SQL environment)")
        return

    conn = None
    cursor = None

    try:
        conn = get_root_connection(node)
        cursor = conn.cursor(buffered=True)

        # First, get all process IDs for the user
        list_query = f"""
            SELECT id, user, host, db, command, time, state
            FROM information_schema.processlist
            WHERE user = '{username}'
        """

        cursor.execute(list_query)
        processes = cursor.fetchall()

        if processes:
            print(f"  [Node {node}] Found {len(processes)} active session(s) for user '{username}':")

            killed_count = 0
            for process in processes:
                process_id = process[0]
                host = process[2]
                db = process[3]
                command = process[4]

                print(f"  [Node {node}]   - Killing Process ID {process_id} (Host: {host}, DB: {db}, Command: {command})")

                try:
                    kill_cmd = f"KILL {process_id}"
                    cursor.execute(kill_cmd)
                    killed_count += 1
                except mysql.connector.Error as e:
                    # Some sessions might close before we kill them
                    print(f"  [Node {node}]     Warning: Could not kill process {process_id}: {str(e)}")

            print(f"  [Node {node}] Successfully killed {killed_count} session(s)")
        else:
            print(f"  [Node {node}] No active sessions found for user '{username}'")

    except Exception as e:
        print(f"  [Node {node}] Warning: Could not kill sessions: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def revoke_privileges(node, username='user'):
    """
    Lock the user account to simulate node failure.
    For local environments, uses ACCOUNT LOCK.
    For cloud environments, revokes privileges.

    Args:
        node (int): Node number (1, 2, or 3)
        username (str): Username to lock/revoke (default: 'user')
    """
    conn = None
    cursor = None
    database = NODE_DATABASES[node]

    try:
        conn = get_root_connection(node)
        cursor = conn.cursor()

        if not USE_CLOUD_SQL:
            # Local environment: Use ACCOUNT LOCK
            print(f"  [Node {node}] Locking account for user '{username}'...")

            # Lock the account to prevent any connections
            lock_query = f"ALTER USER '{username}'@'%' ACCOUNT LOCK"
            cursor.execute(lock_query)
            cursor.execute("FLUSH PRIVILEGES")

            print(f"  [Node {node}] Account locked for user '{username}'")

            # Kill existing connections
            kill_query = f"""
                SELECT GROUP_CONCAT(CONCAT('KILL ', id, ';') SEPARATOR ' ')
                FROM information_schema.processlist
                WHERE user = '{username}'
            """

            cursor.execute(kill_query)
            result = cursor.fetchone()

            if result and result[0]:
                kill_commands = result[0]
                print(f"  [Node {node}] Killing {len(kill_commands.split(';'))-1} existing connection(s)...")

                # Execute kill commands using prepared statement
                try:
                    cursor.execute(f"SET @kills = (SELECT GROUP_CONCAT(CONCAT('KILL ', id, ';') SEPARATOR ' ') FROM information_schema.processlist WHERE user = '{username}')")
                    cursor.execute("PREPARE stmt FROM @kills")
                    cursor.execute("EXECUTE stmt")
                    cursor.execute("DEALLOCATE PREPARE stmt")
                    print(f"  [Node {node}] Successfully killed all connections for user '{username}'")
                except mysql.connector.Error as e:
                    # If prepared statement fails, try individual kills
                    for kill_cmd in kill_commands.split(';'):
                        kill_cmd = kill_cmd.strip()
                        if kill_cmd:
                            try:
                                cursor.execute(kill_cmd)
                            except mysql.connector.Error:
                                pass
                    print(f"  [Node {node}] Killed connections using fallback method")
            else:
                print(f"  [Node {node}] No existing connections to kill")

        else:
            # Cloud environment: Revoke privileges (account lock may not work)
            print(f"  [Node {node}] Revoking privileges for user '{username}' on {database}...")
            revoke_query = f"REVOKE ALL PRIVILEGES ON {database}.* FROM '{username}'@'%'"
            cursor.execute(revoke_query)
            cursor.execute("FLUSH PRIVILEGES")
            print(f"  [Node {node}] Successfully revoked privileges for user '{username}' on {database}")

        print(f"  [Node {node}] ❌ NODE {node} IS NOW OFFLINE (simulated)")

    except mysql.connector.Error as e:
        print(f"  [Node {node}] Error simulating failure: {str(e)}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def grant_privileges(node, username='user'):
    """
    Unlock the user account to simulate node recovery.
    For local environments, uses ACCOUNT UNLOCK.
    For cloud environments, grants privileges back.

    Args:
        node (int): Node number (1, 2, or 3)
        username (str): Username to unlock/grant (default: 'user')
    """
    conn = None
    cursor = None
    database = NODE_DATABASES[node]

    try:
        conn = get_root_connection(node)
        cursor = conn.cursor()

        if not USE_CLOUD_SQL:
            # Local environment: Use ACCOUNT UNLOCK
            print(f"  [Node {node}] Unlocking account for user '{username}'...")

            # Unlock the account to restore access
            unlock_query = f"ALTER USER '{username}'@'%' ACCOUNT UNLOCK"
            cursor.execute(unlock_query)
            cursor.execute("FLUSH PRIVILEGES")

            print(f"  [Node {node}] Successfully unlocked account for user '{username}'")

        else:
            # Cloud environment: Grant privileges back
            print(f"  [Node {node}] Granting privileges for user '{username}' on {database}...")
            grant_query = f"GRANT ALL PRIVILEGES ON {database}.* TO '{username}'@'%'"
            cursor.execute(grant_query)
            cursor.execute("FLUSH PRIVILEGES")
            print(f"  [Node {node}] Successfully granted privileges for user '{username}' on {database}")

        print(f"  [Node {node}] ✅ NODE {node} IS NOW ONLINE (recovered)")

    except mysql.connector.Error as e:
        print(f"  [Node {node}] Error restoring access: {str(e)}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main():
    """
    Main function to handle node failure simulation.
    """
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python fail_start.py <node_number> [<node_number> ...]")
        print("\nExamples:")
        print("  python fail_start.py 1       # Fail node 1")
        print("  python fail_start.py 2       # Fail node 2")
        print("  python fail_start.py 2 3     # Fail nodes 2 and 3")
        print("  python fail_start.py 1 2 3   # Fail all nodes")
        print("\nTest Cases:")
        print("  Case 1: python fail_start.py 1       (Node 1 fails)")
        print("  Case 2: Grant privileges back         (Node 1 recovers)")
        print("  Case 3: python fail_start.py 2 3     (Nodes 2/3 fail)")
        print("  Case 4: Grant privileges back         (Nodes 2/3 recover)")
        sys.exit(1)

    # Parse node numbers from command line
    try:
        nodes = [int(arg) for arg in sys.argv[1:]]

        # Validate node numbers
        for node in nodes:
            if node not in [1, 2, 3]:
                print(f"Error: Invalid node number '{node}'. Must be 1, 2, or 3.")
                sys.exit(1)

        # Remove duplicates while preserving order
        nodes = list(dict.fromkeys(nodes))

    except ValueError:
        print("Error: Node numbers must be integers (1, 2, or 3)")
        sys.exit(1)

    # Display configuration
    env_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"
    print("=" * 60)
    print("NODE FAILURE SIMULATION SCRIPT")
    print("=" * 60)
    print(f"Environment: {env_type}")
    print(f"Target Nodes: {', '.join([f'Node {n}' for n in nodes])}")
    print("=" * 60)
    print()

    # Revoke privileges to simulate failure
    print("🔴 SIMULATING NODE FAILURE...")
    print("-" * 60)
    for node in nodes:
        try:
            revoke_privileges(node)
        except Exception as e:
            print(f"  [Node {node}] Failed to revoke privileges: {str(e)}")

    print("-" * 60)
    print()
    print("✅ Node failure simulation complete!")
    print(f"   Failed nodes: {', '.join([f'Node {n}' for n in nodes])}")
    print()
    print("You can now test your application's behavior with these nodes offline.")
    print()

    # Wait for user to restore access
    while True:
        response = input("Start Server Again (Grant Privileges) (Y/N): ").strip().upper()

        if response == 'Y':
            print()
            print("🟢 RESTORING NODE ACCESS...")
            print("-" * 60)

            for node in nodes:
                try:
                    grant_privileges(node)
                except Exception as e:
                    print(f"  [Node {node}] Failed to grant privileges: {str(e)}")

            print("-" * 60)
            print()
            print("✅ Node recovery complete!")
            print(f"   Recovered nodes: {', '.join([f'Node {n}' for n in nodes])}")
            print()
            print("All specified nodes are now back online.")
            break

        elif response == 'N':
            print("Nodes remain offline. Enter 'Y' when ready to restore access.")

        else:
            print("Invalid input. Please enter 'Y' or 'N'.")


if __name__ == "__main__":
    main()


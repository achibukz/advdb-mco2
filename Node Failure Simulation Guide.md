# Node Failure Simulation Guide

This guide explains how to use `fail-start.py` to simulate node failures and test your distributed database system's behavior during various failure scenarios.

## Overview

The `fail-start.py` script simulates node failures by revoking database privileges from the `user` account, effectively making the node inaccessible for read/write operations. This allows you to test how your system handles:

- Write transaction failures when target nodes are down
- Node recovery and transaction replay
- Multi-node failures
- Data consistency across nodes

## Prerequisites

- Python 3.x installed
- MySQL connector installed (`pip install mysql-connector-python`)
- Access to the database with root/admin privileges
- All database nodes running (before simulating failure)

## Usage

### Basic Syntax

```bash
python fail_start.py <node_number> [<node_number> ...]
```

### Arguments

- `<node_number>`: One or more node numbers (1, 2, or 3) to simulate failure
- Multiple nodes can be specified separated by spaces

### Examples

```bash
# Fail a single node
python fail_start.py 1

# Fail multiple nodes
python fail_start.py 2 3

# Fail all nodes
python fail_start.py 1 2 3
```

## Test Cases

### Case 1: Node 1 Failure (Write from Node 2/3 to Node 1 Fails)

**Scenario:** Node 1 goes offline. Write transactions from Node 2 or Node 3 targeting Node 1 should fail.

**Steps:**
1. Run the failure simulation:
   ```bash
   python fail_start.py 1
   ```

2. The script will:
   - Kill all active sessions for `user` on Node 1 (local environment only)
   - Revoke all privileges for `user` on `node1_db`
   - Display status: `❌ NODE 1 IS NOW OFFLINE (simulated)`

3. Test your application:
   - Try to write data to Node 1 from Node 2 or Node 3
   - Verify that the transaction fails gracefully
   - Check that error handling works correctly

4. Keep the script running (do NOT enter 'Y' yet)

---

### Case 2: Node 1 Recovery (Node 1 Comes Back Online)

**Scenario:** Continuation of Case 1. Node 1 recovers and comes back online.

**Steps:**
1. In the same terminal where `fail-start.py` is running, enter `Y` when prompted:
   ```
   Start Server Again (Grant Privileges) (Y/N): Y
   ```

2. The script will:
   - Grant all privileges back to `user` on `node1_db`
   - Display status: `✅ NODE 1 IS NOW ONLINE (recovered)`
   - Exit the script

3. Test your application:
   - Verify that Node 1 is accessible again
   - Check if pending transactions are replayed/synchronized
   - Ensure data consistency across all nodes

---

### Case 3: Nodes 2 & 3 Failure (Write from Node 1 to Node 2/3 Fails)

**Scenario:** Nodes 2 and 3 go offline. Write transactions from Node 1 targeting Node 2 or Node 3 should fail.

**Steps:**
1. Run the failure simulation:
   ```bash
   python fail_start.py 2 3
   ```

2. The script will:
   - Kill all active sessions for `user` on Nodes 2 and 3 (local environment only)
   - Revoke all privileges for `user` on `node2_db` and `node3_db`
   - Display status: `❌ NODE 2 IS NOW OFFLINE (simulated)`
   - Display status: `❌ NODE 3 IS NOW OFFLINE (simulated)`

3. Test your application:
   - Try to write data to Node 2 or Node 3 from Node 1
   - Verify that the transactions fail gracefully
   - Check that error handling works correctly
   - Ensure Node 1 can still operate independently

4. Keep the script running (do NOT enter 'Y' yet)

---

### Case 4: Nodes 2 & 3 Recovery (Nodes 2/3 Come Back Online)

**Scenario:** Continuation of Case 3. Nodes 2 and 3 recover and come back online.

**Steps:**
1. In the same terminal where `fail-start.py` is running, enter `Y` when prompted:
   ```
   Start Server Again (Grant Privileges) (Y/N): Y
   ```

2. The script will:
   - Grant all privileges back to `user` on `node2_db` and `node3_db`
   - Display status: `✅ NODE 2 IS NOW ONLINE (recovered)`
   - Display status: `✅ NODE 3 IS NOW ONLINE (recovered)`
   - Exit the script

3. Test your application:
   - Verify that Nodes 2 and 3 are accessible again
   - Check if pending transactions are replayed/synchronized
   - Ensure data consistency across all nodes
   - Verify that all nodes can communicate with each other

---

## Script Behavior

### Failure Simulation (Account Lock)

When you run `fail-start.py <node_number>`, the script:

**For Local Environment:**

1. **Locks the user account** to prevent any new connections:
   ```sql
   ALTER USER 'user'@'%' ACCOUNT LOCK;
   FLUSH PRIVILEGES;
   ```

2. **Kills all existing connections** for the user:
   ```sql
   SET @kills = (
       SELECT GROUP_CONCAT(CONCAT('KILL ', id, ';') SEPARATOR ' ')
       FROM information_schema.processlist
       WHERE user = 'user'
   );
   PREPARE stmt FROM @kills;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;
   ```

3. **Waits for recovery command** (keeps script running)

**For Cloud Environment:**

1. **Revokes privileges** for the specified node database:
   ```sql
   REVOKE ALL PRIVILEGES ON node<X>_db.* FROM 'user'@'%';
   FLUSH PRIVILEGES;
   ```

2. **Waits for recovery command** (keeps script running)

### Recovery (Account Unlock)

When you enter `Y` at the prompt, the script:

**For Local Environment:**

1. **Unlocks the user account** to restore access:
   ```sql
   ALTER USER 'user'@'%' ACCOUNT UNLOCK;
   FLUSH PRIVILEGES;
   ```

2. **Exits the script** (nodes are back online)

**For Cloud Environment:**

1. **Grants privileges back** to the user:
   ```sql
   GRANT ALL PRIVILEGES ON node<X>_db.* TO 'user'@'%';
   FLUSH PRIVILEGES;
   ```

2. **Exits the script** (nodes are back online)

---

## Environment Support

### Local Environment
- **Session Termination:** Active connections are killed before revoking privileges
- **Admin Access:** Uses `tester` user with `testpass` password for privilege management
- **Ports:** Node 1 (3306), Node 2 (3307), Node 3 (3308)

### Cloud SQL Environment
- **Session Termination:** Skipped (not required for Cloud SQL)
- **Admin Access:** Uses `tester` user with `Testpass123!` password for privilege management
- **Privilege Management:** Same SQL commands, but executed on cloud instances

The script automatically detects the environment based on `USE_CLOUD_SQL` setting in `db_config.py`.

**Note:** The `tester` user is an admin user with all privileges, created for consistency across both local and cloud deployments. This user is used instead of the `root` user because Cloud SQL sometimes restricts root user access.

---

## Important Notes

1. **Admin Access Required:** The script requires admin database access (`tester` user) to lock/unlock user accounts or manage privileges.

2. **Account Locking (Local):** In local environments, the script uses MySQL's `ACCOUNT LOCK` feature to prevent any connections from the `user` account, simulating a complete node failure. This is more effective than just revoking privileges.

3. **Active Connections:** After locking the account, all active user sessions are terminated to ensure immediate disconnection.

4. **Graceful Testing:** The script provides a controlled way to test failure scenarios without actually stopping database servers.

5. **Data Safety:** This script only affects user account status and privileges, not data. All data remains intact during failure simulation.

6. **Recovery Required:** Always remember to enter `Y` to restore access after testing, or your application will remain unable to access the simulated-failed nodes.

7. **No Hardcoding:** The script dynamically determines the database name based on the node number argument:
   - Node 1 → `node1_db`
   - Node 2 → `node2_db`
   - Node 3 → `node3_db`

8. **Environment Differences:** 
   - **Local:** Uses `ACCOUNT LOCK/UNLOCK` (more reliable)
   - **Cloud:** Uses `REVOKE/GRANT PRIVILEGES` (account lock may not work on Cloud SQL)

---

## Troubleshooting

### "Access Denied" Error
- Ensure the `tester` user has admin/all privileges on the database
- Check that tester password is correct in `.env` file:
  - Local: `TESTER_LOCAL_DB_PASSWORD=testpass`
  - Cloud: `TESTER_CLOUD_DB_PASSWORD=Testpass123!`

### "Cannot Connect" Error
- Verify that the database servers are running
- Check network connectivity
- Verify port numbers in configuration

### Privileges Not Restoring
- Manually grant privileges using MySQL client (log in as tester):
  ```sql
  GRANT ALL PRIVILEGES ON node<X>_db.* TO 'user'@'%';
  FLUSH PRIVILEGES;
  ```

### Script Hangs
- Press `Ctrl+C` to interrupt
- Manually check and restore privileges if needed

---

## Example Workflow

Here's a complete workflow for testing all four cases:

```bash
# Case 1: Fail Node 1
cd python
python fail_start.py 1
# Test your app with Node 1 offline
# Enter 'Y' to recover (Case 2)

# Case 3: Fail Nodes 2 and 3
python fail_start.py 2 3
# Test your app with Nodes 2 & 3 offline
# Enter 'Y' to recover (Case 4)
```
---

## Additional Test Scenarios

Beyond the four main test cases, you can simulate other scenarios:

### Single Node Failure (Any Node)
```bash
python fail_start.py 1  # Test Case 1
python fail_start.py 2  # Alternative scenario
python fail_start.py 3  # Alternative scenario
```

### Two Node Failure (Various Combinations)
```bash
python fail_start.py 1 2  # Nodes 1 and 2 fail
python fail_start.py 1 3  # Nodes 1 and 3 fail
python fail_start.py 2 3  # Test Case 3 (Nodes 2 and 3)
```

### Complete System Failure
```bash
python fail_start.py 1 2 3  # All nodes fail
```

This allows comprehensive testing of your distributed system's fault tolerance and recovery mechanisms.


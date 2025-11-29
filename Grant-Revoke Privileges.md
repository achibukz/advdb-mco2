# MySQL Grant and Revoke Privileges

## Cloud Environment

### Granting Privileges

```mysql
GRANT ALL PRIVILEGES ON node1_db.* TO 'user'@'%';
FLUSH PRIVILEGES;
```
This is a sample of granting privileges to User `user` on database `node1_db`.

### Revoking Privileges

```mysql
REVOKE ALL PRIVILEGES ON node1_db.* FROM 'user'@'%';
FLUSH PRIVILEGES;
```

## Local Environment Only

> **Note:** The following commands below are only required for LOCAL environments, not for Cloud.

### Show Active Connections

```mysql
SHOW PROCESSLIST;
```

If User `user` has active connections, you need to terminate them before revoking privileges.

### Kill User Sessions

#### Automated Method

To automatically kill all connections for a specific user, use a prepared statement with `CONCAT` and `GROUP_CONCAT`:

```mysql
-- Generate and execute KILL commands for all connections from 'user'
SET @kills = (
    SELECT GROUP_CONCAT(CONCAT('KILL ', id, ';') SEPARATOR ' ')
    FROM information_schema.processlist
    WHERE user = 'user'
);

PREPARE stmt FROM @kills;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Now revoke privileges
REVOKE ALL PRIVILEGES ON node1_db.* FROM 'user'@'%';
FLUSH PRIVILEGES;
```
Grant back privileges if needed:

```mysql
GRANT ALL PRIVILEGES ON node1_db.* TO 'user'@'%';
FLUSH PRIVILEGES;
```
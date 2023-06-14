# Xlinectl

This crate provides a command line client for Xline.

## Global Options
- endpoints <SERVER_NAME ADDR>... -- Set Xline endpoints, which are seperated by ','
    ```bash
    # connect to servers with specifc addresses
    ./xlinectl --endpoints "server0 10.0.0.1:2379, server1 10.0.0.2:2379, server2 10.0.0.3:2379"
    ```
- user <USERNAME[:PASSWD]> -- The name of the user, this provide a shorthand to set password
    ```bash
    # connect to servers using user `foo` with password `bar`
    ./xlinectl --user foo:bar
    ```
- password <PASSWD> -- The password of the user, should exist if password not set in `--user`
    ```bash
    # connect to servers using user `foo` with password `bar`
    ./xlinectl --user foo --password bar
    ```
- wait_synced_timeout <TIMEOUT> -- The timeout for Curp client waiting synced(in secs) [default: 2]
- propose_timeout <TIMEOUT> -- The timeout for Curp client proposing request(in secs) [default: 1]
- retry_timeout <TIMEOUT> -- The timeout for Curp client retry interval(in millis) [default: 50]

## Output Format

All command output will first print the response header, and then print the response fields

## Key-value commands

### PUT
Puts the given key-value into the store. If key already holds a value, it is overwritten.

#### Usage

```bash
put [options] <key> <value>
```

#### Options
- lease -- lease ID to attach to the key [default: 0]
- prev_kv --  return the previous key-value pair before modification
- ignore_value --  updates the key using its current value
- ignore_lease --  updates the key using its current lease

#### Examples

```bash
# put key `foo` with value `bar` and attach the lease `123` to the key
./xlinectl put foo bar --lease=123

# detach the lease by updating with empty lease
./xlinectl put foo --ignore-value
```

```bash
# same as above
./xlienctl put foo bar --lease=123

# use existing lease
./xlinectl put foo bar1 --ignore-lease
```

### GET
Gets the key or a range of keys

#### Usage

```bash
get [options] <key> [range_end]
```

#### Options
- consistency -- Linearizable(l) or Serializable(s) [default: l]
- order -- Order of results; ASCEND or DESCEND
- sort_by -- Sort target; CREATE, KEY, MODIFY, VALUE, or VERSION
- limit -- Maximum number of results [default: 0]
- prefix -- Get keys with matching prefix (conflicts with range_end)
- from_key -- Get keys that are greater than or equal to the given key using byte compare (conflicts with prefix and range_end)
- rev -- Specify the kv revision [default: 0]
- keys_only -- Get only the keys
- count_only -- Get only the count (conflicts with keys_only)

#### Examples

```bash
# get the key `foo`
./xlinectl get foo
```

```bash
# get all keys prefixed with `foo`
./xlinectl get foo --prefix
```

```bash
# get all keys prefixed with `foo` sort in descend order
./xlinectl get foo --prefix --order DESCEND
```

```bash
# get all keys from `foo` to `foo3`
./xlinectl get foo foo3
```

### DELETE
Deletes the key or a range of keys

#### Usage

```bash
delete [options] <key> [range_end]
```

#### Options
- prefix -- delete keys with matching prefix
- prev_kv -- return deleted key-value pairs
- from_key -- delete keys that are greater than or equal to the given key using byte compare

#### Examples

```bash
# delete the key `foo`
./xlinectl delete foo
```

```bash
# delete all keys prefixed with `foo`
./xlinectl delete foo --prefix
```

### TXN
### WATCH
### LEASE
### LEASE GRANT
Create a lease with a given TTL

#### Usage
```bash
grant <ttl>
```

#### Examples
```bash
# create a new lease with 100s TTL
./xlinectl lease grant 100
```

### LEASE REVOKE
Revoke a lease

#### Usage

```bash
revoke <leaseId>
```

#### Examples
```bash
# Revoke a lease with leaseId 123
./xlinectl lease revoke 123
```

### LEASE TIMETOLIVE
Get lease ttl information

#### Usage

```bash
timetolive <leaseId>
```

#### Examples

```bash
# Get the TTL of a lease with leaseId 123
./xlinectl lease timetolive 123
```

### LEASE LIST
List all active leases

#### Usage

```bash
list
```

#### Examples
```bash
# List all leases
./xlinectl lease list
```

### LEASE KEEP-ALIVE
lease keep alive periodically

#### Usage

```bash
keep_alive [options] <leaseId>
```

#### Options
- once -- keep alive once

#### Examples

```bash
# keep alive forever with leaseId 123 until the process receive `SIGINT`
./xlinectl lease keep_alive 123
```

```bash
# renew the lease ttl only once
./xlinectl lease keep_alive 123 --once
```

## Cluster maintenance commands

### SNAPSHOT
Get snapshots of xline nodes

### SNAPSHOT SAVE
Save snapshot to file

#### Usage

```bash
snapshot save <filename>
```
#### Examples

```bash
# Save snapshot to /tmp/foo
./xlinectl snapshot save /tmp/foo
```

## Concurrency commands

### LOCK
Acquire a lock, which will return a unique key that exists so long as the lock is held.

#### Usage

```bash
lock <lockname>
```

#### Examples

```bash
# Hold a lock named foo until `SIGINT` is received
./xlinectl lock foo
```
## Authentication commands
### AUTH
Manage authentication

### AUTH ENABLE
Enable authentication

#### Usage

```bash
auth enable
```

#### Examples
```bash
# Enable authentication
./xlinectl auth enable
```

### AUTH DISABLE
Disable authentication

#### Usage

```bash
auth disable
```

#### Examples
```bash
# Disable authentication
./xlinectl auth disable
```

### AUTH STATUS
Status of authentication

#### Usage

```bash
auth status
```

#### Examples
```bash
# Check the status of authentication
./xlinectl auth status
```

### ROLE
Role related commands

### ROLE ADD
Create a new role

#### Usage

```bash
add <name>
```

#### Examples

```bash
# add a new role named 'foo'
./xlinectl --user=root:root role add foo
```

### ROLE GET
List role information

#### Usage

```bash
get <name>
```

#### Examples

```bash
# Get role named 'foo'
./xlinectl --user=root:root role get foo
```

### ROLE DELETE
Delete a role

#### Usage

```bash
delete <name>
```

#### Examples
```bash
# delete the role named `foo`
./xlinectl --user=root:root role delete foo
```

### ROLE LIST
List all roles

#### Usage

```bash
list
```

#### Examples

```bash
# list all roles
./xlinectl --user=root:root role list
```

### ROLE GRANT-PERMISSION
Grant permission to a role, including READ, WRITE or READWRITE

#### Usage

```bash
grant_perm [options] <name> <perm_type> <key> [range_end]
```

#### Options
- prefix -- Get keys with matching prefix
- from_key -- Get keys that are greater than or equal to the given key using byte compare (conflicts with `range_end`)

#### Examples

```bash
# Grant read permission to role 'foo' for key 'mykey' with read permission
./xlinectl --user=root:root grant_perm foo READ mykey
```

### ROLE REVOKE-PERMISSION
Revoke permission from a role

#### Usage

```bash
revoke_perm <name> <key> [range_end]
```

#### Examples

```bash
# Revoke permission from role 'foo' for the range from mykey to mykey2
./xlinectl --user=root:root revoke_perm foo mykey mykey2
```

### USER
### USER ADD
Add a new user

#### Usage

```bash
add [options] <name> [password]
```

#### Options
- no_password -- Create without password

#### Examples
```bash
# Add a new user with a specified password
./xlinectl --user=root:root user add foo bar

# Add a new user without a password
./xlinectl --user=root:root user add foo1 --no_password
```

### USER GET
Get a new user

#### Usage

```bash
get [options] <name>
```

#### Options
- detail -- Show permissions of roles granted to the user

#### Examples
```bash
# Get a user named `foo`
./xlinectl --user=root:root user get foo

# Get detailed information about a user named `foo`
./xlinectl --user=root:root user get foo --detail
```

### USER LIST
List all users

#### Usage

```bash
list
```

#### Examples
```bash
# List all users
./xlinectl --user=root:root user list
```

### USER PASSWD
Change the password of a user

#### Usage

```bash
passwd <name> <password>
```

#### Examples

```bash
# Change the password of user `foo` to `bar`
./xlinectl --user=root:root user passwd foo bar
```

### USER GRANT-ROLE
Grant role to a user

#### Usage

```bash
grant_role <name> <role>
```

#### Examples

```bash
# Grant `bar` role to the user `foo`
./xlinectl --user=root:root revoke_role foo bar
```

### USER REVOKE-ROLE
Revoke role from a user

#### Usage

```bash
revoke_role <name> <role>
```

#### Examples

```bash
# Revoke 'bar' role from the user 'foo'
./xlinectl --user=root:root revoke_role foo bar
```

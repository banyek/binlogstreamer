# binlogstreamer
Tool for creating streamed binlog backup with mysqlbinlog utility

## Usage

    binlogstreamer [-cfg binlogstreamer.cfg]

## Config file

The application uses the following config file

    mysqlhost   = mysqlhost
    mysqluser   = user.with.replication.client.rights
    mysqlpass   = password
    mysqldb     = db.to.connect
    binlogdir   = /path/to/binlogs
    mysqlbinlog = /path/to/mysqlbinlog
    keep_days   = 0 # if zero, no binlogs will be cleaned up

## How it works

At startup, binlogstreamer compares the names and sizes returned by
`SHOW BINARY LOGS` with the backups in `binlogdir`. It starts `mysqlbinlog` at
the first missing or size-mismatched file. If all files match, it starts at
the server's latest log so it can continue receiving new events.

Downloads go into a hidden `.binlogstreamer-*` subdirectory of `binlogdir`.
When the next log appears, the previous download can be published atomically.
A complete local copy of the same size is kept. Retention applies to published
logs older than the file currently being streamed.

When the stream stops, binlogstreamer checks the latest staged file against
the server inventory and verifies it with `mysqlbinlog` before publishing it.
It retries this check at startup if the server was unavailable. A partial
file stays in staging while the missing data is downloaded again from the
server. If the server has already purged the log, its staged copy is kept
for manual recovery.

MySQL decrypts encrypted binary logs when it streams them. The local copies
are plaintext, and binlogstreamer excludes the server's encryption header
when comparing file sizes.

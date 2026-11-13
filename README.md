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

If the stream stops, the latest staged log stays in its staging directory.
It may contain partial data. A restart starts a new download based on the
published backups and leaves earlier staging directories available for manual
recovery or removal. MySQL logs reported as encrypted cause startup to fail
because their server file sizes differ from the streamed output.

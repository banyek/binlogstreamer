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

## How it is works

After application is started it connects to the remote mysql instance, and with 
    
    mysql> show binary logs;
    
gets the list of the binary logs which exists on the server.
It also takes a look to the local directory where the binlogs will be kept, and compares all the locally existing binlogs with the remotely available binlogs. The script compares the filename and the file size of the logs, and if they are not the same, it will rename the local file to <filename>_incomplete.
The script will invoke mysqlbinlog utility to stream the binlogs from the remote server from the first one which is not existing locally.


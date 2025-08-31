It is important to set the `LD_LIBRARY_PATH` environment variable to include the path to Hadoop's native libraries. 
This ensures that tests do not use DebugFS which spams chmod and can lead often to defunct processes.
root@bdb195e101ca:~/mosaic# export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
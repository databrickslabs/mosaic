It is important to set the `LD_LIBRARY_PATH` environment variable to include the path to Hadoop's native libraries. 
This ensures that tests do not use DebugFS which spams chmod and can lead often to defunct processes.
root@bdb195e101ca:~/mosaic# export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH

To use RasterDebugger in cli while in debug mode please run the following in your terminal to enable UTF-8 support:
apt-get update && apt-get install -y locales
locale-gen en_GB.UTF-8
update-locale LANG=en_GB.UTF-8
export LANG=en_GB.UTF-8 LC_ALL=en_GB.UTF-8
export TERM=xterm-256color COLORTERM=truecolor

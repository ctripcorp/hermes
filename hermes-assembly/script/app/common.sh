#!/bin/bash

# directories
SYSOUT_LOG=$LOG_PATH/sysout.log
OP_LOG=$LOG_PATH/op.log
mkdir -p $LOG_PATH

# write to op log
log_op() {
	timestamp=$(date +"%F %T")
	echo "[$timestamp] $@" >> $OP_LOG
}

# check whether hostname resolvable
hostname_ok=true
set +e
hostname=$(hostname)
ping -c 1 -w 2 -q $hostname >/dev/null 2>&1
if [ $? -ne 0 ];then
	hostname_ok=false
fi
set -e


JAVA_CMD=/usr/bin/java

if [ $hostname_ok == true ];then
	log_op "jmx enabled"
	JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT \
            -Dcom.sun.management.jmxremote.authenticate=false \
            -Dcom.sun.management.jmxremote.ssl=false "
else
	log_op "jmx disabled due to unresolvable hostname"
fi

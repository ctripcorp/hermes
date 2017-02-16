#!/bin/sh

set -e
set -u

cd `dirname $0`

mkdir -p /opt/logs/hermes/

ENV_FILE="./env.sh"
. "${ENV_FILE}"

LOG_PATH="../"
LOG_FILE="run.log"

SERVER_DRIVER=com.ctrip.hermes.example.ConsumerExample

SERVER_HOME=..

CLASSPATH=${SERVER_HOME}

CLASSPATH="${CLASSPATH}":"${CLASSPATH}/lib/*":"${SERVER_HOME}/*":"${SERVER_HOME}/conf"
for i in "${SERVER_HOME}"/*.jar; do
   CLASSPATH="${CLASSPATH}":"${i}"
done



start() {
    ensure_not_started
	if [ ! -d "${LOG_PATH}" ]; then
        mkdir "${LOG_PATH}"
    fi
    exec nohup java ${JAVA_OPTS} -classpath ${CLASSPATH} ${SERVER_DRIVER} > "${LOG_PATH}/${LOG_FILE}" 2>&1 &
    # exec java ${JAVA_OPTS} -classpath ${CLASSPATH} ${SERVER_DRIVER} &
    echo $!
    echo $?
    echo "BrokerServer Started!"
}

stop(){
    serverPID=`jps | grep ConsumerExample | awk '{print $1;" "}'`
    if [ "${serverPID}" == "" ]; then
        echo "no HermesRestServer is running"
    else
        kill -9 ${serverPID}
        echo "HermesRestServer Stopped"
    fi
}

ensure_not_started() {
	serverPID=`jps -lvm | grep ConsumerExample | awk '{print $1}'`
    if [ "${serverPID}" != "" ]; then
        echo "BrokerServer is already running"
        exit 1
    fi
}

_start() {
    java ${JAVA_OPTS} -classpath ${CLASSPATH} ${SERVER_DRIVER}
}


case "$1" in
    start)
        start
	    ;;
	stop)
	    stop
	    ;;
    *)
        echo "Usage: $0 {start|stop}"
   	    exit 1;
	    ;;
esac
exit 0

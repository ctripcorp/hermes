#!/bin/bash

set -e
set -u

cd `dirname $0`
BIN_DIR=`pwd`
APP_DIR=`dirname $BIN_DIR`
CONTEXT_DIR=$APP_DIR/context
TOMCAT_DIR=$BIN_DIR/../tomcat


if [ $# -lt 1 ];then
	echo "usage: startup.sh start|stop [port] [debug] [T<stop-timeout>]"
	exit 1
fi

# default config, can be override in env.sh
STOP_TIMEOUT=10
JAVA_OPTS=""

. "./env.sh"
. "./common.sh"

SERVER_HOME=..
WAR=$(ls ../*.war)
STOP_KEY=hermes4EVER
DEBUG_OPT="-Xdebug -agentlib:jdwp=transport=dt_socket,address=8787,server=y,suspend=n"

if [ ! -f $JAVA_CMD ];then
	log_op "$JAVA_CMD not found!"
	exit 1
fi

if [ ! -f $CONTEXT_DIR/WEB-INF/web.xml ];then
	log_op "Unzip war into $CONTEXT_DIR ..."
	unzip -q -d $CONTEXT_DIR $WAR
fi

port=${2:-${HTTP_PORT:-8080}}

can_sudo=false
set +e
sudo -n ls >/dev/null 2>&1
if [ $? -eq 0 ];then
	can_sudo=true
fi
set -e

enable_debug() {
	echo "Enable debug mode."
	JAVA_OPTS="${JAVA_OPTS} $DEBUG_OPT"
	export JAVA_OPTS=${JAVA_OPTS}
}
			
sudo=""

if [[ $1=="start" ]]; then
	if [[ ${!#} == "debug" ]];then
		enable_debug
	fi
	if [[ $# -ge 2 ]];then
		num_regex='^[0-9]+$'
		if [[ $2 =~ $num_regex ]];then
			port=$2
		elif [[ $2 != "debug" ]];then
			log_op "$2 is not a valid port, use default: 8080"
		fi
		if [ $port -lt 1024 ];then
			if [ $can_sudo == false ];then
				log_op "[ERROR] Attemp to start tomcat at port $port but without passwordless sudo"
				exit 1
			fi
			sudo="sudo"
		fi
	fi
fi

if [[ $1=="stop" ]]; then
	timeout_regex='^T[0-9]+$'
	if [[ ${!#} =~ $timeout_regex ]];then
		echo "Set stop timeout to ${!#:1}"
		STOP_TIMEOUT=${!#:1}
	fi
fi

backup_sysout_log(){
	set +e
	if [ -f "${SYSOUT_LOG}" ]; then
		echo "Backup $SYSOUT_LOG ..."
		ARCH_DIR=$LOG_PATH/`date "+%Y-%m"`
		SUFFIX=`date "+%Y-%m-%d.%H.%M.%S"`.gz
		gzip -S .$SUFFIX $SYSOUT_LOG
		if [ ! -d "${ARCH_DIR}" ]; then
			mkdir "${ARCH_DIR}"
		fi
		thissudo=""
		if [ $can_sudo == true ];then
    		thissudo="sudo"
    	fi

		$thissudo mv $SYSOUT_LOG.$SUFFIX $ARCH_DIR
	fi
	set -e
}

start() {
    ensure_not_started
	if [ ! -d "${LOG_PATH}" ]; then
        mkdir "${LOG_PATH}"
    fi
    log_op $(pwd)
    backup_sysout_log
    BUILD_ID=jenkinsDontKillMe $sudo nohup $JAVA_CMD ${JAVA_OPTS} -DshutdownPort=$STOP_PORT -DshutdownString=$STOP_KEY -classpath "$TOMCAT_DIR/*" com.ctrip.hermes.tomcat.HermesTomcat $CONTEXT_DIR $port > $SYSOUT_LOG 2>&1 &
    log_op "PID $$"
    log_op "Instance Started!"
}

stop(){
    serverPID=$(find_pid)
    if [ "${serverPID}" == "" ]; then
    	echo "No Instance Is Running"
        log_op "No Instance Is Running"
    else
		if [ $can_sudo == true ];then
    		sudo="sudo"
    	fi
    	echo "Stop port is $STOP_PORT"
    	set +e
    	$sudo echo $STOP_KEY | nc 127.0.0.1 $STOP_PORT
    	stop_success=$?
    	set -e
    	if [[ ! $stop_success == 0 ]]; then
    		echo "Shutdown with stop port failed, try to kill it... "
    		kill_all
    	else
			wait_or_kill        
    	fi       
        log_op "Instance Stopped"
    fi
}

wait_or_kill() {
	pid=$(find_pid)
	for (( i=$STOP_TIMEOUT; pid>1024 && i>0; i--)); do
		sleep 1 &
		printf ">>> Waiting $i seconds for gracefully shutdown... \r"
		wait
		pid=$(find_pid)
		if [[ "$pid" -eq "" ]];then
			printf "\n"
			pid=-1
		fi
	done

	if [[ $i -le 0 ]]; then
		printf "\nWait for gracefully shutdown failed. Will kill the process.\n"
		kill_all
    	echo "Process $pid is shutdown."
    else
    	echo "Gracefully shutdown success!"
	fi
}

kill_all(){
	ps ax | grep java | grep tomcat | grep $APP_ID | awk '{print $1}' | xargs $sudo kill -9
}




ensure_not_started() {
	serverPID=$(find_pid)
    if [ "${serverPID}" != "" ]; then
        log_op "Instance Already Running"
        exit 1
    fi
}

find_pid() {
	echo $(ps ax | grep java | grep tomcat | grep $APP_ID | awk '{print $1}' | head -n1)
}


case "$1" in
    start)
        start
	    ;;
	stop)
	    stop
	    ;;
	check_pid)
	    check_pid
	    ;;
    *)
        echo "Usage: $0 {start|check_pid}"
   	    exit 1;
	    ;;
esac
exit 0

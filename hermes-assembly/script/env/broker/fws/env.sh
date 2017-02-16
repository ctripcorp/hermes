#!/bin/bash

LOG_PATH=/opt/logs/100003804/
JMX_PORT=8301
STOP_PORT=9301
STOP_TIMEOUT=90
APP_ID=100003804
# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -Xms4g \
            -Xmx4g \
            -Xmn3g \
            -Dtomcat.log=$LOG_PATH/tomcat \
            -XX:PermSize=256m \
            -XX:MaxPermSize=256m \
            -XX:MaxDirectMemorySize=1g \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -XX:+PrintReferenceGC \
            -Xloggc:$LOG_PATH/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:ParallelGCThreads=1 \
            -XX:SurvivorRatio=8 \
            -XX:-UseAdaptiveSizePolicy \
            -XX:+UseParNewGC \
            -XX:+UseConcMarkSweepGC \
            -XX:+UseCMSInitiatingOccupancyOnly \
            -XX:+ScavengeBeforeFullGC \
            -XX:+CMSParallelRemarkEnabled \
            -XX:CMSInitiatingOccupancyFraction=80 \
            -XX:+CMSClassUnloadingEnabled \
            -XX:+CMSScavengeBeforeRemark \
            -XX:+ExplicitGCInvokesConcurrent \
            -XX:ParallelCMSThreads=2 \
            -Dcom.mchange.v2.log.MLog=com.mchange.v2.log.FallbackMLog \
            -Dcom.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL=WARNING \
            -Dmysql.initialReusablePacketSize=1048576 \
            -Dmysql.initialSharedSendPacketSize=1048576 \
            -Dmysql.maxSharedSendPacketSize=1048576 \
            -Dmysql.maxReuseablePacketSize=1048576 \
            -XX:+UnlockCommercialFeatures \
            -XX:+FlightRecorder \
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_PATH
            "
export JAVA_OPTS=${JAVA_OPTS}

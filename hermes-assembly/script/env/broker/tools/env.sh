#!/bin/bash

LOG_PATH=/opt/logs/100003804/
JMX_PORT=8301
STOP_PORT=9301
STOP_TIMEOUT=90
APP_ID=100003804
# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -Xms15g \
            -Xmx15g \
            -Xmn10g \
            -Dtomcat.log=$LOG_PATH/tomcat \
            -XX:PermSize=256m \
            -XX:MaxPermSize=256m \
            -XX:MaxDirectMemorySize=4g \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -XX:+PrintReferenceGC \
            -Xloggc:$LOG_PATH/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:ParallelGCThreads=4 \
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
            -XX:ParallelCMSThreads=8 \
            -Dcom.mchange.v2.log.MLog=com.mchange.v2.log.FallbackMLog \
            -Dcom.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL=WARNING \
            -Dmysql.initialReusablePacketSize=2621440 \
            -Dmysql.initialSharedSendPacketSize=2621440 \
            -Dmysql.maxSharedSendPacketSize=2621440 \
            -Dmysql.maxReuseablePacketSize=2621440 \
            -XX:+UnlockCommercialFeatures \
            -XX:+FlightRecorder \
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_PATH
            "
export JAVA_OPTS=${JAVA_OPTS}

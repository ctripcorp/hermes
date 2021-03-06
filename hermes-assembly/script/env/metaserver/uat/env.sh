#!/bin/bash

LOG_PATH=/opt/logs/100003805/
JMX_PORT=8304
STOP_PORT=9304
APP_ID=100003805
# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -DmaxThreads=200 \
            -DmaxQueueSize=200 \
            -Dtomcat.log=$LOG_PATH/tomcat \
            -Xms4g \
            -Xmx4g \
            -Xmn3g \
            -XX:PermSize=256m \
            -XX:MaxPermSize=256m \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -XX:+PrintReferenceGC \
            -Xloggc:$LOG_PATH/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:ParallelGCThreads=2 \
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
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_PATH
            "
export JAVA_OPTS=${JAVA_OPTS}

#!/bin/bash

LOG_PATH=/opt/logs/100003805/
JMX_PORT=8304
STOP_PORT=9304
APP_ID=100003805
# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -DmaxThreads=1500 \
            -Dtomcat.log=$LOG_PATH/tomcat \
            -Xms12g \
            -Xmx12g \
            -Xmn10g \
            -XX:PermSize=256m \
            -XX:MaxPermSize=256m \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -XX:+PrintReferenceGC \
            -Xloggc:$LOG_PATH/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:ParallelGCThreads=4 \
            -XX:SurvivorRatio=18 \
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
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_PATH
            "
export JAVA_OPTS=${JAVA_OPTS}

#!/bin/bash
ANALYTIC_NODE_IP=$1
CONFIG_FILE_PATH=$2
CONFIG_FILE_NAME=$3
VERSION_RETAIN_UPTO_DATE=$4
DURATION_TO_RETAIN=$5

VACUUMING_USERNAME=cassandra
VACUUMING_PWD=cassandra

MASTER_NODE_IP=$(dse client-tool -u $VACUUMING_USERNAME -p $VACUUMING_PWD spark leader-address | grep -Po '\d+\.\d+\.\d+\.\d+')

dse -u $VACUUMING_USERNAME -p $VACUUMING_PWD spark-submit \
    --master dse://$MASTER_NODE_IP \
    --deploy-mode cluster \
    --class com.kronos.kpifrm.vacuuming.cassandra.CleanUp \
    --conf spark.kronos.config=$CONFIG_FILE_PATH/$CONFIG_FILE_NAME \
    --jars /jars/kronos.cleanup-assembly-0.0.1.jar \
    --files=$CONFIG_FILE_PATH/$CONFIG_FILE_NAME \
    /jars/kronos.cleanup-assembly-0.0.1.jar \
    $ANALYTIC_NODE_IP \
    $VACUUMING_USERNAME \
    $VACUUMING_PWD \
    $VERSION_RETAIN_UPTO_DATE \
    $DURATION_TO_RETAIN | tee tempfile

sleep 5
DRIVER_ID=$(cat tempfile | cut -d " " -f 2)
echo $DRIVER_ID
rm tempfile

DRIVER_STATE=$(dse -u $VACUUMING_USERNAME -p $VACUUMING_PWD spark-submit --status $DRIVER_ID | cut -d " " -f 4)
echo $DRIVER_STATE
while [ "$DRIVER_STATE" == "state=RUNNING," ]
do
    sleep 1m
    DRIVER_STATE=$(dse -u $VACUUMING_USERNAME -p $VACUUMING_PWD spark-submit --status $DRIVER_ID | cut -d " " -f 4)
    sleep 5
    echo $DRIVER_STATE
done
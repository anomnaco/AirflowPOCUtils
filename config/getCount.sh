#!/bin/bash
VACUUMING_USERNAME=cassandra
VACUUMING_PWD=cassandra

MASTER_NODE_IP=$(dse client-tool -u $VACUUMING_USERNAME -p $VACUUMING_PWD spark leader-address | grep -Po '\d+\.\d+\.\d+\.\d+')

dse -u $VACUUMING_USERNAME -p $VACUUMING_PWD spark \
    --master dse://$MASTER_NODE_IP \
    --deploy-mode client  \
    --jars /jars/kronos.cleanup-assembly-0.0.1.jar \
    --conf spark.kronos.config=config.yaml \
    --files=/app/config/config.yaml \
    /jars/kronos.cleanup-assembly-0.0.1.jar \
    -i /app/config/countTable.scala
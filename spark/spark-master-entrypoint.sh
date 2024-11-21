#!/bin/bash


/opt/spark/sbin/start-master.sh \
    --host 0.0.0.0 \
    --port 7077 \
    --webui-port 8080

tail -F /opt/spark/logs/spark*

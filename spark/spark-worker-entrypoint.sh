#!/bin/bash


/opt/spark/sbin/start-worker.sh spark://spark-master:7077

tail -F /opt/spark/logs/spark*

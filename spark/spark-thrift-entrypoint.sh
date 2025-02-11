#!/bin/bash


/opt/spark/sbin/start-thriftserver.sh --master spark://spark-master:7077

tail -F /opt/spark/logs/spark*

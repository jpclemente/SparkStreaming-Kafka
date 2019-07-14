# SparkStreaming-Kafka

## Overview
This is Streaming program that reads from a kafka topic and uses SparkStreaming to process the data and write into another topic.

## User guide

### Starting zookeeer kafka
First you need to start zookeeper and kafka server. Assuming that there is KAFKA_HOME environment variable, this commands should to the work:

$KAFKA_HOME/bin/zookeeper-server-start.sh config/zookeeper.properties

$KAFKA_HOME/bin/kafka-server-start.sh config/server.properties


### Starting kafka producer

python kafka_producer.py 0.6 1.3 test ./data/datatest.txt

### Starting the SparkStreaming programm

### Stoping zookeeer and kafka
Once you have finished, you should stop kafka and zookeeper:

$KAFKA_HOME/bin/zookeeper-server-stop.sh

$KAFKA_HOME/bin/kafka-server-stop.sh

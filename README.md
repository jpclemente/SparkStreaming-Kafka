# SparkStreaming-Kafka

## Overview
This is a streaming program that reads data from a kafka topic and uses Spark to process it and write it into another kafka topic.


## User guide

### Starting zookeeper and kafka
First of all, you need to start zookeeper and kafka servers. Assuming there is KAFKA_HOME environment variable, these commands should start them:

$KAFKA_HOME/bin/zookeeper-server-start.sh config/zookeeper.properties

$KAFKA_HOME/bin/kafka-server-start.sh config/server.properties


### Starting kafka producer
Now you can start sending messages to the kafka queue. This command sends te content of the given file line by line in intervals between 0.6 and 0.3 seconds to the "test" topic:

python kafka_producer.py 0.6 1.3 test ./data/occupancy_data.csv


### Running the Spark streaming program
Now that the data is in the kafka queue, you can lanch the jupyter-notebook, which reads from the queue, does the processing and writes the data into another kafka queue to read it again and print it on the console.


### Stoping zookeeper and kafka
Once you have finished, you should stop kafka and zookeeper servers:

$KAFKA_HOME/bin/zookeeper-server-stop.sh

$KAFKA_HOME/bin/kafka-server-stop.sh

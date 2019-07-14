#!/usr/bin/env bash

cd ~/Documents/SDPDII/kafka_2.11-2.1.0/ &

bin/zookeeper-server-start.sh config/zookeeper.properties &

bin/kafka-server-start.sh config/server.properties &

cd ~/Documents/SDPDIII/practica/ &

source activate pyspark &

python kafka_producer.py 0.6 1.3 test ./data/datatest.txt &&

bin/zookeeper-server-stop.sh &&

bin/kafka-server-stop.sh
#!/bin/sh

##########################################################################
################################ Important ###############################
##########################################################################
##  This script implements workarounds for the current Docker image of  ##
##  Apache Kafka from Confluent. Eventually, newer images will fix the  ##
##  issues found here, and this script will no longer be required.      ##
##########################################################################

# Workaround: Remove check for KAFKA_ZOOKEEPER_CONNECT parameter
sed -i '/KAFKA_ZOOKEEPER_CONNECT/d' /etc/confluent/docker/configure

# Workaround: Ignore cub zk-ready
sed -i 's/cub zk-ready/echo ignore zk-ready/' /etc/confluent/docker/ensure

# KRaft required: Format the storage directory with a new cluster ID
export KAFKA_CLUSTER_ID="p8fFEbKGQ22B6M_Da_vCBw"
echo "kafka-storage format --ignore-formatted -t $KAFKA_CLUSTER_ID -c /etc/kafka/kafka.properties" >> /etc/confluent/docker/ensure

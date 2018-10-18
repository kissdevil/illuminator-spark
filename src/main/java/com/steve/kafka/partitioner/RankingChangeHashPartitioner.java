package com.steve.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class RankingChangeHashPartitioner implements Partitioner {

  @Override
  public int partition(String topicName, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
    int numPartitions = cluster.partitionCountForTopic(topicName);
    return Math.abs(key.hashCode() % numPartitions);
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}



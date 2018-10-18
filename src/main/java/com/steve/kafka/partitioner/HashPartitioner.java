package com.steve.kafka.partitioner;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class HashPartitioner implements Partitioner {

  /*public HashPartitioner(VerifiableProperties verifiableProperties) {}

  @Override
  public int partition(Object key, int numPartitions) {
    if ((key instanceof Integer)) {
      return Math.abs(Integer.parseInt(key.toString())) % numPartitions;
    }
    return Math.abs(key.hashCode() % numPartitions);
  }*/

  @Override
  public int partition(String topicName, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
    int numPartitions = cluster.partitionCountForTopic(topicName);
    return Math.abs(Integer.parseInt((String)key) % numPartitions);
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}



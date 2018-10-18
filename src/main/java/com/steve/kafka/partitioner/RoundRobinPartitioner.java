package com.steve.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RoundRobinPartitioner implements Partitioner {
  
  private static AtomicLong next = new AtomicLong();


  @Override
  public int partition(String topicName, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
    int numPartitions = cluster.partitionCountForTopic(topicName);
    long nextIndex = next.incrementAndGet();
    return (int)nextIndex % numPartitions;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}



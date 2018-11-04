package com.steve.kafka.consumer;

import com.steve.kafka.pojo.CqiMessage;
import com.steve.kafka.pojo.ReconciledMessage;
import com.steve.kafka.serialize.CqiMessageDeSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Created by stevexu on 1/16/17.
 */
public class CqiMessageConsumerTask implements Runnable{

    private static List<String> topics = new ArrayList<>();

    private final Consumer<String, CqiMessage> consumer;

    private final int id;

    private static final Logger logger = LoggerFactory.getLogger(CqiMessageConsumerTask.class);

    private int count = 0;

    public CqiMessageConsumerTask(int id, String groupId, List<String> topics, String brokerList) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760 / 100);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CqiMessageDeSerializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        this.topics = topics;
        this.id = id;
    }

    private boolean doCommitSync(CqiMessage cqiMessage) {
        try {
            consumer.commitSync();
            return true;
        } catch (WakeupException e) {
            // we're shutting down, but finish the commit first and then
            // rethrow the exception so that the main loop can exit
            doCommitSync(cqiMessage);
            throw e;
        } catch (CommitFailedException e) {
            // the commit failed with an unrecoverable error. if there is any
            // internal state which depended on the commit, you can clean it
            // up here. otherwise it's reasonable to ignore the error and go on
            if (cqiMessage != null && cqiMessage.getItemId() != null) {
                logger.error("Commit failed, itemid:"+cqiMessage.getItemId());
                //ProducerDemo.sendOne(Constants.TOPIC, vendorItemDTO);
            }
            return false;
        }
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics, new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    partitions.forEach(p -> {
                        logger.info("Revoked partition for client: " + id + " topic:" + p.topic() + "," + p.partition());
                    });
                    //doCommitSync(null);
                }

                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    partitions.forEach(p -> {
                        logger.info("assigned partition for client: " + id + " topic:" + p.topic() + ",partition:" + p.partition());
                    });
                }
            });

            while (true) {
                ConsumerRecords<String, CqiMessage> records = consumer.poll(1000 * 60);
                if (records.count() > 0) {
                    logger.info("pull {} to consume", records.count());
                } else {
                    logger.warn("there is no records pooling from consumer within 60 seconds");
                }
                for (ConsumerRecord<String, CqiMessage> record : records) {
                    logger.info("consumer {} receives message:{} ",id, record.value());
                    count++;
                    doCommitSync(record.value());
                }
                if (records.count() > 0) {
                    logger.info("consumer {} finished consumes：{} messages", id, count);
                }
            }
        } catch (WakeupException e) {
            // ignore, we're closing
            logger.warn("wakeup exception");
            if ( count > 0) {
                logger.info("consumer: {} finished consumes：{} messages", id, count);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unexpected error", e);
        }
        finally {
            try {
                doCommitSync(null);
            } finally {
                consumer.close();
            }
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}

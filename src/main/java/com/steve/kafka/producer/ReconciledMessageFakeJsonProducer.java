package com.steve.kafka.producer;

import com.steve.kafka.pojo.ReconciledBrandMessage;
import com.steve.kafka.serialize.ReconciledMessageSerializer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * Created by stevexu on 1/16/17.
 */
public class ReconciledMessageFakeJsonProducer {

    static Producer<String, ReconciledBrandMessage> producer;

    private static final Logger logger = LoggerFactory.getLogger(ReconciledMessageFakeProducer.class);

    public static void main(String[] args) throws Exception {
        InputStream input = ReconciledMessageFakeProducer.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(input);

        initProducer(properties);
        sendBatch(producer, properties.getProperty("demeterner"));
    }

    public static void initProducer(Properties properties) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("brandstreaminghosts"));
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ReconciledMessageSerializer.class.getName());

        KafkaProducer kafkaProducer = new KafkaProducer<String, ReconciledBrandMessage>(props);
        producer = kafkaProducer;
    }

    public static void sendBatch(Producer<String, ReconciledBrandMessage> producer, String topic) throws InterruptedException {
        long itemId = 1L;
        for (int round = 1; round <= 500; round++) {
            //Long itemId = Long.valueOf(round - 1) * 10 + i;
            ProducerRecord<String, ReconciledBrandMessage> message = new ProducerRecord<>(topic, String.valueOf(itemId),
                         new ReconciledBrandMessage(itemId, 100L, "SK-II LXP 얼티미트 퍼펙팅 크림 50g",
                                 "sk2", "56653", 56653L, 56649L, 56648L, 56112L, "sk-II", "에스케이투",
                                  0, new Date().getTime(), java.util.UUID.randomUUID().toString()));
            producer.send(message, (RecordMetadata recordMetadata, Exception e) -> {
                if (e != null) {
                    logger.error("error while send to kafka, itemid:" + message.value().getItemId(), e);
                }
            });
            itemId++;

            ProducerRecord<String, ReconciledBrandMessage> message2 = new ProducerRecord<>(topic, String.valueOf(itemId),
                         new ReconciledBrandMessage(itemId, 200L, "[중고명품 미스터문] MCM(엠씨엠) 1011103041506 베이지 브라운 레더 토트백",
                                  "[중고명품 미스터문] MCM(엠씨엠)", "69284", 71945L, 69283L, 69183L, 69182L, "MCM",
                                  "엠씨엠", 0, new Date().getTime(), java.util.UUID.randomUUID().toString()));
            producer.send(message2, (RecordMetadata recordMetadata, Exception e) -> {
                if (e != null) {
                    logger.error("error while send to kafka, itemid:" + message2.value().getItemId(), e);
                }
            });
            itemId++;
            //Thread.sleep(1000*5);
        }
        producer.close();
    }

}

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
        sendBatch(producer, properties.getProperty("demetercqibrand"));
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
        for (int round = 1; round <= 20; round++) {
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

            ProducerRecord<String, ReconciledBrandMessage> message3 = new ProducerRecord<>(topic, String.valueOf(itemId),
                   new ReconciledBrandMessage(itemId, 300L, "해외) 아디다스 3스트라이프 크롭 후드티 ADIDAS 3 STRIPE CROPPED HOODIE",
                     "해외) 아디다스 3스트라이프 크롭 후드티", "79269", 104059L, 104026L, 103621L, 103371L, "",
                     "상세페이지 참조", 0, new Date().getTime(), java.util.UUID.randomUUID().toString()));
            producer.send(message3, (RecordMetadata recordMetadata, Exception e) -> {
                if (e != null) {
                    logger.error("error while send to kafka, itemid:" + message3.value().getItemId(), e);
                }
            });
            itemId++;

            ProducerRecord<String, ReconciledBrandMessage> message4 = new ProducerRecord<>(topic, String.valueOf(itemId),
                        new ReconciledBrandMessage(itemId, 400L, "신송 된장골드 4kg 2kgX2입 조미료/장류/분말류", "주)팡팡마트",
                        "82528", 58467L, 72679L, 58461L, 59258L, "",
                        "[상세설명참조] / [상세설명참조]", 0, new Date().getTime(), java.util.UUID.randomUUID().toString()));
            producer.send(message4, (RecordMetadata recordMetadata, Exception e) -> {
                if (e != null) {
                    logger.error("error while send to kafka, itemid:" + message4.value().getItemId(), e);
                }
            });
            itemId++;

            ProducerRecord<String, ReconciledBrandMessage> message5 = new ProducerRecord<>(topic, String.valueOf(itemId),
                         new ReconciledBrandMessage(itemId, 400L, "test book dvd", "book",
                          "92149", 0L, 0L, 0L, 0L, "",
                          "", 0, new Date().getTime(), java.util.UUID.randomUUID().toString()));
            producer.send(message5, (RecordMetadata recordMetadata, Exception e) -> {
                if (e != null) {
                    logger.error("error while send to kafka, itemid:" + message5.value().getItemId(), e);
                }
            });
            itemId++;
            //Thread.sleep(1000*5);
        }
        producer.close();
    }

}

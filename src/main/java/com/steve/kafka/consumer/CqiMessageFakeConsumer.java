package com.steve.kafka.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author stevexu
 * @since 10/30/18
 */
public class CqiMessageFakeConsumer {

    public static void main(String args[]) throws InterruptedException, IOException {
        int numConsumers = 3;
        InputStream input = CqiMessageFakeConsumer.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(input);

        List<String> topics = Arrays.asList(properties.getProperty("cqibrandtopic"));
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<CqiMessageConsumerTask> consumers = new ArrayList<>();
        for (int i = 1; i <= numConsumers; i++) {
            CqiMessageConsumerTask consumer = new CqiMessageConsumerTask(i, (String)properties.getProperty("cqibrandgroup"),
                                                                                       topics, (String)properties.get("brandstreaminghosts"));
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (CqiMessageConsumerTask consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}

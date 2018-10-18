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
 * Created by stevexu on 1/16/17.
 */
public class ReconciledMessageFakeConsumer {

    public static void main(String args[]) throws InterruptedException, IOException {
        int numConsumers = 3;
        InputStream input = ReconciledMessageFakeConsumer.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(input);

        List<String> topics = Arrays.asList(properties.getProperty("brandstreamingtopic"));
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<ReconciledMessageConsumerTask> consumers = new ArrayList<>();
        for (int i = 1; i <= numConsumers; i++) {
            ReconciledMessageConsumerTask consumer = new ReconciledMessageConsumerTask(i, (String)properties.getProperty("brandstreaminggroup"),
                                topics, (String)properties.get("brandstreaminghosts"));
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ReconciledMessageConsumerTask consumer : consumers) {
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

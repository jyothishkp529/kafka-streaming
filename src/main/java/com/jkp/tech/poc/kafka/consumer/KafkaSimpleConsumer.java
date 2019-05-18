package com.jkp.tech.poc.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;


public class KafkaSimpleConsumer {
    private static final String BROKERHOST = "localhost";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC = "test";
    private static final String CONSUMER_GROUP = "FirstConsumer";

    public static void main(String[] args) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BROKERHOST+":"+BROKERPORT);
        p.put("group.id", CONSUMER_GROUP);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> c = new KafkaConsumer<String, String>(p);
        c.subscribe(Collections.singletonList(TOPIC));
        try {
            while (true) {
                ConsumerRecords<String, String> rec = c.poll(1000);
                System.out.println("We got record count " + rec.count());
                for (ConsumerRecord<String, String> r : rec) {
                    System.out.println(r.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            c.close();
        }
    }
}

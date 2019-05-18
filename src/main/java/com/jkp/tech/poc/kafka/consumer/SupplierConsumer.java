package com.jkp.tech.poc.kafka.consumer;

import com.jkp.tech.poc.kafka.config.Config;
import com.jkp.tech.poc.kafka.message.SupplierDo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SupplierConsumer {

    private static final String BROKERHOST = "localhost";
    private static final String BROKERPORT = "9092";
    private static final String CONSUMER_GROUP = "SupplierConsumer";


    public static void main(String[] args){
        SupplierConsumer sc = new SupplierConsumer();
        sc.doExecute();

    }

    private void doExecute(){
        Properties p = new Properties();
        p.put("bootstrap.servers", BROKERHOST+":"+BROKERPORT);
        p.put("group.id", CONSUMER_GROUP);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "SupplierDeSerializer");

        KafkaConsumer<String, SupplierDo> c = new KafkaConsumer<String, SupplierDo>(p);
        c.subscribe(Arrays.asList(Config.SUPPLIER_TOPIC));
        try {
            while (true) {
                ConsumerRecords<String, SupplierDo> rec = c.poll(1000);
                System.out.println("We got record count " + rec.count());
                for (ConsumerRecord<String, SupplierDo> r : rec) {
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

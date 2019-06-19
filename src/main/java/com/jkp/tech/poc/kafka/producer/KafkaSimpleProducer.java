package com.jkp.tech.poc.kafka.producer;

import com.jkp.tech.poc.kafka.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class KafkaSimpleProducer {


    public static void main(String[] args) throws InterruptedException, IOException {

        Properties p = new Properties();

        // Declare the propeties of cluster and informationa about data key and value
        p.put("bootstrap.servers", Config.BROKER_HOST + ":" + Config.BROKER_PORT);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create producer and send data in format : (topic name , key , value)
        Producer<String,String> pd = new KafkaProducer<>(p);
        ProducerRecord<String,String> rec = new ProducerRecord<>(Config.TOPIC ,"key","Hello from Java client_"+System.currentTimeMillis());

        // 1) Fire and forget
        pd.send(rec);
        pd.close();
        System.out.println("Completed. Msg Published");


    }
}
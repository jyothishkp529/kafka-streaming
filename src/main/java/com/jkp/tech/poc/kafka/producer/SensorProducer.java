package com.jkp.tech.poc.kafka.producer;

import com.jkp.tech.poc.kafka.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * This is reference implementation for the usage of Custom partitioner.
 * This producer send sensor data from various devices to a single topic. Of which the generated from a a specific devices ghas to be send to a subset of partitions.
 * For eg: There are total 10 and first 3 is reserved  for a devices TSS.
 *
 *  To handle this requirement define a custom Partitioner.
 */
public class SensorProducer {
    public static void main(String[] args){
        SensorProducer sp = new SensorProducer();
        sp.doExecute();

    }

    private void doExecute(){
        Properties p = new Properties();

        // Declare the propeties of cluster and informationa about data key and value
        p.put("bootstrap.servers", Config.BROKER_HOST + ":" + Config.BROKER_PORT);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("partitioner.class","com.jkp.tech.poc.kafka.partitioner.SensorPartitioner");
        p.put("speed.sensor.name","TSS");


        // Create producer and send data in format : (topic name , key , value)
        Producer<String,String> producer = new KafkaProducer<>(p);
        for (int i=0;i<10;i++){
            producer.send( new ProducerRecord<>(Config.SENSOR_TOPIC ,"TSS","Temperature_"+i));
        }

        for (int i=0;i<10;i++){
            producer.send( new ProducerRecord<>(Config.SENSOR_TOPIC ,"SSH-"+i,"Pressure_"+i));
        }
        producer.close();
        System.out.println("Completed. Msg Published");
    }
}

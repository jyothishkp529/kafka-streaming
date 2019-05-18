package com.jkp.tech.poc.kafka.producer;

import com.jkp.tech.poc.kafka.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;

import static java.lang.System.out;

/**
 * Mandatory properties.
 * <p>
 * bootstrap.servers
 * key.serializer   - This is optional
 * value.serializer
 */
public class KafkaSynchrnousProducer {


    public static void main(String[] args) throws InterruptedException, IOException {

        KafkaSynchrnousProducer ksp = new KafkaSynchrnousProducer();
        ksp.doExecute();
    }

    private void doExecute() {


        Producer<String, String> pd = getStringProducer(Config.BROKER_HOST, Config.BROKER_PORT);
        ProducerRecord<String, String> message = createMessage(Config.TOPIC, "key", "Hello from "+ this.getClass().getSimpleName()+"_" + System.currentTimeMillis());

        // 1) Fire and forget
        sendMessageSync(pd, message);
        pd.close();
        System.out.println("Completed. Msg Published");
    }


    private Producer<String, String> getStringProducer(String brokerHosts, String brokerPort) {
        Properties p = new Properties();

        // Declare the propeties of cluster and informationa about data key and value
        p.put("bootstrap.servers", Config.BROKER_HOST + ":" + Config.BROKER_PORT);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create producer and send data in format : (topic name , key , value)
        return new KafkaProducer<>(p);
    }


    private ProducerRecord<String, String> createMessage(String topic, String key, String msg) {
        ProducerRecord<String, String> message = new ProducerRecord<>(topic, key, msg);
        return message;

    }

    /**
     * This is a blocking producer. It waits for the confirmation of delivery.
     * @param producer
     * @param message
     * @return
     */
    private RecordMetadata sendMessageSync(Producer<String, String> producer, ProducerRecord<String, String> message) {
        RecordMetadata result = null;
        try {
            result = producer.send(message).get();
            out.println("Message sent opic => " + result.topic() + ",  Partition No=>" + result.partition() + " Offset=>" + result.offset());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;

    }
}
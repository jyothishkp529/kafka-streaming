package com.jkp.tech.poc.kafka.producer;

import com.jkp.tech.poc.kafka.config.Config;
import com.jkp.tech.poc.kafka.message.SupplierDo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

import static java.lang.System.out;

public class SupplierProducer {
    public static void main(String[] args) throws Exception {
        SupplierProducer sp = new SupplierProducer();
        sp.doExecute();

    }

    private void doExecute() throws Exception {
        DateFormat df = new SimpleDateFormat("YYYY-MM-dd");
        SupplierDo s1 = new SupplierDo(1001, "TCS", "2018-10-16");
        SupplierDo s2 = new SupplierDo(1001, "TCS", "2019-05-20");
/*

        Properties props = null;
        InputStream input = new FileInputStream("producer_supplier.properties");
        props.load(input);
*/


        Properties props = new Properties();
        props.put("bootstrap.servers", Config.BROKER_HOST + ":" + Config.BROKER_PORT);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.jkp.tech.poc.kafka.serde.SupplierSerializer");

        out.println("Configuration loaded.");


        Producer<String, SupplierDo> producer = new KafkaProducer<String, SupplierDo>(props);
        for (int i=0; i < 10;i++) {
            producer.send(new ProducerRecord<String, SupplierDo>(Config.SUPPLIER_TOPIC, "SUP", s1)).get();
            //producer.send(new ProducerRecord<String, SupplierDo>(Config.SUPPLIER_TOPIC, "SUP", s2)).get();
            System.out.println("Message send");
            Thread.sleep(2000);
        }
        producer.close();


    }
}

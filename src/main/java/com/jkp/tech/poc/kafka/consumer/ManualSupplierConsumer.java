package com.jkp.tech.poc.kafka.consumer;

import com.jkp.tech.poc.kafka.config.Config;
import com.jkp.tech.poc.kafka.message.SupplierDo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import static java.lang.System.out;

public class ManualSupplierConsumer {

    private static final String BROKERHOST = "localhost";
    private static final String BROKERPORT = "9092";
    private static final String CONSUMER_GROUP = "SupplierConsumer";


    public static void main(String[] args){
        ManualSupplierConsumer sc = new ManualSupplierConsumer();
        sc.doExecute();

    }

    private void doExecute(){
        Properties p = new Properties();
        p.put("bootstrap.servers", BROKERHOST+":"+BROKERPORT);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "com.jkp.tech.poc.kafka.serde.SupplierDeSerializer");
        p.put("enable.auto.commit","false");
        p.put("group.id", CONSUMER_GROUP);

        KafkaConsumer<String, SupplierDo> consumer = new KafkaConsumer<String, SupplierDo>(p);
        consumer.subscribe(Arrays.asList(Config.SUPPLIER_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, SupplierDo> rec = consumer.poll(1000);
                out.println("We got record count " + rec.count());
                for (ConsumerRecord<String, SupplierDo> r : rec) {
                    out.println(String.format("ID = %s, Name=%s", r.value().getId(),r.value().getName()));
                }
                consumer.commitAsync();
                List<PartitionInfo> partInfoList=  consumer.partitionsFor(Config.SUPPLIER_TOPIC);
                for (PartitionInfo pf : partInfoList) {
                    out.println("Manually Commited the offset ::> "+ consumer.committed(new TopicPartition(Config.SUPPLIER_TOPIC,pf.partition())));


                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.commitSync();
            consumer.close();
        }

    }
}

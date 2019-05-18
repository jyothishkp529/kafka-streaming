package com.jkp.tech.poc.kafka.partitioner;

import com.jkp.tech.poc.kafka.config.Config;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.*;
import org.apache.kafka.common.record.InvalidRecordException;

import java.util.Map;

import static java.lang.System.out;

public class SensorPartitioner implements Partitioner {

    private String speedSensorName = null;

    public void configure(Map<String, ?> configs) {
        String speedSensorName = configs.get("speed.sensor.name").toString();

    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        int partition_id = 0;
        if ((keyBytes == null) || !(key instanceof String)) {
            throw new InvalidRecordException("All messages must have sensor nae as Key.");
        }

        int numPartitions = cluster.partitionCountForTopic(topic);
        int speedPartitionSize = (int) Math.abs(numPartitions * .3);
        int normalPartitions = numPartitions - speedPartitionSize;

        if (Config.SPEED_SENSOR.equals((String) key)) {
            partition_id = Utils.toPositive(Utils.murmur2(valueBytes) % speedPartitionSize);
        } else {
            partition_id = Utils.toPositive(Utils.murmur2(keyBytes) % normalPartitions) + speedPartitionSize;
        }
        out.println("Key = " + (String) key + ", Partition-Id =" + partition_id);

        return partition_id;
    }

    public void close() {

    }


}

package com.jkp.tech.poc.kafka.serde;

import com.jkp.tech.poc.kafka.message.SupplierDo;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SupplierSerializer implements Serializer<SupplierDo> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, SupplierDo data) {
        int nameLength;
        byte[] serializedNameBytes;
        int dateLength;
        byte[] serializedDateBytes;
        

        try {
            serializedNameBytes = data.getName().getBytes(encoding);
            nameLength = serializedNameBytes.length;
            serializedDateBytes = data.getStartDate().getBytes(encoding);
            dateLength = serializedDateBytes.length;

            ByteBuffer buf = ByteBuffer.allocate(4 + 4 + nameLength + 4 + dateLength);
            buf.putInt(data.getId());
            buf.putInt(nameLength);
            buf.put(serializedNameBytes);
            buf.putInt(dateLength);
            buf.put(serializedDateBytes);
            return buf.array();

        } catch (Exception e) {
            throw new SerializationException("Failed to serialize the object.");

        }

    }

    @Override
    public void close() {

    }
}

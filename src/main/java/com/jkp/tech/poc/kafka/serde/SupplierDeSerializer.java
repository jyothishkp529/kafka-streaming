package com.jkp.tech.poc.kafka.serde;

import com.jkp.tech.poc.kafka.message.SupplierDo;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SupplierDeSerializer implements Deserializer<SupplierDo> {
    private String encoding = "UTF8";
    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public SupplierDo deserialize(String topic, byte[] data) {
        if ( data == null){
            System.err.println("No data found");
            return  null;
        }
        try {

            ByteBuffer buf = ByteBuffer.wrap(data);
            int id = buf.getInt();
            int nameLength = buf.getInt();
            byte[] nameBytes = new byte[nameLength];
            buf.get(nameBytes);
            String name = new String(nameBytes, encoding);



            int dateLength = buf.getInt();
            byte[] dateBytes = new byte[dateLength];
            buf.get(dateBytes);
            String dateStr =  new String(dateBytes,encoding);
            System.out.println(String.format("Parsed Value : %s",dateStr));
             /*
            DateFormat df = new SimpleDateFormat("YYYY-MM-dd");
            Date date = df.parse(dateStr);
            */

            return new SupplierDo(id,name,dateStr);
        }catch (Exception e){
            throw new SerializationException("Failed deserialise");
        }
    }

    @Override
    public void close() {


    }
}

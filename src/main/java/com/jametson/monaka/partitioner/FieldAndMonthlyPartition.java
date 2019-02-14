package com.jametson.monaka.partitioner;

import io.confluent.connect.hdfs.errors.PartitionException;
import io.confluent.connect.hdfs.partitioner.FieldPartitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class FieldAndMonthlyPartition extends FieldPartitioner {
    private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);
    private static final DateFormat df = new SimpleDateFormat("MM-yyyy");
    private static String fieldName, timeFieldName;
    private ArrayList<FieldSchema> partitionFields = new ArrayList<>();

    public void configure(Map<String, Object> config) {
        fieldName = (String)config.get("partition.field.name");
        timeFieldName = System.getProperty("partition.time.field.name", "");
        partitionFields.add(new FieldSchema(fieldName, TypeInfoFactory.stringTypeInfo.toString(), ""));
    }

    public String encodePartition(SinkRecord sinkRecord) {
        String fieldKey = switchByField(sinkRecord);
        String timeKey = switchByMonth(sinkRecord);
        return fieldKey + "/" + timeKey;
    }

    private String switchByField(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        Schema valueSchema = sinkRecord.valueSchema();
        if (value instanceof Struct) {
            boolean includeFieldName = System.getProperty("partition.include.field.name", "false").toLowerCase().equals("true");
            String pathFormat = "";
            if (includeFieldName) {
                pathFormat = fieldName + "=";
            }
            Struct struct = (Struct)value;
            Object fieldKey = struct.get(fieldName);
            Schema.Type type = valueSchema.field(fieldName).schema().type();
            switch(type) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    Number record = (Number)fieldKey;
                    return pathFormat + record.toString();
                case STRING:
                    return pathFormat + (String)fieldKey;
                case BOOLEAN:
                    boolean booleanRecord = (Boolean)fieldKey;
                    return pathFormat + Boolean.toString(booleanRecord);
                default:
                    log.error("Type {} is not supported as a partition key.", type.getName());
                    throw new PartitionException("Error encoding partition.");
            }
        } else {
            log.error("Value is not Struct type.");
            throw new PartitionException("Error encoding partition.");
        }
    }

    private String switchByMonth(SinkRecord sinkRecord) {
        long timestamp;

        if (timeFieldName.equals("")) {
            timestamp = System.currentTimeMillis();
        } else {
            try {
                Struct struct = (Struct) sinkRecord.value();
                timestamp = (long) struct.get(timeFieldName);
            } catch (ClassCastException | DataException e) {
                throw new ConnectException(e);
            }
        }
        Date date = new Date(timestamp);
        return df.format(date);
    }

    public String generatePartitionedPath(String topic, String encodedPartition) {
        return topic + "/" + encodedPartition;
    }

    public List<FieldSchema> partitionFields() {
        return this.partitionFields;
    }
}

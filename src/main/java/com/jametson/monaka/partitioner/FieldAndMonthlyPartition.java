package com.jametson.monaka.partitioner;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class FieldAndMonthlyPartition<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(FieldAndMonthlyPartition.class);
    private static final DateFormat df = new SimpleDateFormat("MM-yyyy");
    private List<String> fieldNames;
    private String timeFieldName;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> config) {
        fieldNames = (List<String>)config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
        timeFieldName = (String)config.get(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG);
        delim = (String)config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        String fieldKey = switchByFields(sinkRecord);
        String timeKey = switchByMonth(sinkRecord);
        return fieldKey + timeKey;
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
        return topic + this.delim + encodedPartition;
    }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields(
                    Utils.join(fieldNames, ",")
            );
        }
        return partitionFields;
    }

    private String switchByFields(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        if (value instanceof Struct) {
            final Schema valueSchema = sinkRecord.valueSchema();
            final Struct struct = (Struct) value;

            StringBuilder builder = new StringBuilder();
            for (String fieldName : fieldNames) {
                if (builder.length() > 0) {
                    builder.append(this.delim);
                }

                Object partitionKey = struct.get(fieldName);
                Schema.Type type = valueSchema.field(fieldName).schema().type();
                switch (type) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        Number record = (Number) partitionKey;
                        builder.append(record.toString());
                        break;
                    case STRING:
                        builder.append((String) partitionKey);
                        break;
                    case BOOLEAN:
                        boolean booleanRecord = (boolean) partitionKey;
                        builder.append(Boolean.toString(booleanRecord));
                        break;
                    default:
                        log.error("Type {} is not supported as a partition key.", type.getName());
                        throw new io.confluent.connect.storage.errors.PartitionException("Error encoding partition.");
                }
            }
            return builder.toString();
        } else {
            log.error("Value is not Struct type.");
            throw new PartitionException("Error encoding partition.");
        }
    }

    private String switchByMonth(SinkRecord sinkRecord) {
        long timestamp;

        // use timestamp from SinkRecord
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
}

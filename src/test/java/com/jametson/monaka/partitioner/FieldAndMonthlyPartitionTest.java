package com.jametson.monaka.partitioner;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class FieldAndMonthlyPartitionTest {

    private static final String TOPIC = "topic";
    private static final int PARTITION = 1;
    private static final String timeZoneString = TimeZone.getDefault().getID();
    private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(timeZoneString);
    private static final String SMS_CHANNEL = "sms";
    private static final String EMAIL_CHANNEL = "email";

    @Ignore
    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionByFieldAndMonth() {
        System.setProperty("partition.include.field.name", "false");
        System.setProperty("partition.time.field.name", "time");

        FieldAndMonthlyPartition<String> partition = new FieldAndMonthlyPartition<>();
        Map<String, Object> config = new HashMap<>();
        config.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, Collections.singletonList("channel"));
        config.put(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG, "time");
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "directory.delim");
        partition.configure(config);

        long timestamp = new DateTime(2019, 9, 19, 16, 0, 0, 0, DATE_TIME_ZONE).getMillis();
        SinkRecord sinkRecord1 = createSinkRecord(timestamp, SMS_CHANNEL);
        SinkRecord sinkRecord2 = createSinkRecord(timestamp, EMAIL_CHANNEL);

        String expected1 = "smsdirectory.delim09-2019";
        String actual1 = partition.encodePartition(sinkRecord1);
        assertEquals(expected1, actual1);

        String expected2 = "emaildirectory.delim09-2019";
        String actual2 = partition.encodePartition(sinkRecord2);
        assertEquals(expected2, actual2);
    }

    private SinkRecord createSinkRecord(long timeInMillis, String channel) {
        Schema schema = SchemaBuilder.struct().name("record").version(2)
                .field("time", Schema.INT64_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("channel", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("time", timeInMillis)
                .put("int", 12)
                .put("long", 12L)
                .put("float", 12.2f)
                .put("double", 12.2)
                .put("channel", channel);

        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, struct, 0L);
    }
}

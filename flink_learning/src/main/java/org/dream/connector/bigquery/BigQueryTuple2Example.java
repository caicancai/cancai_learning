package org.dream.connector.bigquery;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.dream.connector.pubsub.PubSubPublisher;

import java.io.IOException;
import java.time.Duration;

public class BigQueryTuple2Example {
    final static String projectName = null;

    final static String subscriptionName = null;

    final static String outputTopicName = null;

    final static String tableName = null;

    final static String datasetName = null;

    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        PubSubPublisher pubSubPublisher = new PubSubPublisher(projectName, outputTopicName);

        pubSubPublisher.publish("{\"string\": \"caicancai\"}");
        runFlinkJob(projectName, subscriptionName);
    }


    public static void runFlinkJob(String projectName, String subscriptionName) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        ExecutionConfig config = env.getConfig();
        config.addDefaultKryoSerializer(Schema.class, AvroKryoSerializerUtils.AvroSchemaSerializer.class);


        // Sink to BigQuery
        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(projectName)
                        .setDataset(datasetName)
                        .setTable(tableName)
                        .build();

        BigQuerySchemaProvider destSchemaProvider =
                new BigQuerySchemaProviderImpl(sinkConnectOptions);

        Schema avroSchema = destSchemaProvider.getAvroSchema();

        System.out.println(avroSchema.toString(true));

        // Create PubSub source

        PubSubDeserializationSchema<Tuple2<String,String>> deserializer = new PubSubDeserializationSchema<Tuple2<String, String>>() {
            @Override
            public TypeInformation<Tuple2<String, String>> getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
            }

            @Override
            public boolean isEndOfStream(Tuple2<String, String> nextElement) {
                return false;
            }

            @Override
            public Tuple2<String, String> deserialize(PubsubMessage message) throws Exception {
                Tuple2<String,String> record = new Tuple2<>();
                try {
                    JsonNode jsonNode = MAPPER.readTree(message.getData().toStringUtf8());
                        record.f0 = "string";
                        record.f1 = jsonNode.get("string").asText();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to deserialize PubsubMessage: " + message.getData().toStringUtf8(), e);
                }
                return record;
            }
        };

        DataStreamSource<Tuple2<String,String>> pubsubSource = env.addSource(PubSubSource.newBuilder()
                .withDeserializationSchema(deserializer)
                .withProjectName(projectName)
                .withSubscriptionName(subscriptionName)
                .withPubSubSubscriberFactory(1000, Duration.ofMillis(60000), 3)
                .build());

        pubsubSource.print();

        DataStream<GenericRecord> transformed = pubsubSource.map(tuple -> {
            GenericRecord record = new GenericData.Record(avroSchema);
            for (Schema.Field field : avroSchema.getFields()) {
                record.put(field.name(), tuple.f1);
            }
            return record;
        });

        transformed.print();

        // output bigquery schema
        System.out.println("BigQuery Table Schema: " + destSchemaProvider.getAvroSchema());

        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(sinkConnectOptions)
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .serializer(new AvroToProtoSerializer())
                        .schemaProvider(destSchemaProvider)
                        .build();

        transformed.sinkTo(BigQuerySink.get(sinkConfig, env));
        env.execute("Flink Streaming PubSubReader");

    }
}

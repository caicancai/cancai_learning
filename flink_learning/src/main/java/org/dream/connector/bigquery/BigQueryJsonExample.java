package org.dream.connector.bigquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.dream.connector.pubsub.PubSubPublisher;
import org.dream.connector.serializer.StringToProtoSerializer;

public class BigQueryJsonExample {

    final static String projectName = null;

    final static String subscriptionName = null;

    final static String outputTopicName = null;

    final static String tableName = null;

    final static String datasetName = null;

    public static void main(String[] args) throws Exception {
        PubSubPublisher pubSubPublisher = new PubSubPublisher(projectName, outputTopicName);

        pubSubPublisher.publish("");
        runFlinkJob(projectName, subscriptionName);
    }

    public static void runFlinkJob(String projectName, String subscriptionName) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        // Create PubSub source
        PubSubSource<String> pubSubSource = PubSubSource.newBuilder()
                .withDeserializationSchema(new SimpleStringSchema())
                .withProjectName(projectName)
                .withSubscriptionName(subscriptionName)
                .build();

        DataStream<String> pubsubStream = env.addSource(pubSubSource);

        // Sink to BigQuery
        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(projectName)
                        .setDataset(datasetName)
                        .setTable(tableName)
                        .build();
        BigQuerySchemaProvider destSchemaProvider =
                new BigQuerySchemaProviderImpl(sinkConnectOptions);

        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(sinkConnectOptions)
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .serializer(new StringToProtoSerializer())
                        .schemaProvider(destSchemaProvider)
                        .build();

        pubsubStream.sinkTo(BigQuerySink.get(sinkConfig, env));
        env.execute("Flink Streaming PubSubReader");
    }


}

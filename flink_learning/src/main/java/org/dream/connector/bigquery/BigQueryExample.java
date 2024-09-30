package org.dream.connector.bigquery;


import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.dream.connector.pubsub.PubSubPublisher;
import org.dream.connector.serializer.IntegerSerializer;
import org.dream.connector.serializer.IntegerToProtoSerializer;

public class BigQueryExample {

    final static String projectName = null;

    final static String subscriptionName = null;

    final static String outputTopicName = null;

    final static String tableName = null;

    final static String datasetName = null;

    public static void main(String[] args) throws Exception {

        PubSubPublisher pubSubPublisher = new PubSubPublisher(projectName, outputTopicName);
        pubSubPublisher.publish(10);

        runFlinkJob(projectName, subscriptionName);
    }

    public static void runFlinkJob(String projectName, String subscriptionName) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        // Create PubSub source
        PubSubSource<Integer> pubSubSource = PubSubSource.newBuilder()
                .withDeserializationSchema(new IntegerSerializer())
                .withProjectName(projectName)
                .withSubscriptionName(subscriptionName)
                .build();

        DataStream<Integer> pubsubStream = env.addSource(pubSubSource);

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
                        .serializer(new IntegerToProtoSerializer())
                        .schemaProvider(destSchemaProvider)
                        .build();

        pubsubStream.sinkTo(BigQuerySink.get(sinkConfig, env));
        env.execute("Flink Streaming PubSubReader");

    }

    private static Integer printAndReturn(Integer i) {
        System.out.println("Processed message with payload: " + i);
        return i;
    }
}

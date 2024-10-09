package org.dream.connector.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubStringExample {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubExample.class);

    final static String projectName = null;

    final static String subscriptionName = null;

    final static String outputTopicName = null;

    public static void main(String[] args) throws Exception {
        // parse input arguments
        PubSubPublisher pubSubPublisher = new PubSubPublisher(projectName, outputTopicName);

        // Publish String message
        pubSubPublisher.publish("");

        runFlinkJob(projectName, subscriptionName, outputTopicName);
    }

    private static void runFlinkJob(
            String projectName, String subscriptionName, String outputTopicName) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        env.addSource(
                        PubSubSource.newBuilder()
                                .withDeserializationSchema(new SimpleStringSchema())
                                .withProjectName(projectName)
                                .withSubscriptionName(subscriptionName)
                                .withMessageRateLimit(1)
                                .build())
                .map(PubSubStringExample::printAndReturn)
                .addSink(
                        PubSubSink.newBuilder()
                                .withSerializationSchema(new SimpleStringSchema())
                                .withProjectName(projectName)
                                .withTopicName(outputTopicName)
                                .build());

        env.execute("Flink Streaming PubSubReader");
    }

    private static String printAndReturn(String i) {
        System.out.println("Processed message with payload: " + i);
        return i;
    }
}

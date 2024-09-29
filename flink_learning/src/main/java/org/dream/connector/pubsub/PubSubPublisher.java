package org.dream.connector.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

/** Helper class to send PubSubMessages to a PubSub topic. */
class PubSubPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubExample.class);

    private final String projectName;
    private final String topicName;

    PubSubPublisher(String projectName, String topicName) {
        this.projectName = projectName;
        this.topicName = topicName;
    }

    /**
     * Publish messages with as payload a single integer. The integers inside the messages start
     * from 0 and increase by one for each message send.
     *
     * @param amountOfMessages amount of messages to send
     */
    void publish(int amountOfMessages) {
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(TopicName.of(projectName, topicName)).build();
            for (int i = 0; i < amountOfMessages; i++) {
                ByteString messageData = ByteString.copyFrom(BigInteger.valueOf(i).toByteArray());
                PubsubMessage message = PubsubMessage.newBuilder().setData(messageData).build();
                publisher.publish(message).get();

                LOG.info("Published message with payload: " + i);
                Thread.sleep(100L);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (publisher != null) {
                    publisher.shutdown();
                }
            } catch (Exception e) {
            }
        }
    }
}
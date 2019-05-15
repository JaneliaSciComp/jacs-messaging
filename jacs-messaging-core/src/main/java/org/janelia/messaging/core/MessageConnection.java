package org.janelia.messaging.core;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Created by schauderd on 11/2/17.
 */
public interface MessageConnection {
    boolean isOpen();

    void openConnection(String host, String username, String password, int threadPoolSize);

    void closeConnection();

    String bindAndConnectTo(String exchangeName, String routingKey, String queueName);

    void publish(String exchange, String routingKey, Map<String, Object> headers, byte[] body);

    String subscribe(String queue, boolean ack, MessageHandler messageHandler);

    void cancelSubscription(String subscriptionTag);

    Stream<GenericMessage> retrieveMessages(String queue, boolean ack);
}

package org.janelia.messaging.core;

public interface MessageConsumer {
    void connectTo(String queueName);
    void bindAndConnectTo(String exchangeName,
                          String routingKey,
                          String queueName);
    void disconnect();
    boolean isConnected();
}

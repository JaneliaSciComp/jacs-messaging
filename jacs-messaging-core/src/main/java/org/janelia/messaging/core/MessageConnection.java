package org.janelia.messaging.core;

/**
 * Created by schauderd on 11/2/17.
 */
public interface MessageConnection {
    boolean isOpen();

    void openConnection(String host, String username, String password, int threadPoolSize);

    void closeConnection();

    String bindAndConnectTo(String exchangeName, String routingKey, String queueName);
}

package org.janelia.messaging.core;

public interface MessageConsumer {
    void connect(String host,
                 String user,
                 String password,
                 String queueName,
                 int threadPoolSize);
    void bindAndConnect(String host,
                        String user,
                        String password,
                        String exchangeName,
                        String routingKey,
                        int threadPoolSize);
    void disconnect();
}

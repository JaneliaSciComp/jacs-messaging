package org.janelia.messaging.core;

public interface MessageConsumer {
    MessageConsumer connect(String host,
                            String user,
                            String password,
                            String queueName,
                            int threadPoolSize,
                            int retries);
    MessageConsumer bindAndConnect(String host,
                                   String user,
                                   String password,
                                   String exchangeName,
                                   String routingKey,
                                   int threadPoolSize,
                                   int retries);
    void disconnect();
}

package org.janelia.messaging.core;

public interface MessageConsumer {
    MessageConsumer connect(String host,
                            String user,
                            String password,
                            String exchangeName,
                            String queueName,
                            int retries);
    void disconnect();
}

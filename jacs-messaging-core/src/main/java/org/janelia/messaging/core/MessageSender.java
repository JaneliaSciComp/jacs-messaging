package org.janelia.messaging.core;

import java.util.Map;

public interface MessageSender {
    MessageSender connect(String host,
                          String user,
                          String password,
                          String exchange,
                          String routingKey,
                          int retries);
    void disconnect();
    void sendMessage(Map<String, Object> messageHeaders, byte[] messageBody);
}

package org.janelia.messaging.core;

import java.util.Map;

public interface MessageSender {
    void connect(String host,
                 String user,
                 String password,
                 String exchange,
                 String routingKey);
    void disconnect();
    void sendMessage(Map<String, Object> messageHeaders, byte[] messageBody);
}

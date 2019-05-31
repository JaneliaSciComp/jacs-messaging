package org.janelia.messaging.core;

import java.util.Map;

public interface MessageSender {
    void connectTo(String exchange, String routingKey);
    void disconnect();
    boolean isConnected();
    void sendMessage(Map<String, Object> messageHeaders, byte[] messageBody);
}

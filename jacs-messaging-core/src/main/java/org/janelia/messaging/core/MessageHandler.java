package org.janelia.messaging.core;

import java.util.Map;

public interface MessageHandler {

    interface HandlerCallback {
        void callback(Map<String, Object> messageHeaders, byte[] messageBody);
    }

    void handleMessage(Map<String, Object> messageHeaders, byte[] messageBody);
    void cancelMessage(String routingTag);
}

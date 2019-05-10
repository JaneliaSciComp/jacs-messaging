package org.janelia.messaging.core;

import java.io.IOException;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageConsumer extends AbstractMessageConsumer {

    public MessageConsumer(ConnectionManager connectionManager) {
        super(connectionManager);
    }

    public MessageConsumer setupMessageHandler(MessageHandler messageHandler){
        if (channel == null) {
            throw new IllegalStateException("Channel has not been opened yet");
        } else {
            try {
                channel.basicConsume(getQueue(), isAutoAck(),
                        (consumerTag, delivery) -> messageHandler.handleMessage(delivery.getProperties().getHeaders(), delivery.getBody()),
                        (consumerTag) -> messageHandler.cancelMessage(consumerTag));
                return this;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

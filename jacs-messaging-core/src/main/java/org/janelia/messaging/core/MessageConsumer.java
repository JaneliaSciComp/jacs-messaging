package org.janelia.messaging.core;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageConsumer extends AbstractMessageConsumer {

    public MessageConsumer(ConnectionManager connectionManager) {
        super(connectionManager);
    }

    public MessageConsumer setupConsumerHandlers(DeliverCallback deliveryHandler, CancelCallback cancelHandler){
        if (channel == null) {
            throw new IllegalStateException("Channel has not been opened yet");
        } else {
            try {
                channel.basicConsume(queue, autoAck,
                        (consumerTag, delivery) -> deliveryHandler.handle(consumerTag, delivery),
                        (consumerTag) -> cancelHandler.handle(consumerTag));
                return this;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

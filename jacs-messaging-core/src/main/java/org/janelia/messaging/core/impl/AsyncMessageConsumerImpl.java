package org.janelia.messaging.core.impl;

import org.janelia.messaging.core.AsyncMessageConsumer;
import org.janelia.messaging.core.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by schauderd on 11/2/17.
 */
public class AsyncMessageConsumerImpl extends AbstractMessageConsumer implements AsyncMessageConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncMessageConsumer.class);

    public AsyncMessageConsumerImpl(MessageConnection messageConnection) {
        super(messageConnection);
    }

    public AsyncMessageConsumerImpl setupMessageHandler(MessageHandler messageHandler){
        if (channel == null) {
            throw new IllegalStateException("Channel has not been opened yet");
        } else {
            try {
                LOG.info("Connect to queue {} using autoAck set to {}", getQueue(), isAutoAck());
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

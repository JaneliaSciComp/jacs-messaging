package org.janelia.messaging.core.impl;

import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.AsyncMessageConsumer;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by schauderd on 11/2/17.
 */
public class AsyncMessageConsumerImpl extends AbstractMessageConsumer implements AsyncMessageConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncMessageConsumer.class);

    private String messageHandlerTag;

    public AsyncMessageConsumerImpl(MessageConnection messageConnection) {
        super((MessageConnectionImpl) messageConnection);
    }

    @Override
    public void disconnect() {
        if (messageConnection.isOpen() && StringUtils.isNotBlank(messageHandlerTag)) {
            try {
                messageConnection.channel.basicCancel(messageHandlerTag);
            } catch (IOException e) {
                LOG.error("Disconnect {}", messageHandlerTag);
            } finally {
                messageHandlerTag = null;
            }
        }
        super.disconnect();
    }

    public AsyncMessageConsumer setupMessageHandler(MessageHandler messageHandler){
        if (messageConnection.isOpen()) {
            try {
                LOG.info("Connect to queue {} using autoAck set to {}", getQueue(), isAutoAck());
                messageHandlerTag = messageConnection.channel.basicConsume(getQueue(), isAutoAck(),
                        (consumerTag, delivery) -> messageHandler.handleMessage(delivery.getProperties().getHeaders(), delivery.getBody()),
                        (consumerTag) -> messageHandler.cancelMessage(consumerTag));
                LOG.info("Connected handler {} to queue {} using autoAck set to {}", messageHandlerTag, getQueue(), isAutoAck());
                return this;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        } else {
            return null;
        }
    }
}

package org.janelia.messaging.core.impl;

import org.janelia.messaging.core.AsyncMessageConsumer;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by schauderd on 11/2/17.
 */
public class AsyncMessageConsumerImpl extends AbstractMessageConsumer implements AsyncMessageConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncMessageConsumer.class);

    private String messageHandlerTag;

    public AsyncMessageConsumerImpl(MessageConnection messageConnection) {
        super(messageConnection);
    }

    @Override
    public void disconnect() {
        messageConnection.cancelSubscription(messageHandlerTag);
        messageHandlerTag = null;
        super.disconnect();
    }

    public AsyncMessageConsumer subscribe(MessageHandler messageHandler){
        if (messageConnection.isOpen()) {
            messageHandlerTag = messageConnection.subscribe(getQueue(), isAutoAck(), messageHandler);
            LOG.info("Connected handler {} to queue {} using autoAck set to {}", messageHandlerTag, getQueue(), isAutoAck());
            return this;
        } else {
            return null;
        }
    }
}

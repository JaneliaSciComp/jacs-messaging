package org.janelia.messaging.core.impl;

import org.janelia.messaging.core.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by schauderd on 11/2/17.
 */
abstract class AbstractMessageConsumer implements MessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMessageConsumer.class);

    final MessageConnectionImpl messageConnection;
    private String queue;
    private boolean autoAck;

    AbstractMessageConsumer(MessageConnectionImpl messageConnection) {
        this.messageConnection = messageConnection;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public AbstractMessageConsumer setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
        return this;
    }

    String getQueue() {
        return queue;
    }

    @Override
    public void connectTo(String queueName) {
        if (messageConnection.isOpen()) {
            LOG.info("Connect to queue {}", queueName);
            this.queue = queueName;
        }
    }

    @Override
    public void disconnect() {
        this.queue = null;
    }

    @Override
    public void bindAndConnectTo(String exchangeName, String routingKey, String queueName) {
        if (messageConnection.isOpen()) {
            this.queue = messageConnection.bindAndConnectTo(exchangeName, routingKey, queueName);
        }
    }

}

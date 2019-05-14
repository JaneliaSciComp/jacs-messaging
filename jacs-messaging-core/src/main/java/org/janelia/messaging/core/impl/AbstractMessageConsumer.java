package org.janelia.messaging.core.impl;

import com.rabbitmq.client.Channel;
import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by schauderd on 11/2/17.
 */
abstract class AbstractMessageConsumer implements MessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMessageConsumer.class);

    private final MessageConnection messageConnection;
    Channel channel;
    private String queue;
    private boolean autoAck;

    public AbstractMessageConsumer(MessageConnection messageConnection) {
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
        if (messageConnection.connection != null) {
            try {
                LOG.info("Connect to queue {}", queueName);
                this.channel = messageConnection.connection.createChannel();
                this.queue = queueName;
            } catch (IOException e) {
                LOG.error("Error connecting to {}", queueName, e);
                throw new IllegalStateException("Error connecting to " + queueName, e);
            }
        }
    }

    @Override
    public void bindAndConnectTo(String exchangeName, String routingKey, String queueName) {
        if (messageConnection.connection != null) {
            if (messageConnection.connection != null) {
                try {
                    this.channel = messageConnection.connection.createChannel();
                    if (StringUtils.isBlank(queueName)) {
                        this.queue = channel.queueDeclare().getQueue();
                        LOG.info("Connect to temporary queue {} bound to {} with routingKey {}", this.queue, exchangeName, routingKey);
                    } else {
                        this.queue = queueName;
                        LOG.info("Connect to queue {} bound to {} with routingKey {}", this.queue, exchangeName, routingKey);
                    }
                    channel.queueBind(this.queue, exchangeName, StringUtils.defaultIfBlank(routingKey, ""));
                } catch (IOException e) {
                    LOG.error("Error connecting and binding to exchange {}", exchangeName, e);
                    throw new IllegalStateException("Error connecting to " + exchangeName, e);
                }
            }
        }
    }

    @Override
    public void disconnect() {
        if (channel != null) {
            try {
                LOG.info("Close message consumer channel");
                channel.close();
            } catch (Exception e) {
                LOG.error("Error disconnecting from the queue {}", queue, e);
            } finally {
                channel = null;
            }
        }
    }
}

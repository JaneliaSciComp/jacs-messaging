package org.janelia.messaging.core.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by schauderd on 11/2/17.
 */
abstract class AbstractMessageConsumer implements MessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMessageConsumer.class);

    private final ConnectionManager connectionManager;
    Connection connection;
    Channel channel;
    private String queue;
    private boolean autoAck;

    public AbstractMessageConsumer(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
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
    public void connect(String host,
                        String user,
                        String password,
                        String queueName,
                        int threadPoolSize) {
        openChannel(host, user, password, threadPoolSize);
        this.queue = queueName;
    }

    @Override
    public void bindAndConnect(String host,
                               String user,
                               String password,
                               String exchangeName,
                               String routingKey,
                               int threadPoolSize) {
        openChannel(host, user, password, threadPoolSize);
        try {
            LOG.debug("Connect to exchange {}", exchangeName);
            this.queue = channel.queueDeclare().getQueue();
            channel.queueBind(this.queue, exchangeName, StringUtils.defaultIfBlank(routingKey, ""));
        } catch (Exception e) {
            LOG.error("Error connecting to exchange {}", exchangeName);
            throw new IllegalStateException("Error connecting to " + exchangeName, e);
        }
    }

    private void openChannel(String host,
                             String user,
                             String password,
                             int threadPoolSize) {
        try {
            LOG.info("Open connection to {} as user {}", host, user);
            connection = connectionManager.openConnection(host, user, password, threadPoolSize);
            channel = connection.createChannel();
        } catch (Exception e) {
            LOG.error("Error connecting to host {} with user {}", host, user);
            throw new IllegalStateException("Error connecting to host " + host, e);
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
                try {
                    connection.close();
                } catch (Exception ignore) {
                }
                connection = null;
            }
        }
    }
}

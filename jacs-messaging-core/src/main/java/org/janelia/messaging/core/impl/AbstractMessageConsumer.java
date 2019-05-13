package org.janelia.messaging.core.impl;

import com.rabbitmq.client.Channel;
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
    public AbstractMessageConsumer connect(String host,
                                           String user,
                                           String password,
                                           String queueName,
                                           int threadPoolSize,
                                           int retries) {
        try {
            LOG.debug("Connect to queue {}", queueName);
            channel = connectionManager.openChannel(host, user, password, threadPoolSize, retries);
            this.queue = queueName;
        } catch (Exception e) {
            LOG.error("Error connecting to queue {} after {} retries", queueName, retries);
            throw new IllegalStateException("Error connecting to " + queueName, e);
        }
        return this;
    }

    @Override
    public AbstractMessageConsumer bindAndConnect(String host,
                                                  String user,
                                                  String password,
                                                  String exchangeName,
                                                  String routingKey,
                                                  int threadPoolSize,
                                                  int retries) {
        try {
            LOG.debug("Connect to exchange {}", exchangeName);
            channel = connectionManager.openChannel(host, user, password, threadPoolSize, retries);
            // if no queue defined, get random queue and bind to this exchange
            this.queue = channel.queueDeclare().getQueue();
            channel.queueBind(this.queue, exchangeName, StringUtils.defaultIfBlank(routingKey, ""));
        } catch (Exception e) {
            LOG.error("Error connecting to exchange {} after {} retries", exchangeName, retries);
            throw new IllegalStateException("Error connecting to " + exchangeName, e);
        }
        return this;
    }

    @Override
    public void disconnect() {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                LOG.error("Error disconnecting from the exchange channel {}", this.queue, e);
            } finally {
                channel = null;
            }
        }
    }
}

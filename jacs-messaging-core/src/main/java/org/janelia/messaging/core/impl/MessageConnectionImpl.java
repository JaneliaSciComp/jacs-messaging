package org.janelia.messaging.core.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.MessageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageConnectionImpl implements MessageConnection {
    private static final Logger LOG = LoggerFactory.getLogger(MessageConnectionImpl.class);

    private final ConnectionFactory factory;
    Connection connection;
    Channel channel;

    public MessageConnectionImpl() {
        factory = new ConnectionFactory();
        factory.setConnectionTimeout(0);
        connection = null;
    }

    public boolean isOpen() {
        return channel != null;
    }

    public void openConnection(String host, String username, String password, int threadPoolSize) {
        LOG.info("Open connection to {} as {}", host, username);
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        try {
            this.connection = threadPoolSize > 0
                    ? factory.newConnection(Executors.newFixedThreadPool(threadPoolSize))
                    : factory.newConnection();
            this.channel = this.connection.createChannel();
        } catch (Exception e) {
            LOG.error("Error connecting to {} as {}", host, username, e);
            throw new IllegalStateException(e);
        }
    }

    public void closeConnection() {
        if (channel != null) {
            try {
                this.connection.close();
            } catch (IOException ignore) {
            } finally {
                channel = null;
            }
        }
        if (connection != null) {
            try {
                this.connection.close();
            } catch (IOException ignore) {
            } finally {
                connection = null;
            }
        }
    }

    @Override
    public String bindAndConnectTo(String exchangeName, String routingKey, String queueName) {
        if (isOpen()) {
            try {
                String declaredQueue;
                if (StringUtils.isBlank(queueName)) {
                    declaredQueue = channel.queueDeclare().getQueue();
                    LOG.info("Connect to temporary queue {} bound to {} with routingKey {}", declaredQueue, exchangeName, routingKey);
                } else {
                    declaredQueue = queueName;
                    LOG.info("Connect to queue {} bound to {} with routingKey {}", declaredQueue, exchangeName, routingKey);
                }
                channel.queueBind(declaredQueue, exchangeName, StringUtils.defaultIfBlank(routingKey, ""));
                return declaredQueue;
            } catch (IOException e) {
                LOG.error("Error connecting and binding to exchange {}", exchangeName, e);
                throw new IllegalStateException("Error connecting to " + exchangeName, e);
            }
        } else {
            return null;
        }
    }

}

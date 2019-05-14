package org.janelia.messaging.core.impl;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageConnection {
    private static final Logger LOG = LoggerFactory.getLogger(MessageConnection.class);

    private final ConnectionFactory factory;
    Connection connection;

    public MessageConnection() {
        factory = new ConnectionFactory();
        factory.setConnectionTimeout(0);
        connection = null;
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
        } catch (Exception e) {
            LOG.error("Error connecting to {} as {}", host, username, e);
            throw new IllegalStateException(e);
        }
    }

    public void closeConnection() {
        if (connection != null) {
            try {
                this.connection.close();
            } catch (IOException ignore) {
            } finally {
                connection = null;
            }
        }
    }
}

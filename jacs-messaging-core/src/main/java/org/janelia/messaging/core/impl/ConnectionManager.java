package org.janelia.messaging.core.impl;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

/**
 * Created by schauderd on 11/2/17.
 */
public class ConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    public static ConnectionManager getInstance() {
        return new ConnectionManager();
    }

    private final ConnectionFactory factory;

    private ConnectionManager() {
        factory = new ConnectionFactory();
        factory.setConnectionTimeout(0);
    }

    Connection openConnection(String host, String username, String password, int threadPoolSize) throws Exception {
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        return threadPoolSize > 0
                ? factory.newConnection(Executors.newFixedThreadPool(threadPoolSize))
                : factory.newConnection();
    }
}

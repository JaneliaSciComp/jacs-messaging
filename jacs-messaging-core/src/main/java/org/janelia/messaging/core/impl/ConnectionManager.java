package org.janelia.messaging.core.impl;

import com.rabbitmq.client.Channel;
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

    private static ConnectionManager connectionManagerInstance;

    public static ConnectionManager getInstance() {
        if (connectionManagerInstance == null) {
            connectionManagerInstance = new ConnectionManager();
        }
        return connectionManagerInstance;
    }

    private final ConnectionFactory factory;

    private ConnectionManager() {
        factory = new ConnectionFactory();
        factory.setConnectionTimeout(0);
    }

    Channel openChannel(String host, String username, String password, int threadPoolSize, int retries) throws Exception {
        int retry = 0;
        Connection conn;
        for (;;) {
            try {
                conn = openConnection(host, username, password, threadPoolSize);
                return conn.createChannel();
            } catch (Exception e) {
                retry++;
                if (retries > 0 && retry < retries) {
                    LOG.warn("Error opening a connection after {} trials ({} retries left)", retry, retries - retry);
                    Thread.sleep(1000);
                } else {
                    throw e;
                }
            }
        }
    }

    private Connection openConnection(String host, String username, String password, int threadPoolSize) throws Exception {
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        return threadPoolSize > 0
                ? factory.newConnection(Executors.newFixedThreadPool(threadPoolSize))
                : factory.newConnection();
    }
}

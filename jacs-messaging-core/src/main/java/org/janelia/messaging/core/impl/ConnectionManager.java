package org.janelia.messaging.core.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by schauderd on 11/2/17.
 */
public class ConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    private final ConnectionFactory factory;
    private final ExecutorService executorService;

    public ConnectionManager() {
        this(0);
    }

    public ConnectionManager(int threadPoolSize) {
        factory = new ConnectionFactory();
        factory.setConnectionTimeout(0);
        executorService = threadPoolSize > 0 ? Executors.newFixedThreadPool(threadPoolSize) : null;
    }

    Channel openChannel(String host, String username, String password, int retries) throws Exception {
        int retry = 0;
        Connection conn;
        for (;;) {
            try {
                conn = openConnection(host, username, password);
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

    private Connection openConnection(String host, String username, String password) throws Exception {
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        return executorService != null
                ? factory.newConnection(executorService)
                : factory.newConnection();
    }
}

package org.janelia.messaging.client;

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
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    ConnectionFactory factory;
    Channel channel;
    ExecutorService executorService = null;

    static ConnectionManager connManager;

    private ConnectionManager() {
    }

    public void setThreadPoolSize(int threadPoolSize) {
        executorService = Executors.newFixedThreadPool(threadPoolSize);
    }
    
    public static ConnectionManager getInstance() {
        if (connManager==null) 
            connManager = new ConnectionManager();
        return connManager;
    }

    public void configureTarget (String host, String username, String password) {
        if (factory==null) {
            factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setConnectionTimeout(0);
        }
    }

    public Channel getConnection(int retries) throws Exception {
        if (channel!=null && channel.isOpen()) {
            return channel;
        }
        int retry = 0;
        Connection conn;
        for (;;) {
            try {
                conn = openConnection();
                break;
            } catch (Exception e) {
                retry++;
                if (retries > 0 && retry < retries) {
                    log.warn("Error opening a connection after {} trials ({} retries left)", retry, retries - retry);
                    Thread.sleep(1000);
                } else {
                    throw e;
                }
            }
        };
        channel = conn.createChannel();
        return channel;
    }

    private Connection openConnection() throws Exception {
        return executorService!=null
                ? factory.newConnection(executorService)
                : factory.newConnection();
    }
}

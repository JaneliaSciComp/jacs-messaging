package org.janelia.messaging.client;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by schauderd on 11/2/17.
 */
public class ConnectionManager {
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

    public Channel getConnection() throws Exception {
        if (channel!=null && channel.isOpen()) {
            return channel;
        }
        Connection conn;
        if (executorService!=null) {
            conn = factory.newConnection(executorService);
        } else {
            conn = factory.newConnection();
        }
        channel = conn.createChannel();
        return channel;
    }

}

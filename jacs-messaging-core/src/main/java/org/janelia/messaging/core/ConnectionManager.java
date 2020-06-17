package org.janelia.messaging.core;

import java.util.function.Consumer;

import org.janelia.messaging.core.impl.MessageConnectionImpl;
import org.janelia.messaging.core.impl.RetriedMessageConnectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);
    private static ConnectionManager instance = new ConnectionManager();

    public static ConnectionManager getInstance() {
        return instance;
    }

    private ConnectionManager() {
    }

    public MessageConnection getConnection() {
        return new MessageConnectionImpl();
    }

    public MessageConnection getConnection(String host, String user, String password, int threadPoolSize, Consumer<Throwable> connectionErrorHandler) {
        return getConnection(
                new ConnectionParameters()
                        .setHost(host)
                        .setUser(user)
                        .setPassword(password)
                        .setMaxRetries(1)
                        .setConsumerThreads(threadPoolSize),
                connectionErrorHandler
        );
    }

    public MessageConnection getConnection(ConnectionParameters connectionParameters, Consumer<Throwable> connectionErrorHandler) {
        LOG.info("Creating connection with max retries {}, pauseBetweenRetries {}, and consumer threads {}",
                connectionParameters.maxRetries,
                connectionParameters.pauseBetweenRetriesInMillis,
                connectionParameters.consumerThreads);
        MessageConnection messageConnection = new RetriedMessageConnectionImpl(getConnection(),
                connectionParameters.maxRetries,
                connectionParameters.pauseBetweenRetriesInMillis);
        try {
            messageConnection.openConnection(
                    connectionParameters.host,
                    connectionParameters.user,
                    connectionParameters.password,
                    connectionParameters.consumerThreads);
        } catch (Exception e) {
            connectionErrorHandler.accept(e);
        }
        return messageConnection;
    }
}

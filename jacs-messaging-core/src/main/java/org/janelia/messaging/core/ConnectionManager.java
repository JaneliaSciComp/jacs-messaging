package org.janelia.messaging.core;

import org.janelia.messaging.core.impl.MessageConnectionImpl;
import org.janelia.messaging.core.impl.RetriedMessageConnectionImpl;

public class ConnectionManager {
    private static ConnectionManager instance = new ConnectionManager();

    public static ConnectionManager getInstance() {
        return instance;
    }

    private ConnectionManager() {
    }

    public MessageConnection getConnection(String host, String user, String password, int threadPoolSize) {
        return getConnection(
                new ConnectionParameters()
                        .setHost(host)
                        .setUser(user)
                        .setPassword(password)
                        .setMaxRetries(1)
                        .setConsumerThreads(threadPoolSize)
        );
    }

    public MessageConnection getConnection(ConnectionParameters connectionParameters) {
        MessageConnection messageConnection = new RetriedMessageConnectionImpl(new MessageConnectionImpl(),
                connectionParameters.maxRetries,
                connectionParameters.pauseBetweenRetriesInMillis);
        messageConnection.openConnection(
                connectionParameters.host,
                connectionParameters.user,
                connectionParameters.password,
                connectionParameters.consumerThreads);
        return messageConnection;
    }
}

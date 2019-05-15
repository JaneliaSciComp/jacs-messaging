package org.janelia.messaging.core;

import org.janelia.messaging.core.impl.MessageConnectionImpl;

public class ConnectionManager {
    private static ConnectionManager instance = new ConnectionManager();

    public static ConnectionManager getInstance() {
        return instance;
    }

    private ConnectionManager() {
    }

    public MessageConnection getConnection(String host, String user, String password, int threadPoolSize) {
        MessageConnection messageConnection = new MessageConnectionImpl();
        messageConnection.openConnection(host, user, password, threadPoolSize);
        return messageConnection;
    }

}

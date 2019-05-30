package org.janelia.messaging.core;

public class ConnectionParameters {
    String host;
    String user;
    String password;
    int maxRetries = 1;
    long pauseBetweenRetriesInMillis = 1000;
    int consumerThreads;

    public ConnectionParameters() {
    }

    public ConnectionParameters setHost(String host) {
        this.host = host;
        return this;
    }

    public ConnectionParameters setUser(String user) {
        this.user = user;
        return this;
    }

    public ConnectionParameters setPassword(String password) {
        this.password = password;
        return this;
    }

    public ConnectionParameters setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public ConnectionParameters setPauseBetweenRetriesInMillis(long pauseBetweenRetriesInMillis) {
        this.pauseBetweenRetriesInMillis = pauseBetweenRetriesInMillis;
        return this;
    }

    public ConnectionParameters setConsumerThreads(int consumerThreads) {
        this.consumerThreads = consumerThreads;
        return this;
    }
}

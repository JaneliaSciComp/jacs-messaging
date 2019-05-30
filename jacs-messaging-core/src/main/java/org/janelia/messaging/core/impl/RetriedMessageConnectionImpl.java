package org.janelia.messaging.core.impl;

import java.util.Map;
import java.util.stream.Stream;

import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetriedMessageConnectionImpl implements MessageConnection {

    private static final Logger LOG = LoggerFactory.getLogger(RetriedMessageConnectionImpl.class);
    private static final long MIN_PAUSE_BETWEEN_RETRIES = 100L;

    private final MessageConnection proxy;
    private final int maxRetries;
    private final long pauseBetweenRetriesInMillis;

    public RetriedMessageConnectionImpl(MessageConnection proxy,
                                        int maxRetries,
                                        long pauseBetweenRetriesInMillis) {
        this.proxy = proxy;
        this.maxRetries = maxRetries;
        this.pauseBetweenRetriesInMillis = pauseBetweenRetriesInMillis > MIN_PAUSE_BETWEEN_RETRIES ? pauseBetweenRetriesInMillis : MIN_PAUSE_BETWEEN_RETRIES;
    }

    @Override
    public boolean isOpen() {
        return proxy.isOpen();
    }

    @Override
    public void openConnection(String host, String username, String password, int threadPoolSize) {
        int nRetries = 0;
        for (;;) {
            nRetries++;
            try {
                proxy.openConnection(host, username, password, threadPoolSize);
                LOG.info("Opened connection to {} as user {} after {} attempts", host, username, nRetries);
                break;
            } catch (RetryableStateException e) {
                LOG.info("Open connection to {} as user {} failed after {} attempts - remaining attempts {}",
                        host, username, nRetries, maxRetries > 0 ? String.valueOf(maxRetries - nRetries) : "unlimited");
            }
            if (maxRetries > 0 && nRetries >= maxRetries) {
                LOG.error("No connection could be opened to {} as user {} after {} attempts", host, username, nRetries);
                throw new IllegalStateException("Message connection could not be opened to host " + host + " after " + nRetries + " attempts");
            }
            try {
                Thread.sleep(pauseBetweenRetriesInMillis);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    @Override
    public void closeConnection() {
        proxy.closeConnection();
    }

    @Override
    public String bindAndConnectTo(String exchangeName, String routingKey, String queueName) {
        return proxy.bindAndConnectTo(exchangeName, routingKey, queueName);
    }

    @Override
    public void publish(String exchange, String routingKey, Map<String, Object> headers, byte[] body) {
        proxy.publish(exchange, routingKey, headers, body);
    }

    @Override
    public String subscribe(String queue, boolean ack, MessageHandler messageHandler) {
        return proxy.subscribe(queue, ack, messageHandler);
    }

    @Override
    public void cancelSubscription(String subscriptionTag) {
        proxy.cancelSubscription(subscriptionTag);
    }

    @Override
    public Stream<GenericMessage> retrieveMessages(String queue, boolean ack) {
        return proxy.retrieveMessages(queue, ack);
    }
}

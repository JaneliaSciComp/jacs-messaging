package org.janelia.messaging.core.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.utils.MessagingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageConnectionImpl implements MessageConnection {
    private static final Logger LOG = LoggerFactory.getLogger(MessageConnectionImpl.class);

    private final ConnectionFactory factory;
    Connection connection;
    Channel channel;

    public MessageConnectionImpl() {
        factory = new ConnectionFactory();
        factory.setConnectionTimeout(0);
        connection = null;
    }

    public boolean isOpen() {
        return channel != null;
    }

    public void openConnection(String host, String username, String password, int threadPoolSize) {
        LOG.info("Try to open connection to {} as {}", host, username);
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        try {
            this.connection = threadPoolSize > 0
                    ? factory.newConnection(Executors.newFixedThreadPool(threadPoolSize))
                    : factory.newConnection();
            this.channel = this.connection.createChannel();
        } catch (Exception e) {
            LOG.debug("Error connecting to {} as {}", host, username, e);
            throw new RetryableStateException(e);
        }
    }

    public void closeConnection() {
        if (connection != null) {
            if (channel != null) {
                try {
                    this.channel.close();
                } catch (Exception ignore) {
                } finally {
                    channel = null;
                }
            }
            try {
                this.connection.close();
            } catch (IOException ignore) {
            } finally {
                connection = null;
            }
        }
    }

    @Override
    public String bindAndConnectTo(String exchangeName, String routingKey, String queueName) {
        if (isNotOpen()) {
            throw new IllegalStateException("Connection must be opened before binding " + exchangeName + " with " + routingKey + " to " + queueName);
        }
        try {
            String declaredQueue;
            if (StringUtils.isBlank(queueName)) {
                declaredQueue = channel.queueDeclare().getQueue();
                LOG.info("Connect to temporary queue {} bound to {} with routingKey {}", declaredQueue, exchangeName, routingKey);
            } else {
                declaredQueue = queueName;
                LOG.info("Connect to queue {} bound to {} with routingKey {}", declaredQueue, exchangeName, routingKey);
            }
            channel.queueBind(declaredQueue, exchangeName, StringUtils.defaultIfBlank(routingKey, ""));
            return declaredQueue;
        } catch (IOException e) {
            LOG.error("Error connecting and binding to exchange {}", exchangeName, e);
            throw new IllegalStateException("Error connecting to " + exchangeName, e);
        }
    }

    @Override
    public void publish(String exchange, String routingKey, Map<String, Object> headers, byte[] body) {
        if (isNotOpen()) {
            throw new IllegalStateException("Connection must be opened before publishing a message to " + exchange + " with " + routingKey);
        }
        try {
            LOG.info("Send message {} to {} with routingKey {}", headers, exchange, routingKey);
            channel.basicPublish(exchange, routingKey,
                    new AMQP.BasicProperties.Builder()
                            .headers(headers)
                            .build(), body);
        } catch (Exception e) {
            LOG.error("Error publishing message {} to the exchange {} with routingKey {}", headers, exchange, routingKey);
        }
    }

    @Override
    public String subscribe(String queue, boolean ack, MessageHandler messageHandler) {
        if (isNotOpen()) {
            throw new IllegalStateException("Connection must be opened before subscribing to " + queue);
        }
        try {
            LOG.info("Connect to queue {} using autoAck set to {}", queue, ack);
            return channel.basicConsume(queue, ack,
                    (consumerTag, delivery) -> messageHandler.handleMessage(delivery.getProperties().getHeaders(), delivery.getBody()),
                    (consumerTag) -> messageHandler.cancelMessage(consumerTag));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void cancelSubscription(String subscriptionTag) {
        if (isNotOpen()) {
            throw new IllegalStateException("Connection must be opened for canceling subscription " + subscriptionTag);
        }
        if (StringUtils.isNotBlank(subscriptionTag)) {
            try {
                LOG.info("Cancel subscription {}", subscriptionTag);
                channel.basicCancel(subscriptionTag);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public Stream<GenericMessage> retrieveMessages(String queue, boolean ack) {
        if (isNotOpen()) {
            throw new IllegalStateException("Connection must be opened for retrieving messages from " + queue);
        }
        Spliterator<GenericMessage> messageSupplier = new Spliterator<GenericMessage>() {
            GetResponse lastResponse = null;
            @Override
            public boolean tryAdvance(Consumer<? super GenericMessage> action) {
                try {
                    if (isNotOpen()) {
                        LOG.info("The message channel was closed");
                        return false;
                    }
                    lastResponse = channel.basicGet(queue, ack);
                    if (lastResponse == null) {
                        return false;
                    }
                    GenericMessage message = new GenericMessage(
                            getMessageHeadersFromResponse(lastResponse.getProps().getHeaders()),
                            lastResponse.getBody());
                    action.accept(message);
                    return lastResponse.getMessageCount() > 0;
                } catch (IOException e) {
                    LOG.error("Error retrieving message from {}", queue, e);
                    return false;
                }
            }

            @Override
            public Spliterator<GenericMessage> trySplit() {
                return null; // no supported
            }

            @Override
            public long estimateSize() {
                if (lastResponse == null) {
                    return Long.MAX_VALUE;
                } else {
                    return lastResponse.getMessageCount();
                }
            }

            @Override
            public int characteristics() {
                return ORDERED;
            }
        };
        return StreamSupport.stream(messageSupplier, false);
    }

    private Map<String, Object> getMessageHeadersFromResponse(Map<String, Object> responseHeaders) {
        Map<String, Object> messageHeaders = new HashMap<>();
        if (responseHeaders != null) {
            // no filtering
            responseHeaders.forEach((k, v) -> messageHeaders.put(k, MessagingUtils.getHeaderValue(responseHeaders, k)));
        }
        return messageHeaders;
    }

}

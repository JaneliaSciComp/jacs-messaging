package org.janelia.messaging.core.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.janelia.messaging.core.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageSenderImpl implements MessageSender {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSenderImpl.class);

    private final ConnectionManager connectionManager;

    private Channel channel;
    private String exchange;
    private String routingKey;

    public MessageSenderImpl(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public MessageSenderImpl connect(String host,
                                     String user,
                                     String password,
                                     String exchange, String routingKey, int retries) {
        this.exchange = exchange;
        this.routingKey = routingKey;

        // get a channel from the connectionManager
        try {
            channel = connectionManager.openChannel(host, user, password, retries);
            return this;
        } catch (Exception e) {
            LOG.error("Error connecting to {} {} after {} retries", exchange, routingKey, retries, e);
            throw new IllegalStateException("Error connecting to " + exchange, e);
        }
    }

    @Override
    public void disconnect() {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                LOG.error("Error disconnecting from the exchange {}", exchange, e);
            }
        }
    }

    @Override
    public void sendMessage(Map<String, Object> messageHeaders, byte[] messageBody) {
        if (channel != null) {
            try {
                channel.basicPublish(exchange, routingKey,
                        new AMQP.BasicProperties.Builder()
                                .headers(messageHeaders)
                                .build(), messageBody);
                LOG.info("Message {} sent to {} with routingKey {}", messageHeaders, exchange, routingKey);
            } catch (Exception e) {
                LOG.error("Error publishing message {} to the exchange {} with routingKey {}", messageHeaders, exchange, routingKey);
            }
        }
    }

}

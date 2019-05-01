package org.janelia.messaging.core;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageSender {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSender.class);

    private final ConnectionManager connectionManager;

    private Channel channel;
    private String exchange;
    private String routingKey;

    public MessageSender(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public MessageSender connect(String exchange, String routingKey, int retries) {
        this.exchange = exchange;
        this.routingKey = routingKey;

        // get a channel from the connectionManager
        try {
            channel = connectionManager.openChannel(retries);
            return this;
        } catch (Exception e) {
            LOG.error("Error connecting to {} {} after {} retries", exchange, routingKey, retries, e);
            throw new IllegalStateException("Error connecting to " + exchange, e);
        }
    }

    public void disconnect() {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                LOG.error("Error disconnecting from the exchange {}", exchange, e);
            }
        }
    }

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

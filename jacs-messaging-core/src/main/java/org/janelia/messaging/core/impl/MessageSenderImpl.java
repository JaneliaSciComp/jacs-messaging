package org.janelia.messaging.core.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.janelia.messaging.core.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageSenderImpl implements MessageSender {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSenderImpl.class);

    private final MessageConnection messageConnection;

    private Channel channel;
    private String exchange;
    private String routingKey;

    public MessageSenderImpl(MessageConnection messageConnection) {
        this.messageConnection = messageConnection;
    }

    @Override
    public void connectTo(String exchange, String routingKey) {
        if (messageConnection.connection != null) {
            try {
                this.channel = messageConnection.connection.createChannel();
                this.exchange = exchange;
                this.routingKey = routingKey;
            } catch (IOException e) {
                LOG.error("Error connecting to {} with routingkey {}", exchange, routingKey, e);
                throw new IllegalStateException("Error connecting to " + exchange, e);
            }
        }
    }

    @Override
    public void disconnect() {
        if (channel != null) {
            try {
                LOG.info("Close message sender channel");
                channel.close();
            } catch (Exception e) {
                LOG.error("Error disconnecting from the exchange {}", exchange, e);
            } finally {
                channel = null;
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

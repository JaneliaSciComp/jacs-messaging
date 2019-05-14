package org.janelia.messaging.core.impl;

import com.rabbitmq.client.AMQP;
import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageSenderImpl implements MessageSender {
    private static final Logger LOG = LoggerFactory.getLogger(MessageSenderImpl.class);

    private final MessageConnectionImpl messageConnection;

    private String exchange;
    private String routingKey;

    public MessageSenderImpl(MessageConnection messageConnection) {
        this.messageConnection = (MessageConnectionImpl) messageConnection;
    }

    @Override
    public void connectTo(String exchange, String routingKey) {
        if (messageConnection.isOpen() && StringUtils.isNotBlank(exchange)) {
            LOG.info("Connect to {} with key {}", exchange, routingKey);
            this.exchange = exchange;
            this.routingKey = routingKey;
        }
    }

    @Override
    public void disconnect() {
        exchange = null;
        routingKey = null;
    }

    @Override
    public void sendMessage(Map<String, Object> messageHeaders, byte[] messageBody) {
        if (messageConnection.isOpen() && StringUtils.isNotBlank(exchange)) {
            try {
                LOG.info("Send message {} to {} with routingKey {}", messageHeaders, exchange, routingKey);
                messageConnection.channel.basicPublish(exchange, routingKey,
                        new AMQP.BasicProperties.Builder()
                                .headers(messageHeaders)
                                .build(), messageBody);
            } catch (Exception e) {
                LOG.error("Error publishing message {} to the exchange {} with routingKey {}", messageHeaders, exchange, routingKey);
            }
        }
    }

}

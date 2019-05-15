package org.janelia.messaging.core.impl;

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

    private final MessageConnection messageConnection;

    private String exchange;
    private String routingKey;

    public MessageSenderImpl(MessageConnection messageConnection) {
        this.messageConnection = messageConnection;
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
        if (StringUtils.isNotBlank(exchange)) {
            try {
                LOG.info("Send message {} to {} with routingKey {}", messageHeaders, exchange, routingKey);
                messageConnection.publish(exchange, routingKey, messageHeaders, messageBody);
            } catch (Exception e) {
                LOG.error("Error publishing message {} to the exchange {} with routingKey {}", messageHeaders, exchange, routingKey);
            }
        }
    }

}

package org.janelia.messaging.core.impl;

import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        LOG.info("Setup sender's exchange '{}' with key '{}'", exchange, routingKey);
        Preconditions.checkArgument(StringUtils.isNotBlank(exchange),
                "The exchange to connect cannot be null or blank");
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    @Override
    public void disconnect() {
        exchange = null;
        routingKey = null;
    }

    @Override
    public boolean isConnected() {
        return messageConnection.isOpen();
    }

    @Override
    public void sendMessage(Map<String, Object> messageHeaders, byte[] messageBody) {
        Preconditions.checkArgument(messageConnection.isOpen(),
                "The connection must be open before trying to send a message");
        try {
            messageConnection.publish(exchange, routingKey, messageHeaders, messageBody);
        } catch (Exception e) {
            LOG.error("Error publishing message {} to the exchange {} with routingKey {}", messageHeaders, exchange, routingKey, e);
        }
    }

}

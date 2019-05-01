package org.janelia.messaging.core;

import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;
import org.janelia.messaging.utils.MessagingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by schauderd on 11/2/17.
 */
public class OnDemandMessageConsumer extends AbstractMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(OnDemandMessageConsumer.class);

    public OnDemandMessageConsumer(ConnectionManager connectionManager) {
        super(connectionManager);
    }

    public List<GenericMessage> retrieveMessages(MessageFilter messageFilter, int maxLength) {
        if (channel == null) {
            throw new IllegalStateException("Channel has not been opened yet");
        } else {
            List<GenericMessage> messageList = new ArrayList<>();
            try {
                for (;;) {
                    GetResponse response = channel.basicGet(queue, isAutoAck());
                    if (response == null) {
                        break;
                    }
                    GenericMessage message = new GenericMessage(
                            filterHeaders(response.getProps().getHeaders(), messageFilter.getHeaderNames()),
                            response.getBody());
                    if (messageFilter.acceptMessage(message)) {
                        messageList.add(message);
                    }
                    if (response.getMessageCount() == 0) {
                        // nothing left at the time the last message was retrieved so we stop here for now
                        break;
                    }
                    if (maxLength > 0 && messageList.size() == maxLength) {
                        break;
                    }
                }
                return messageList;
            } catch (IOException e) {
                LOG.error("Error retrieving message from {}", queue, e);
                throw new IllegalStateException(e);
            }
        }

    }

    private Map<String, String> filterHeaders(Map<String, Object> headers, Set<String> requiredHeaders) {
        Map<String, String> newHeaders = new HashMap<>();
        requiredHeaders.forEach(header -> {
            newHeaders.put(header, MessagingUtils.convertLongString((LongString) headers.get(header)));
        });
        return newHeaders;
    }
}

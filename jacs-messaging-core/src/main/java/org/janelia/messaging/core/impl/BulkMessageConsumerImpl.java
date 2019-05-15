package org.janelia.messaging.core.impl;

import org.janelia.messaging.core.BulkMessageConsumer;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.utils.MessagingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Created by schauderd on 11/2/17.
 */
public class BulkMessageConsumerImpl extends AbstractMessageConsumer implements BulkMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(BulkMessageConsumerImpl.class);

    public BulkMessageConsumerImpl(MessageConnection messageConnection) {
        super(messageConnection);
    }

    @Override
    public void disconnect() {
        // do nothing
    }

    @Override
    public Stream<GenericMessage> retrieveMessages() {
        return messageConnection.retrieveMessages(getQueue(), isAutoAck());
    }

    private Map<String, String> filterHeaders(Map<String, Object> headers, Set<String> requiredHeaders) {
        Map<String, String> newHeaders = new HashMap<>();
        if (requiredHeaders == null) {
            // no filtering
            headers.forEach((k, v) -> newHeaders.put(k, MessagingUtils.valueAsString(v)));
        } else {
            requiredHeaders.forEach(header -> {
                newHeaders.put(header, MessagingUtils.getHeaderAsString(headers, header));
            });
        }
        return newHeaders;
    }
}

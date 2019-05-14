package org.janelia.messaging.core.impl;

import com.rabbitmq.client.GetResponse;
import org.janelia.messaging.core.BulkMessageConsumer;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.utils.MessagingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by schauderd on 11/2/17.
 */
public class BulkMessageConsumerImpl extends AbstractMessageConsumer implements BulkMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(BulkMessageConsumerImpl.class);

    public BulkMessageConsumerImpl(MessageConnection messageConnection) {
        super((MessageConnectionImpl) messageConnection);
    }

    @Override
    public void disconnect() {
        // do nothing
    }

    @Override
    public Stream<GenericMessage> retrieveMessages(Set<String> messageHeaders) {
        Spliterator<GenericMessage> messageSupplier = new Spliterator<GenericMessage>() {
            GetResponse lastResponse = null;
            @Override
            public boolean tryAdvance(Consumer<? super GenericMessage> action) {
                try {
                    if (!messageConnection.isOpen()) {
                        LOG.info("The message channel was either closed or never opened");
                        return false;
                    }
                    lastResponse = messageConnection.channel.basicGet(getQueue(), isAutoAck());
                    if (lastResponse == null) {
                        return false;
                    }
                    GenericMessage message = new GenericMessage(
                            filterHeaders(lastResponse.getProps().getHeaders(), messageHeaders),
                            lastResponse.getBody()
                    );
                    action.accept(message);
                    return lastResponse.getMessageCount() > 0;
                } catch (IOException e) {
                    LOG.error("Error retrieving message from {}", getQueue(), e);
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

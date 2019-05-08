package org.janelia.messaging.core;

import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;
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
public class BulkMessageConsumer extends AbstractMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(BulkMessageConsumer.class);

    public BulkMessageConsumer(ConnectionManager connectionManager) {
        super(connectionManager);
    }

    public Stream<GenericMessage> retrieveMessages(Set<String> messageHeaders) {
        Spliterator<GenericMessage> messageSupplier = new Spliterator<GenericMessage>() {
            GetResponse lastResponse = null;
            @Override
            public boolean tryAdvance(Consumer<? super GenericMessage> action) {
                try {
                    if (channel == null) {
                        LOG.info("The message channel was either closed or never opened");
                        return false;
                    }
                    lastResponse = channel.basicGet(queue, isAutoAck());
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

    private Map<String, String> filterHeaders(Map<String, Object> headers, Set<String> requiredHeaders) {
        Map<String, String> newHeaders = new HashMap<>();
        if (requiredHeaders == null) {
            // no filtering
            headers.forEach((k, v) -> newHeaders.put(k, MessagingUtils.convertLongString((LongString) v)));
        } else {
            requiredHeaders.forEach(header -> {
                newHeaders.put(header, MessagingUtils.convertLongString((LongString) headers.get(header)));
            });
        }
        return newHeaders;
    }
}

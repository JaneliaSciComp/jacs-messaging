package org.janelia.messaging.neuronbroker;

import com.rabbitmq.client.CancelCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class MessageDeliveryErrorHandler implements CancelCallback {
    private static final Logger LOG = LoggerFactory.getLogger(MessageDeliveryErrorHandler.class);

    @Override
    public void handle(String consumerTag) throws IOException {
        // process failed message handling, redirecting to dead-letter queue
        LOG.error("MESSAGE DELIVERY FAILED {}", consumerTag);
    }
}

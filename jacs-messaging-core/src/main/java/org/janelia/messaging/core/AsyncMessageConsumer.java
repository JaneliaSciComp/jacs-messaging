package org.janelia.messaging.core;

public interface AsyncMessageConsumer extends MessageConsumer {
    AsyncMessageConsumer subscribe(MessageHandler messageHandler);
}

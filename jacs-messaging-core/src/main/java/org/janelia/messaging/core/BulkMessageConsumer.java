package org.janelia.messaging.core;

import java.util.Set;
import java.util.stream.Stream;

public interface BulkMessageConsumer extends MessageConsumer {
    Stream<GenericMessage> retrieveMessages(Set<String> messageHeaders);
}

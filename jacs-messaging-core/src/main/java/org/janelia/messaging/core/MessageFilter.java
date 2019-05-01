package org.janelia.messaging.core;

import java.util.Set;

public interface MessageFilter {
    Set<String> getHeaderNames();
    boolean acceptMessage(GenericMessage message);
}

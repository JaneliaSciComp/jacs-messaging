package org.janelia.messaging.core.impl;

public class RetryableStateException extends IllegalStateException {

    public RetryableStateException(Throwable cause) {
        super(cause);
    }

}

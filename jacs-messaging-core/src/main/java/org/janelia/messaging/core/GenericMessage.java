package org.janelia.messaging.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Created by schauderd on 3/21/18.
 */
public class GenericMessage {
    private final byte[] body;
    private final Map<String, Object> headers;

    @JsonCreator
    public GenericMessage(@JsonProperty("headers") Map<String, Object> headers,
                          @JsonProperty("body") byte[] body) {
        this.headers = headers;
        this.body = body;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }
}

package org.janelia.messaging.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Created by schauderd on 3/21/18.
 */
public class GenericMessage {
    private final byte[] body;
    private final Map<String, String> headers;

    @JsonCreator
    GenericMessage(@JsonProperty("headers") Map<String, String> headers,
                   @JsonProperty("body") byte[] body) {
        this.headers = headers;
        this.body = body;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }
}

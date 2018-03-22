package org.janelia.messaging.client;

import java.util.Map;

/**
 * Created by schauderd on 3/21/18.
 */
public class VanillaBean {
    VanillaBean() {
    }
    byte[] body;
    Map<String,Object> headers;

    VanillaBean(Map<String,Object> headers, byte[] body) {
        this.headers = headers;
        this.body = body;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}

package org.janelia.messaging.broker;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A web client providing access to the Tiled Microscope REST Service.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class AbstractRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRestClient.class);

    protected final String serverURL;
    private final String apiKey;
    protected final WebTarget serverTarget;

    protected AbstractRestClient(String serverURL, String apiKey) {
        this.serverURL = StringUtils.appendIfMissing(serverURL, "/");
        this.apiKey = apiKey;
        JacksonJsonProvider provider = new JacksonJaxbJsonProvider();
        ObjectMapper mapper = provider.locateMapper(Object.class, MediaType.APPLICATION_JSON_TYPE);
        mapper.addHandler(new DeserializationProblemHandler() {
            @Override
            public boolean handleUnknownProperty(DeserializationContext ctxt, JsonParser jp, JsonDeserializer<?> deserializer, Object beanOrClass, String propertyName) {
                LOG.error("Failed to deserialize property which does not exist in model: {}.{}", beanOrClass.getClass().getName(), propertyName);
                return true;
            }
        });

        ClientConfig clientConfig = new ClientConfig();
        // values are in milliseconds
        clientConfig.property(ClientProperties.READ_TIMEOUT, 2000);
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 5000);

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(100);
        connectionManager.setDefaultMaxPerRoute(100);

        clientConfig.property(ApacheClientProperties.CONNECTION_MANAGER, connectionManager);

        Client client = ClientBuilder.newClient();
        client.register(provider);
        client.register(MultiPartFeature.class);

        serverTarget = client.target(serverURL);
    }

    protected Invocation.Builder createRequestWithCredentials(WebTarget requestTarget) {
        LOG.info("Create request {}", requestTarget);
        if (StringUtils.isNotBlank(apiKey)) {
            return requestTarget.request().header(
                    "Authorization",
                    "APIKEY " + apiKey);
        } else {
            return requestTarget.request();
        }
    }

    protected boolean checkResponse(Response response, String failureError) {
        int responseStatus = response.getStatus();
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if (responseStatus < 200 || responseStatus >= 300) {
            LOG.error("Server responded with error code: {} {} -> {}", response.getStatus(), status, failureError);
            return true;
        }
        return false;
    }

}

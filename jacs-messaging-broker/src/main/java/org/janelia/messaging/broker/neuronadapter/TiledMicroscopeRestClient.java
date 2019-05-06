package org.janelia.messaging.broker.neuronadapter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.MultiPartMediaTypes;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A web client providing access to the Tiled Microscope REST Service.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class TiledMicroscopeRestClient {

    private static final Logger log = LoggerFactory.getLogger(TiledMicroscopeRestClient.class);

    private static final String REMOTE_MOUSELIGHT_DATA_PREFIX = "mouselight/data";
    private static final String REMOTE_DOMAIN_PERMISSIONS_PREFIX = "data/user/permissions";

    private final Client client;
    private String remoteApiURL;

    TiledMicroscopeRestClient(String remoteApiURL) {
        log.info("Using server URL: {}", remoteApiURL);
        this.remoteApiURL = remoteApiURL;
        JacksonJsonProvider provider = new JacksonJaxbJsonProvider();
        ObjectMapper mapper = provider.locateMapper(Object.class, MediaType.APPLICATION_JSON_TYPE);
        mapper.addHandler(new DeserializationProblemHandler() {
            @Override
            public boolean handleUnknownProperty(DeserializationContext ctxt, JsonParser jp, JsonDeserializer<?> deserializer, Object beanOrClass, String propertyName) throws IOException, JsonProcessingException {
                log.error("Failed to deserialize property which does not exist in model: {}.{}", beanOrClass.getClass().getName(), propertyName);
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

        this.client = ClientBuilder.newClient();
        client.register(provider);
        client.register(MultiPartFeature.class);
    }

    WebTarget getMouselightEndpoint(String suffix, String subjectKey) {
        log.info("Endpoint target: {}", remoteApiURL + REMOTE_MOUSELIGHT_DATA_PREFIX + suffix);
        return client.target(remoteApiURL + REMOTE_MOUSELIGHT_DATA_PREFIX + suffix)
                .queryParam("subjectKey", subjectKey);
    }

    WebTarget getDomainPermissionsEndpoint() {
        log.info("Endpoint target: {}", remoteApiURL + REMOTE_DOMAIN_PERMISSIONS_PREFIX);
        return client.target(remoteApiURL + REMOTE_DOMAIN_PERMISSIONS_PREFIX);
    }

    List<TmNeuronMetadata> getNeuronMetadata(List<String> neuronIds, String subjectKey) throws Exception {
        String parsedNeuronIds = StringUtils.join(neuronIds, ",");
        Response response = getMouselightEndpoint("/neuron/metadata", subjectKey)
                .queryParam("neuronIds", parsedNeuronIds)
                .request()
                .header("username", subjectKey)
                .get();
        if (checkBadResponse(response, "getNeuronMetadata: " + neuronIds)) {
            response.close();
            throw new WebApplicationException(response);
        }
        List<TmNeuronMetadata> list = response.readEntity(new GenericType<List<TmNeuronMetadata>>() {
        });
        return list;
    }

    TmNeuronMetadata createMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        FormDataMultiPart multiPart = new FormDataMultiPart()
                .field("neuronMetadata", neuronMetadata, MediaType.APPLICATION_JSON_TYPE);
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .request()
                .header("username", subjectKey)
                .put(Entity.entity(multiPart, multiPart.getMediaType()));
        if (checkBadResponse(response, "createMetadata: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    TmSample getSampleForWorkspace(Long workspaceId, String subjectKey) throws Exception {
        log.info("Workspace request on {}", workspaceId);
        Response response = getMouselightEndpoint("/workspace/{workspaceId}", subjectKey)
                .resolveTemplate("workspaceId", workspaceId)
                .request("application/json")
                .header("username", subjectKey)
                .get();
        if (checkBadResponse(response, "getTmWorkspace")) {
            response.close();
            throw new WebApplicationException(response);
        }
        TmWorkspace workspace = response.readEntity(TmWorkspace.class);
        if (workspace != null) {
            log.info("Sample ID request on {}", workspace.getSampleId());
            response = getMouselightEndpoint("/sample/{sampleId}", subjectKey)
                    .resolveTemplate("sampleId", workspace.getSampleId())
                    .request("application/json")
                    .header("username", subjectKey)
                    .get();
            if (checkBadResponse(response, "getTmSample")) {
                response.close();
                throw new WebApplicationException(response);
            }
        } else {
            return null;
        }
        return response.readEntity(TmSample.class);
    }

    TmNeuronMetadata create(TmNeuronMetadata neuronMetadata, InputStream protobufStream, String subjectKey) throws Exception {
        FormDataMultiPart multiPart = new FormDataMultiPart()
                .field("neuronMetadata", neuronMetadata, MediaType.APPLICATION_JSON_TYPE)
                .field("protobufBytes", protobufStream, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .request()
                .header("username", subjectKey)
                .put(Entity.entity(multiPart, multiPart.getMediaType()));
        if (checkBadResponse(response, "create: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    TmNeuronMetadata updateMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        return update(neuronMetadata, null, subjectKey);
    }

    TmNeuronMetadata update(TmNeuronMetadata neuronMetadata, InputStream protobufStream, String subjectKey) throws Exception {
        List<TmNeuronMetadata> list = update(Arrays.asList(Pair.of(neuronMetadata, protobufStream)), subjectKey);
        if (list.isEmpty()) return null;
        if (list.size() > 1) log.warn("update(TmNeuronMetadata) returned more than one result.");
        return list.get(0);
    }

    List<TmNeuronMetadata> updateMetadata(List<TmNeuronMetadata> neuronList, String subjectKey) throws Exception {
        List<Pair<TmNeuronMetadata, InputStream>> pairs = new ArrayList<>();
        for (TmNeuronMetadata tmNeuronMetadata : neuronList) {
            pairs.add(Pair.of(tmNeuronMetadata, (InputStream) null));
        }
        return update(pairs, subjectKey);
    }

    List<TmNeuronMetadata> update(Collection<Pair<TmNeuronMetadata, InputStream>> neuronPairs, String subjectKey) throws Exception {
        if (neuronPairs.isEmpty()) return Collections.emptyList();
        MultiPart multiPartEntity = new MultiPart();
        for (Pair<TmNeuronMetadata, InputStream> neuronPair : neuronPairs) {
            multiPartEntity.bodyPart(new BodyPart(neuronPair.getLeft(), MediaType.APPLICATION_JSON_TYPE));
            if (neuronPair.getRight() != null) {
                multiPartEntity.bodyPart(new BodyPart(neuronPair.getRight(), MediaType.APPLICATION_OCTET_STREAM_TYPE));
            } else {
                multiPartEntity.bodyPart(new BodyPart("", MediaType.TEXT_PLAIN_TYPE));
            }
        }

        String logStr = null;
        if (neuronPairs.size() == 1) {
            TmNeuronMetadata neuron = neuronPairs.iterator().next().getLeft();
            logStr = neuron == null ? "null neuron" : neuron.toString();
        } else {
            logStr = neuronPairs.size() + " neurons";
        }

        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .request()
                .header("username", subjectKey)
                .post(Entity.entity(multiPartEntity, MultiPartMediaTypes.MULTIPART_MIXED));
        if (checkBadResponse(response, "update: " + logStr)) {
            response.close();
            throw new WebApplicationException(response);
        }

        List<TmNeuronMetadata> list = response.readEntity(new GenericType<List<TmNeuronMetadata>>() {
        });
        return list;
    }

    void remove(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .queryParam("neuronId", neuronMetadata.getId())
                .request()
                .header("username", subjectKey)
                .delete();
        if (checkBadResponse(response.getStatus(), "remove: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }
    }

    TmNeuronMetadata setPermissions(String subjectKey, TmNeuronMetadata neuron, String newOwner) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("targetClass", TmNeuronMetadata.class.getName());
        params.put("targetId", neuron.getId());
        params.put("granteeKey", newOwner);
        params.put("rights", "rw");
        params.put("subjectKey", subjectKey);
        Response response = getDomainPermissionsEndpoint()
                .request("application/json")
                .header("username", subjectKey)
                .put(Entity.json(params));
        if (checkBadResponse(response.getStatus(), "problem making request changePermissions to server: " + neuron + "," + newOwner)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    private boolean checkBadResponse(Response response, String failureError) {
        int responseStatus = response.getStatus();
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if (responseStatus < 200 || responseStatus >= 300) {
            log.error("Problem making request for {}", failureError);
            // TODO: we want to print the request URI here, but I don't have time to search through the JAX-RS APIs right now
            log.error("Server responded with error code: {} {}", response.getStatus(), status);
            return true;
        }
        return false;
    }

    private boolean checkBadResponse(int responseStatus, String failureError) {
        if (responseStatus < 200 || responseStatus >= 300) {
            log.error("ERROR RESPONSE: " + responseStatus);
            log.error(failureError);
            return true;
        }
        return false;
    }
}

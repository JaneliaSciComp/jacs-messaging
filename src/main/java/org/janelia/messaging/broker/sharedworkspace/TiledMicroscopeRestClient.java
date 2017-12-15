package org.janelia.messaging.broker.sharedworkspace;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.*;

/**
 * A web client providing access to the Tiled Microscope REST Service.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class TiledMicroscopeRestClient {

    private static final Logger log = LoggerFactory.getLogger(TiledMicroscopeRestClient.class);

 private static final String REMOTE_MOUSELIGHT_DATA_PREFIX = "mouselight/data";

    private final Client client;
    private String REMOTE_API_URL;

    public TiledMicroscopeRestClient(String REMOTE_API_URL) {
        log.info("Using server URL: {}",REMOTE_API_URL);
        this.REMOTE_API_URL = REMOTE_API_URL;
        JacksonJsonProvider provider = new JacksonJaxbJsonProvider();
        ObjectMapper mapper = provider.locateMapper(Object.class, MediaType.APPLICATION_JSON_TYPE);
        mapper.addHandler(new DeserializationProblemHandler() {
            @Override
            public boolean handleUnknownProperty(DeserializationContext ctxt, JsonParser jp, JsonDeserializer<?> deserializer, Object beanOrClass, String propertyName) throws IOException, JsonProcessingException {
                log.error("Failed to deserialize property which does not exist in model: {}.{}",beanOrClass.getClass().getName(),propertyName);
                return true;
            }
        });
        this.client = ClientBuilder.newClient();
        client.register(provider);
        client.register(MultiPartFeature.class);
    }

    public WebTarget getMouselightEndpoint(String suffix, String subjectKey) {
        log.info("Endpoint target: {}", REMOTE_API_URL + REMOTE_MOUSELIGHT_DATA_PREFIX + suffix);
        return client.target(REMOTE_API_URL + REMOTE_MOUSELIGHT_DATA_PREFIX + suffix)
                .queryParam("subjectKey", subjectKey);
    }

    public List<TmNeuronMetadata> getNeuronMetadata(List<String> neuronIds, String subjectKey) throws Exception {
        Response response = getMouselightEndpoint("/neuron/metadata", subjectKey)
                .queryParam("neuronId", neuronIds)
                .request("multipart/mixed")
                .get();
        if (checkBadResponse(response, "getNeuronMetadata: "+neuronIds)) {
            throw new WebApplicationException(response);
        }
        List<TmNeuronMetadata> list = response.readEntity(new GenericType<List<TmNeuronMetadata>>() {});
        return list;
    }

    public TmNeuronMetadata createMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        FormDataMultiPart multiPart = new FormDataMultiPart()
                .field("neuronMetadata", neuronMetadata, MediaType.APPLICATION_JSON_TYPE);
        Response response = getMouselightEndpoint("/workspace/neuron",subjectKey)
                .request()
                .put(Entity.entity(multiPart, multiPart.getMediaType()));
        if (checkBadResponse(response, "createMetadata: "+neuronMetadata)) {
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    public TmNeuronMetadata create(TmNeuronMetadata neuronMetadata, InputStream protobufStream,  String subjectKey) throws Exception {
        FormDataMultiPart multiPart = new FormDataMultiPart()
                .field("neuronMetadata", neuronMetadata, MediaType.APPLICATION_JSON_TYPE)
                .field("protobufBytes", protobufStream, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        Response response = getMouselightEndpoint("/workspace/neuron",subjectKey)
                .request()
                .put(Entity.entity(multiPart, multiPart.getMediaType()));
        if (checkBadResponse(response, "create: "+neuronMetadata)) {
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    public TmNeuronMetadata updateMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        return update(neuronMetadata, null, subjectKey);
    }

    public TmNeuronMetadata update(TmNeuronMetadata neuronMetadata, InputStream protobufStream, String subjectKey) throws Exception {
       List<TmNeuronMetadata> list = update(Arrays.asList(Pair.of(neuronMetadata, protobufStream)), subjectKey);
       if (list.isEmpty()) return null;
       if (list.size()>1) log.warn("update(TmNeuronMetadata) returned more than one result.");
       return list.get(0);
    }

    public List<TmNeuronMetadata> updateMetadata(List<TmNeuronMetadata> neuronList, String subjectKey) throws Exception {
        List<Pair<TmNeuronMetadata,InputStream>> pairs = new ArrayList<>();
        for(TmNeuronMetadata tmNeuronMetadata : neuronList) {
            pairs.add(Pair.of(tmNeuronMetadata, (InputStream)null));
        }
        return update(pairs, subjectKey);
    }

    public List<TmNeuronMetadata> update(Collection<Pair<TmNeuronMetadata,InputStream>> neuronPairs, String subjectKey) throws Exception {
        if (neuronPairs.isEmpty()) return Collections.emptyList();
        MultiPart multiPartEntity = new MultiPart();
        for (Pair<TmNeuronMetadata, InputStream> neuronPair : neuronPairs) {
            multiPartEntity.bodyPart(new BodyPart(neuronPair.getLeft(), MediaType.APPLICATION_JSON_TYPE));
            if (neuronPair.getRight()!=null) {
                multiPartEntity.bodyPart(new BodyPart(neuronPair.getRight(), MediaType.APPLICATION_OCTET_STREAM_TYPE));
            }
            else {
                multiPartEntity.bodyPart(new BodyPart("", MediaType.TEXT_PLAIN_TYPE));
            }
        }
        
        String logStr = null;
        if (neuronPairs.size()==1) {
            TmNeuronMetadata neuron = neuronPairs.iterator().next().getLeft();
            logStr = neuron==null?"null neuron":neuron.toString();
        }
        else {
            logStr = neuronPairs.size()+" neurons";
        }
        
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .request()
                .post(Entity.entity(multiPartEntity, MultiPartMediaTypes.MULTIPART_MIXED));
        if (checkBadResponse(response, "update: " +logStr)) {
            throw new WebApplicationException(response);
        }

        List<TmNeuronMetadata> list = response.readEntity(new GenericType<List<TmNeuronMetadata>>() {});
        return list;
    }
    
    public void remove(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .queryParam("neuronId", neuronMetadata.getId())
                .request()
                .delete();
        if (checkBadResponse(response.getStatus(), "remove: " + neuronMetadata)) {
            throw new WebApplicationException(response);
        }
    }

    protected boolean checkBadResponse(Response response, String failureError) {
        int responseStatus = response.getStatus();
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if (responseStatus<200 || responseStatus>=300) {
            log.error("Problem making request for {}", failureError);
            // TODO: we want to print the request URI here, but I don't have time to search through the JAX-RS APIs right now
            log.error("Server responded with error code: {} {}",response.getStatus(), status);
            return true;
        }
        return false;
    }

    protected boolean checkBadResponse(int responseStatus, String failureError) {
        if (responseStatus<200 || responseStatus>=300) {
            log.error("ERROR RESPONSE: " + responseStatus);
            log.error(failureError);
            return true;
        }
        return false;
    }
}

package org.janelia.messaging.broker.neuronadapter;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartMediaTypes;
import org.janelia.messaging.broker.AbstractRestClient;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A web client providing access to the Tiled Microscope REST Service.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class TiledMicroscopeRestClient extends AbstractRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(TiledMicroscopeRestClient.class);

    private static final String REMOTE_MOUSELIGHT_DATA_PREFIX = "mouselight/data";
    private static final String REMOTE_DOMAIN_PERMISSIONS_PREFIX = "data/user/permissions";

    TiledMicroscopeRestClient(String remoteApiURL, String apiKey) {
        super(remoteApiURL, apiKey);
    }

    private WebTarget getMouselightEndpoint(String suffix, String subjectKey) {
        LOG.info("Endpoint target: {}", serverURL + REMOTE_MOUSELIGHT_DATA_PREFIX + suffix);
        return serverTarget.path(REMOTE_MOUSELIGHT_DATA_PREFIX + suffix)
                .queryParam("subjectKey", subjectKey);
    }

    private WebTarget getDomainPermissionsEndpoint() {
        LOG.info("Endpoint target: {}", serverURL + REMOTE_DOMAIN_PERMISSIONS_PREFIX);
        return serverTarget.path(REMOTE_DOMAIN_PERMISSIONS_PREFIX);
    }

    List<TmNeuronMetadata> getNeuronMetadata(List<String> neuronIds, String subjectKey) {
        String parsedNeuronIds = StringUtils.join(neuronIds, ",");
        Response response = getMouselightEndpoint("/neuron/metadata", subjectKey)
                .queryParam("neuronIds", parsedNeuronIds)
                .request()
                .header("username", subjectKey)
                .get();
        if (checkResponse(response, "getNeuronMetadata: " + neuronIds)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(new GenericType<List<TmNeuronMetadata>>() {});
    }

    TmNeuronMetadata createMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) {
        FormDataMultiPart multiPart = new FormDataMultiPart()
                .field("neuronMetadata", neuronMetadata, MediaType.APPLICATION_JSON_TYPE);
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .request()
                .header("username", subjectKey)
                .put(Entity.entity(multiPart, multiPart.getMediaType()));
        if (checkResponse(response, "createMetadata: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    TmSample getSampleForWorkspace(Long workspaceId, String subjectKey) {
        LOG.info("Workspace request on {}", workspaceId);
        Response response = getMouselightEndpoint("/workspace/{workspaceId}", subjectKey)
                .resolveTemplate("workspaceId", workspaceId)
                .request("application/json")
                .header("username", subjectKey)
                .get();
        if (checkResponse(response, "getTmWorkspace")) {
            response.close();
            throw new WebApplicationException(response);
        }
        TmWorkspace workspace = response.readEntity(TmWorkspace.class);
        if (workspace != null) {
            LOG.info("Sample ID request on {}", workspace.getSampleId());
            response = getMouselightEndpoint("/sample/{sampleId}", subjectKey)
                    .resolveTemplate("sampleId", workspace.getSampleId())
                    .request("application/json")
                    .header("username", subjectKey)
                    .get();
            if (checkResponse(response, "getTmSample")) {
                response.close();
                throw new WebApplicationException(response);
            }
            return response.readEntity(TmSample.class);
        } else {
            return null;
        }
    }

    TmNeuronMetadata create(TmNeuronMetadata neuronMetadata, InputStream protobufStream, String subjectKey) {
        FormDataMultiPart multiPart = new FormDataMultiPart()
                .field("neuronMetadata", neuronMetadata, MediaType.APPLICATION_JSON_TYPE)
                .field("protobufBytes", protobufStream, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .request()
                .header("username", subjectKey)
                .put(Entity.entity(multiPart, multiPart.getMediaType()));
        if (checkResponse(response, "create: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    TmNeuronMetadata updateMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) {
        return update(neuronMetadata, null, subjectKey);
    }

    TmNeuronMetadata update(TmNeuronMetadata neuronMetadata, InputStream protobufStream, String subjectKey) {
        List<TmNeuronMetadata> list = update(Arrays.asList(Pair.of(neuronMetadata, protobufStream)), subjectKey);
        if (list.isEmpty()) return null;
        if (list.size() > 1) LOG.warn("update(TmNeuronMetadata) returned more than one result.");
        return list.get(0);
    }

    List<TmNeuronMetadata> updateMetadata(List<TmNeuronMetadata> neuronList, String subjectKey) {
        List<Pair<TmNeuronMetadata, InputStream>> pairs = new ArrayList<>();
        for (TmNeuronMetadata tmNeuronMetadata : neuronList) {
            pairs.add(Pair.of(tmNeuronMetadata, null));
        }
        return update(pairs, subjectKey);
    }

    private List<TmNeuronMetadata> update(Collection<Pair<TmNeuronMetadata, InputStream>> neuronPairs, String subjectKey) {
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

        String logStr;
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
        if (checkResponse(response, "update: " + logStr)) {
            response.close();
            throw new WebApplicationException(response);
        }

        return response.readEntity(new GenericType<List<TmNeuronMetadata>>() {});
    }

    void remove(TmNeuronMetadata neuronMetadata, String subjectKey) {
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .queryParam("neuronId", neuronMetadata.getId())
                .request()
                .header("username", subjectKey)
                .delete();
        if (checkResponse(response, "remove: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }
    }

    TmNeuronMetadata setPermissions(String subjectKey, TmNeuronMetadata neuron, String newOwner) {
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
        if (checkResponse(response, "problem making request changePermissions to server: " + neuron + "," + newOwner)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

}

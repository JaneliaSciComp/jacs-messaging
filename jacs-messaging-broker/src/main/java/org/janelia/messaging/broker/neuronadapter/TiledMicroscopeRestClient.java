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
import org.janelia.model.domain.DomainConstants;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.janelia.model.domain.workspace.TreeNode;
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

    List<TmNeuronMetadata> getNeuronMetadata(String workspaceId, List<String> neuronIds, String subjectKey) {
        String parsedNeuronIds = StringUtils.join(neuronIds, ",");
        Response response = getMouselightEndpoint("/neuron/metadata", subjectKey)
                .queryParam("workspaceId", workspaceId)
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

    TmSample getSample (Long sampleId, String subjectKey) {
        LOG.info("Sample ID request on {}", sampleId);
        Response response = getMouselightEndpoint("/sample/{sampleId}", subjectKey)
                .resolveTemplate("sampleId", sampleId)
                .request("application/json")
                .header("username", subjectKey)
                .get();
        if (checkResponse(response, "getTmSample")) {
            response.close();
            throw new WebApplicationException(response);
        }
        return null;
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

    public TmSample create(TmSample tmSample, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setSubjectKey(subjectKey);
        query.setDomainObject(tmSample);
        WebTarget target = getMouselightEndpoint("/sample", subjectKey);
        Response response = target
                .request("application/json")
                .put(Entity.json(query));
        if (checkResponse(response, "create: " + tmSample)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmSample.class);
    }

    public TmWorkspace create(TmWorkspace tmWorkspace, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setSubjectKey(subjectKey);
        query.setDomainObject(tmWorkspace);
        WebTarget target = getMouselightEndpoint("/workspace", subjectKey);
        Response response = target
                .request("application/json")
                .put(Entity.json(query));
        if (checkResponse(response, "create: " + tmWorkspace)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmWorkspace.class);
    }

    public TmNeuronMetadata create(TmNeuronMetadata neuronMetadata, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setDomainObject(neuronMetadata);
        query.setSubjectKey(subjectKey);
        WebTarget target =  getMouselightEndpoint("/workspace/neuron", subjectKey);
        Response response = target
                .request()
                .header("username", subjectKey)
                .put(Entity.json(query));
        if (checkResponse(response, "create: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    public TmSample update(TmSample tmSample, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setDomainObject(tmSample);
        query.setSubjectKey(subjectKey);
        WebTarget target = getMouselightEndpoint("/sample",subjectKey);
        Response response = target
                .request("application/json")
                .post(Entity.json(query));
        if (checkResponse(response, "update: " + tmSample)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmSample.class);
    }

    public TmWorkspace update(TmWorkspace tmWorkspace, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setDomainObject(tmWorkspace);
        query.setSubjectKey(subjectKey);
        WebTarget target = getMouselightEndpoint("/workspace", subjectKey);
        Response response = target
                .request("application/json")
                .post(Entity.json(query));
        if (checkResponse(response, "update: " + tmWorkspace)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmWorkspace.class);
    }

    TmNeuronMetadata update(TmNeuronMetadata neuronMetadata, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setDomainObject(neuronMetadata);
        query.setSubjectKey(subjectKey);

        LOG.info("Connecting to REST server to persist ownership change {}",neuronMetadata.getId());
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .queryParam("subjectKey",subjectKey)
                .request()
                .header("username", subjectKey)
                .post(Entity.json(query));
        if (checkResponse(response, "update: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }

        LOG.info("Finished update of neuron's owner {}",neuronMetadata.getId());

        return response.readEntity(TmNeuronMetadata.class);
    }


    public List<TmNeuronMetadata> updateNeurons(Collection<TmNeuronMetadata> neurons, String subjectKey) {
        if (neurons.isEmpty()) return Collections.emptyList();

        String logStr = neurons.size() + " neurons";

        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .queryParam("subjectKey",subjectKey)
                .request()
                .header("username", subjectKey)
                .post(Entity.json(neurons));
        if (checkResponse(response, "update: " + logStr)) {
            response.close();
            throw new WebApplicationException(response);
        }

        return response.readEntity(new GenericType<List<TmNeuronMetadata>>() {});
    }

    void remove(TmNeuronMetadata neuronMetadata, String subjectKey) {
        Response response = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .queryParam("workspaceId", neuronMetadata.getWorkspaceId())
                .queryParam("neuronId", neuronMetadata.getId())
                .queryParam("isLarge", neuronMetadata.isLargeNeuron())
                .request()
                .header("username", subjectKey)
                .delete();
        if (checkResponse(response, "remove: " + neuronMetadata)) {
            response.close();
            throw new WebApplicationException(response);
        }
    }

    public void remove(TmSample tmSample, String subjectKey) {
        WebTarget target = getMouselightEndpoint("/sample", subjectKey)
                .queryParam("sampleId", tmSample.getId());
        Response response = target
                .request("application/json")
                .delete();
        if (checkResponse(response, "delete: " + tmSample)) {
            response.close();
            throw new WebApplicationException(response);
        }
    }

    public void remove(TmWorkspace tmWorkspace, String subjectKey) {
        WebTarget target = getMouselightEndpoint("/workspace", subjectKey)
                .queryParam("workspaceId", tmWorkspace.getId());
        Response response = target
                .request("application/json")
                .delete();
        if (checkResponse(response, "delete: " + tmWorkspace)) {
            response.close();
            throw new WebApplicationException(response);
        }
    }

    public TmWorkspace save(TmWorkspace tmWorkspace, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setSubjectKey(subjectKey);
        query.setDomainObject(tmWorkspace);
        WebTarget target = getMouselightEndpoint("/workspace", subjectKey);
        Response response = target
                .request("application/json")
                .put(Entity.json(query));
        if (checkResponse(response, "create: " + tmWorkspace)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmWorkspace.class);
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

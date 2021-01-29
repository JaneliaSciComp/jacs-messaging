package org.janelia.messaging.broker.agentadapter;

import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.broker.AbstractRestClient;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.TmAgentMetadata;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * A web client for tracking requests and information needed to integrate
 * machine learning agents with the workstation
 *
 */
class AgentTrackingClient extends AbstractRestClient {
    private static final Logger LOG = LoggerFactory.getLogger(AgentTrackingClient.class);

    private static final String REMOTE_MOUSELIGHT_DATA_PREFIX = "mouselight/data";

    AgentTrackingClient(String remoteApiURL, String apiKey) {
        super(remoteApiURL, apiKey);
    }

    private WebTarget getMouselightEndpoint(String suffix, String subjectKey) {
        LOG.info("Endpoint target: {}", serverURL + REMOTE_MOUSELIGHT_DATA_PREFIX + suffix);
        return serverTarget.path(REMOTE_MOUSELIGHT_DATA_PREFIX + suffix)
                .queryParam("subjectKey", subjectKey);
    }

    TmAgentMetadata getAgentMetadata(Long workspaceId, String subjectKey) {
        WebTarget agentEndpoint = getMouselightEndpoint("/agent/{workspaceId}", subjectKey)
                .resolveTemplate("workspaceId", workspaceId);
        Response response = agentEndpoint
                .request()
                .get();
        if (isErrorResponse(agentEndpoint.getUri(), response)) {
            response.close();
            LOG.error("Error retrieving agent metadata from workspace {} for {}", workspaceId, subjectKey);
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmAgentMetadata.class);
    }

    List<TmNeuronMetadata> getNeuronMetadata(String workspaceId, List<String> neuronIds, String subjectKey) {
        String parsedNeuronIds = StringUtils.join(neuronIds, ",");
        WebTarget neuronMetadataTarget = getMouselightEndpoint("/neuron/metadata", subjectKey)
                .queryParam("workspaceId", workspaceId)
                .queryParam("neuronIds", parsedNeuronIds);
        Response response = neuronMetadataTarget
                .request()
                .header("username", subjectKey)
                .get();
        if (isErrorResponse(neuronMetadataTarget.getUri(), response)) {
            response.close();
            LOG.error("Error retrieving neurons {} from workspace {} for {}", neuronIds, workspaceId, subjectKey);
            throw new WebApplicationException(response);
        }
        return response.readEntity(new GenericType<List<TmNeuronMetadata>>() {});
    }

    TmSample getSampleForWorkspace(Long workspaceId, String subjectKey) {
        LOG.info("Workspace request on {}", workspaceId);
        WebTarget workspaceEndpoint = getMouselightEndpoint("/workspace/{workspaceId}", subjectKey)
                .resolveTemplate("workspaceId", workspaceId);
        Response workspaceResponse = workspaceEndpoint
                .request("application/json")
                .header("username", subjectKey)
                .get();
        if (isErrorResponse(workspaceEndpoint.getUri(), workspaceResponse)) {
            workspaceResponse.close();
            LOG.error("Error retrieving workspace {} for {}", workspaceId, subjectKey);
            throw new WebApplicationException(workspaceResponse);
        }
        TmWorkspace workspace = workspaceResponse.readEntity(TmWorkspace.class);
        if (workspace != null) {
            LOG.info("Sample ID request on {}", workspace.getSampleId());
            WebTarget sampleEndpoint = getMouselightEndpoint("/sample/{sampleId}", subjectKey)
                    .resolveTemplate("sampleId", workspace.getSampleId());
            Response sampleResponse = sampleEndpoint
                    .request("application/json")
                    .header("username", subjectKey)
                    .get();
            if (isErrorResponse(sampleEndpoint.getUri(), sampleResponse)) {
                sampleResponse.close();
                throw new WebApplicationException(sampleResponse);
            }
            return sampleResponse.readEntity(TmSample.class);
        } else {
            return null;
        }
    }

    TmNeuronMetadata create(TmNeuronMetadata neuronMetadata, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setDomainObject(neuronMetadata);
        query.setSubjectKey(subjectKey);
        WebTarget target =  getMouselightEndpoint("/workspace/neuron", subjectKey);
        Response response = target
                .request()
                .header("username", subjectKey)
                .put(Entity.json(query));
        if (isErrorResponse(target.getUri(), response)) {
            response.close();
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmNeuronMetadata.class);
    }

    TmNeuronMetadata update(TmNeuronMetadata neuronMetadata, String subjectKey) {
        DomainQuery query = new DomainQuery();
        query.setDomainObject(neuronMetadata);
        query.setSubjectKey(subjectKey);
        WebTarget target = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .queryParam("subjectKey",subjectKey);
        Response response = target
                .request()
                .header("username", subjectKey)
                .post(Entity.json(query));
        if (isErrorResponse(target.getUri(), response)) {
            response.close();
            throw new WebApplicationException(response);
        }

        return response.readEntity(TmNeuronMetadata.class);
    }

    TmWorkspace createWorkspaceForSample(Long sampleId, String workspaceName, String subjectKey) {
        LOG.info("Workspace creation request for sample {}, using name {}",
                sampleId, workspaceName);
        TmWorkspace newWorkspace = new TmWorkspace(workspaceName, sampleId);

        DomainQuery createRequest = new DomainQuery();
        createRequest.setSubjectKey(subjectKey);
        createRequest.setDomainObject(newWorkspace);
        WebTarget workspaceEndpoint = getMouselightEndpoint("/workspace", subjectKey);
        Response response = workspaceEndpoint
                .request("application/json")
                .put(Entity.json(createRequest));
        if (isErrorResponse(workspaceEndpoint.getUri(), response)) {
            response.close();
            LOG.error("Error creating workspace {} for sample {}", workspaceName,
                    sampleId);
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmWorkspace.class);
    }

    TmAgentMetadata createAgentMapping(Long workspaceId, TmAgentMetadata agentMetadata, String subjectKey) {
        LOG.info("creating new Agent mapping for workspace {}",
                workspaceId);
        DomainQuery createRequest = new DomainQuery();
        createRequest.setSubjectKey(subjectKey);
        createRequest.setDomainObject(agentMetadata);
        WebTarget workspaceEndpoint = getMouselightEndpoint("/agent", subjectKey);
        Response response = workspaceEndpoint
                .request("application/json")
                .put(Entity.json(createRequest));
        if (isErrorResponse(workspaceEndpoint.getUri(), response)) {
            response.close();
            LOG.error("Error creating new Agent mapping for workspace {}", workspaceId);
            throw new WebApplicationException(response);
        }
        return response.readEntity(TmAgentMetadata.class);
    }
}

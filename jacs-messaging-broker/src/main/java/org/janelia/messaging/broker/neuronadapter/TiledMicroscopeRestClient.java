package org.janelia.messaging.broker.neuronadapter;

import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.broker.AbstractRestClient;
import org.janelia.model.domain.dto.DomainQuery;
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

    TiledMicroscopeRestClient(String remoteApiURL, String apiKey) {
        super(remoteApiURL, apiKey);
    }

    private WebTarget getMouselightEndpoint(String suffix, String subjectKey) {
        LOG.info("Endpoint target: {}", serverURL + REMOTE_MOUSELIGHT_DATA_PREFIX + suffix);
        return serverTarget.path(REMOTE_MOUSELIGHT_DATA_PREFIX + suffix)
                .queryParam("subjectKey", subjectKey);
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

    void createOperationLog(Long workspaceId, Long neuronId,
                            String operationType, String timestamp, String subjectKey ) {
        LOG.info("operation log - {},{},{},{},{}", workspaceId, neuronId, operationType, timestamp, subjectKey);
        WebTarget target =  getMouselightEndpoint("/operation/log", subjectKey)
                .queryParam("username", subjectKey)
                .queryParam("workspaceId", workspaceId)
                .queryParam("neuronId", neuronId)
                .queryParam("operationType", operationType)
                .queryParam("timestamp", timestamp);
        Response response = target
                .request("application/json")
                .get();
        if (isErrorResponse(target.getUri(), response)) {
            response.close();
            throw new WebApplicationException(response);
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

    void remove(TmNeuronMetadata neuronMetadata, String subjectKey) {
        WebTarget target = getMouselightEndpoint("/workspace/neuron", subjectKey)
                .queryParam("workspaceId", neuronMetadata.getWorkspaceId())
                .queryParam("neuronId", neuronMetadata.getId())
                .queryParam("isLarge", neuronMetadata.isLargeNeuron());
        Response response = target
                .request()
                .header("username", subjectKey)
                .delete();
        if (isErrorResponse(target.getUri(), response)) {
            response.close();
            throw new WebApplicationException(response);
        }
    }

}

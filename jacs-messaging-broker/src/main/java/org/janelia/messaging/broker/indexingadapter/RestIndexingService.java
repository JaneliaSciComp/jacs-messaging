package org.janelia.messaging.broker.indexingadapter;

import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.janelia.messaging.broker.AbstractRestClient;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RestIndexingService extends AbstractRestClient implements IndexingService {

    private static final Logger LOG = LoggerFactory.getLogger(RestIndexingService.class);
    private static final String INDEXING_ENDPOINT_PATH = "data/searchIndex";

    private final WebTarget endpointTarget;

    RestIndexingService(String serverURL, String apiKey) {
        super(serverURL, apiKey);
        endpointTarget = serverTarget.path(INDEXING_ENDPOINT_PATH);
    }

    @Override
    public void indexDocReferences(List<Reference> docReferences) {
        Response response = createRequestWithCredentials(endpointTarget)
                .post(Entity.entity(docReferences, MediaType.APPLICATION_JSON_TYPE));
        if (isErrorResponse(endpointTarget.getUri(), response)) {
            LOG.error("Errors occurred while indexing {} documents", docReferences.size());
        } else {
            LOG.info("Successfully indexed {} documents", docReferences.size());
        }
        response.close();
    }

    @Override
    public void remmoveDocIds(List<Long> docIds) {
        WebTarget target = endpointTarget.path("docsToRemove");
        Response response = createRequestWithCredentials(target)
                .post(Entity.entity(docIds, MediaType.APPLICATION_JSON_TYPE));
        if (isErrorResponse(target.getUri(), response)) {
            LOG.error("Errors occurred while removing {} documents", docIds.size());
        } else {
            LOG.info("Successfully removed {}", docIds);
        }
        response.close();
    }

    @Override
    public void addAncestorToDocs(Long ancestorId, List<Long> docIds) {
        WebTarget target = endpointTarget.path(ancestorId.toString()).path("descendants");
        Response response = createRequestWithCredentials(target)
                .put(Entity.entity(docIds, MediaType.APPLICATION_JSON_TYPE));
        if (isErrorResponse(target.getUri(), response)) {
            LOG.error("Errors occurred while adding ancestor {} to {}", ancestorId, docIds);
        } else {
            LOG.info("Successfully added ancestor {} to {}", ancestorId, docIds);
        }
        response.close();
    }

}

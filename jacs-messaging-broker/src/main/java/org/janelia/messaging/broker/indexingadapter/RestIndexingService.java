package org.janelia.messaging.broker.indexingadapter;

import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.janelia.messaging.broker.AbstractRestClient;
import org.janelia.model.domain.Reference;

class RestIndexingService extends AbstractRestClient implements IndexingService {

    private static final String INDEXING_ENDPOINT_PATH = "data/searchIndex";

    private final WebTarget endpointTarget;

    RestIndexingService(String serverURL, String apiKey) {
        super(serverURL, apiKey);
        endpointTarget = serverTarget.path(INDEXING_ENDPOINT_PATH);
    }

    @Override
    public void indexDocReferences(List<Reference> docReferences) {
        Response response = createRequestWithCredentials(
                endpointTarget)
                .post(Entity.entity(docReferences, MediaType.APPLICATION_JSON_TYPE));
        checkResponse(response, "index documents: " + docReferences);
        response.close();
    }

    @Override
    public void remmoveDocIds(List<Long> docIds) {
        Response response = createRequestWithCredentials(
                endpointTarget
                        .path("docsToRemove"))
                .post(Entity.entity(docIds, MediaType.APPLICATION_JSON_TYPE));
        checkResponse(response, "removed documents: " + docIds);
        response.close();
    }

    @Override
    public void addAncestorToDocs(Long ancestorId, List<Long> docIds) {
        Response response = createRequestWithCredentials(
                endpointTarget
                        .path(ancestorId.toString()).path("descendants"))
                .put(Entity.entity(docIds, MediaType.APPLICATION_JSON_TYPE));
        checkResponse(response, "add ancestor " + ancestorId + " to " + docIds);
        response.close();
    }

}

package org.janelia.messaging.broker.indexingadapter;

import org.janelia.messaging.broker.AbstractRestClient;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

class IndexingRestClient extends AbstractRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(IndexingRestClient.class);
    private static final String INDEXING_ENDPOINT_PATH = "data/searchIndex";

    IndexingRestClient(String remoteApiURL) {
        super(remoteApiURL);
    }

    void indexDocReferences(List<Reference> docReferences) {
        Response response = getIndexingEndpoint()
                .request()
                .post(Entity.entity(docReferences, MediaType.APPLICATION_JSON_TYPE));
        checkResponse(response, "index documents: " + docReferences);
        response.close();
    }

    void remmoveDocIds(List<Long> docIds) {
        Response response = getIndexingEndpoint()
                .path("docsToRemove")
                .request()
                .post(Entity.entity(docIds, MediaType.APPLICATION_JSON_TYPE));
        checkResponse(response, "removed documents: " + docIds);
        response.close();
    }

    void addAncestorToDocs(Long ancestorId, List<Long> docIds) {
        Response response = getIndexingEndpoint()
                .path(ancestorId.toString()).path("descendants")
                .request()
                .put(Entity.entity(docIds, MediaType.APPLICATION_JSON_TYPE));
        checkResponse(response, "add ancestor " + ancestorId + " to " + docIds);
        response.close();
    }

    WebTarget getIndexingEndpoint() {
        LOG.info("Endpoint target: {}", serverURL + INDEXING_ENDPOINT_PATH);
        return client.target(serverURL)
                .path(INDEXING_ENDPOINT_PATH);
    }

}
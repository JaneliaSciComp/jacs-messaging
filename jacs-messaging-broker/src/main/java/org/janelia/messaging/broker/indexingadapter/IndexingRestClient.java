package org.janelia.messaging.broker.indexingadapter;

import org.janelia.messaging.broker.AbstractRestClient;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class IndexingRestClient extends AbstractRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(IndexingRestClient.class);

    IndexingRestClient(String remoteApiURL) {
        super(remoteApiURL);
    }

    void indexDocReferences(List<Reference> docReferences) {
    }

    void remmoveDocIds(List<Long> docIds) {
    }

    void addAncestorToDocs(Long ancestorId, List<Long> docIds) {
    }
}

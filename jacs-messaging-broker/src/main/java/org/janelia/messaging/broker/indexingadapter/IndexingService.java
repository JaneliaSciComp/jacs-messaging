package org.janelia.messaging.broker.indexingadapter;

import java.util.List;

import org.janelia.model.domain.Reference;

interface IndexingService {
    void indexDocReferences(List<Reference> docReferences);

    void remmoveDocIds(List<Long> docIds);

    void addAncestorToDocs(Long ancestorId, List<Long> docIds);
}

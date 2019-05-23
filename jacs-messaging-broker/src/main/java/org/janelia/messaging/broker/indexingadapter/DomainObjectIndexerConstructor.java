package org.janelia.messaging.broker.indexingadapter;

import org.janelia.model.access.domain.search.DomainObjectIndexer;

public interface DomainObjectIndexerConstructor<T> {
    DomainObjectIndexer createDomainObjectIndexer(T helper);
}

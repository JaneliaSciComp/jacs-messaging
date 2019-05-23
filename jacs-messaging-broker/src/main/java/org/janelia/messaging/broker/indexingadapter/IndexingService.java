package org.janelia.messaging.broker.indexingadapter;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.SolrServer;
import org.janelia.model.access.domain.dao.ReferenceDomainObjectReadDao;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainObjectGetter;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class IndexingService {

    private static final Logger LOG = LoggerFactory.getLogger(IndexingService.class);

    private final DomainObjectGetter domainObjectGetter;
    private final SolrServerConstructor solrServerConstructor;
    private final DomainObjectIndexerConstructor<SolrServer> domainObjectIndexerConstructor;
    private final String solrServerBaseURL;
    private final String solrMainCore;
    private final int solrLoaderQueueSize;
    private final int solrLoaderThreadCount;

    IndexingService(DomainObjectGetter domainObjectGetter,
                    SolrServerConstructor solrServerConstructor,
                    DomainObjectIndexerConstructor<SolrServer> domainObjectIndexerConstructor,
                    String solrServerBaseURL,
                    String solrMainCore,
                    int solrLoaderQueueSize,
                    int solrLoaderThreadCount) {
        this.domainObjectGetter = domainObjectGetter;
        this.solrServerConstructor = solrServerConstructor;
        this.domainObjectIndexerConstructor = domainObjectIndexerConstructor;
        this.solrServerBaseURL = solrServerBaseURL;
        this.solrMainCore = solrMainCore;
        this.solrLoaderQueueSize = solrLoaderQueueSize;
        this.solrLoaderThreadCount = solrLoaderThreadCount;
    }

    void indexDocReferences(List<Reference> docReferences) {
        List<DomainObject> domainObjects = domainObjectGetter.getDomainObjectsByReferences(docReferences);
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        int nIndexed = domainObjectIndexer.indexDocumentStream(domainObjects.stream());
        LOG.info("Indexed {} documents out of {} requested", nIndexed, docReferences.size());
    }

    void remmoveDocIds(List<Long> docIds) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        int nRemoved = domainObjectIndexer.removeDocumentStream(docIds.stream());
        LOG.info("Removed {} documents out of {} requested", nRemoved, docIds.size());
    }

    void addAncestorToDocs(Long ancestorId, List<Long> docIds) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        domainObjectIndexer.updateDocsAncestors(ImmutableSet.copyOf(docIds), ancestorId);
    }

    private SolrServer createSolrServer(String coreName, boolean forConcurrentUpdate) {
        return solrServerConstructor.createSolrServer(
                solrServerBaseURL,
                coreName,
                forConcurrentUpdate,
                solrLoaderQueueSize,
                solrLoaderThreadCount);
    }

}

package org.janelia.messaging.broker.indexingadapter;

import org.apache.solr.client.solrj.SolrServer;

public interface SolrServerConstructor {
    SolrServer createSolrServer(String solrBaseURL, String coreName, boolean forConcurrentUpdate, int queueSize, int threadCount);
}
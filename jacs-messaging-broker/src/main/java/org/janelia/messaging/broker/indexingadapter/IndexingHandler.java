package org.janelia.messaging.broker.indexingadapter;

import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.utils.MessagingUtils;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class IndexingHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(IndexingHandler.class);

    private final IndexingRestClient indexingRestClient;
    private final int workDelay;
    private final DedupedDelayQueue<Reference> docsToIndex;
    private final DedupedDelayQueue<Long> docIdsToRemove;
    private final Map<Long, DedupedDelayQueue<Long>> docDescendantsToAdd;

    IndexingHandler(String indexingServerURL, int workDelay) {
        this.indexingRestClient = new IndexingRestClient(indexingServerURL);
        this.workDelay = workDelay;

        docsToIndex = new DedupedDelayQueue<Reference>() {
            {
                setWorkItemDelay(workDelay);
            }

            @Override
            void process(List<Reference> workItems) {
                indexingRestClient.indexDocReferences(workItems);
            }
        };

        docIdsToRemove = new DedupedDelayQueue<Long>() {
            {
                setWorkItemDelay(workDelay);
            }

            @Override
            void process(List<Long> workItems) {
                indexingRestClient.remmoveDocIds(workItems);
            }
        };

        docDescendantsToAdd = new ConcurrentSkipListMap<>();
    }

    @Override
    public void handleMessage(Map<String, Object> messageHeaders, byte[] messageBody) {
        if (messageHeaders == null) {
            return; // won't be able to figure out what to do anyway
        }
        LOG.info("Processing request {}", messageHeaders);
        IndexingMessageType action = IndexingMessageType.valueOf(MessagingUtils.getHeaderAsString(messageHeaders, IndexingMessageHeaders.TYPE));
        switch (action) {
            case ADD_ANCESTOR:
                handleAddAncestor(messageHeaders);
                break;
            case DELETE_DOC:
                handleDeleteDoc(messageHeaders);
                break;
            case UPDATE_DOC:
                handleUpdateDoc(messageHeaders);
                break;
        }
    }

    @Override
    public void cancelMessage(String routingTag) {
        LOG.error("Canceled message for {}", routingTag);
    }

    private void handleAddAncestor(Map<String, Object> messageHeaders) {
        Long objectId = MessagingUtils.getHeaderAsLong(messageHeaders, IndexingMessageHeaders.OBJECT_ID);
        Long ancestorId =  MessagingUtils.getHeaderAsLong(messageHeaders, IndexingMessageHeaders.ANCESTOR_ID);
        // map descendants to the ancestor and only add the ancestor to its descendants after the work delay
        synchronized (docDescendantsToAdd) {
            DedupedDelayQueue<Long> docDescendants;
            if (docDescendantsToAdd.get(ancestorId) == null) {
                docDescendants = new DedupedDelayQueue<Long>() {
                    {
                        setWorkItemDelay(workDelay);
                    }
                    @Override
                    void process(List<Long> workItems) {
                        indexingRestClient.addAncestorToDocs(ancestorId, workItems);
                        if (getQueueSize() == 0) {
                            // nothing left in the queue
                            docDescendantsToAdd.remove(ancestorId);
                        }
                    }
                };
            } else {
                docDescendants = docDescendantsToAdd.get(ancestorId);

            }
            docDescendants.addWorkItem(objectId);
        }
    }

    private void handleDeleteDoc(Map<String, Object> messageHeaders) {
        Long objectId = MessagingUtils.getHeaderAsLong(messageHeaders, IndexingMessageHeaders.OBJECT_ID);
        docIdsToRemove.addWorkItem(objectId);
    }

    private void handleUpdateDoc(Map<String, Object> messageHeaders) {
        Long objectId = MessagingUtils.getHeaderAsLong(messageHeaders, IndexingMessageHeaders.OBJECT_ID);
        if (objectId != null) {
            String objectClass = MessagingUtils.getHeaderAsString(messageHeaders, IndexingMessageHeaders.OBJECT_CLASS);
            docsToIndex.addWorkItem(Reference.createFor(objectClass, objectId));
        }
    }
}

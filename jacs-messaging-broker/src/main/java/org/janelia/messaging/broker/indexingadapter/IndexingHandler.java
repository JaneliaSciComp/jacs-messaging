package org.janelia.messaging.broker.indexingadapter;

import java.util.Map;
import java.util.function.Function;

import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.utils.MessagingUtils;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(IndexingHandler.class);

    private final DedupedDelayQueue<Reference> docsToIndex;
    private final DedupedDelayQueue<Long> docIdsToRemove;
    private final Map<Long, DedupedDelayQueue<Long>> docDescendantsToAdd;
    private final Function<Long, DedupedDelayQueue<Long>> docDescendantsQueueSupplier;

    IndexingHandler(DedupedDelayQueue<Reference> docsToIndex,
                    DedupedDelayQueue<Long> docIdsToRemove,
                    Map<Long, DedupedDelayQueue<Long>> docDescendantsToAdd,
                    Function<Long, DedupedDelayQueue<Long>> docDescendantsQueueSupplier) {
        this.docsToIndex = docsToIndex;
        this.docIdsToRemove = docIdsToRemove;
        this.docDescendantsToAdd = docDescendantsToAdd;
        this.docDescendantsQueueSupplier = docDescendantsQueueSupplier;
    }

    @Override
    public void handleMessage(Map<String, Object> messageHeaders, byte[] messageBody) {
        if (messageHeaders == null) {
            return; // won't be able to figure out what to do anyway
        }
        LOG.debug("Processing request {}", messageHeaders);
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
            LOG.debug("Queue add ancestor {} to {}", ancestorId, objectId);
            DedupedDelayQueue<Long> docDescendants;
            if (docDescendantsToAdd.get(ancestorId) == null) {
                docDescendants = docDescendantsQueueSupplier.apply(ancestorId);
            } else {
                docDescendants = docDescendantsToAdd.get(ancestorId);
            }
            docDescendants.addWorkItem(objectId);
        }
    }

    private void handleDeleteDoc(Map<String, Object> messageHeaders) {
        Long objectId = MessagingUtils.getHeaderAsLong(messageHeaders, IndexingMessageHeaders.OBJECT_ID);
        LOG.debug("Queue delete {}", objectId);
        docIdsToRemove.addWorkItem(objectId);
    }

    private void handleUpdateDoc(Map<String, Object> messageHeaders) {
        Long objectId = MessagingUtils.getHeaderAsLong(messageHeaders, IndexingMessageHeaders.OBJECT_ID);
        if (objectId != null) {
            String objectClass = MessagingUtils.getHeaderAsString(messageHeaders, IndexingMessageHeaders.OBJECT_CLASS);
            LOG.debug("Queue update {}:{}", objectClass, objectId);
            docsToIndex.addWorkItem(Reference.createFor(objectClass, objectId));
        }
    }
}

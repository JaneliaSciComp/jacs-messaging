package org.janelia.messaging.broker.indexingadapter;

import org.janelia.messaging.broker.BrokerAdapter;
import org.janelia.messaging.broker.BrokerAdapterArgs;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class IndexingBrokerAdapter extends BrokerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(IndexingBrokerAdapter.class);
    private static final int MAX_BATCH_SIZE = 10000;
    private static final int WORK_DELAY_MILLIS = 5000;
    private static final long INITIAL_DELAY_IN_MILLIS = 15000L;
    private static final long INDEXING_INTERVAL_IN_MILLIS = 15000L;

    private final IndexingRestClient indexingRestClient;
    private final DedupedDelayWithCallbackQueue<Reference> docsToIndex;
    private final DedupedDelayWithCallbackQueue<Long> docIdsToRemove;
    private final NavigableMap<Long, DedupedDelayQueue<Long>> docDescendantsToAdd;

    IndexingBrokerAdapter(BrokerAdapterArgs adapterArgs, String indexingServerURL) {
        super(adapterArgs);
        this.indexingRestClient = new IndexingRestClient(indexingServerURL);

        docsToIndex = new DedupedDelayWithCallbackQueue<Reference>() {
            {
                setWorkItemDelay(WORK_DELAY_MILLIS);
            }

            @Override
            void processList(List<Reference> workItems) {
                LOG.info("Index items {}", workItems);
                indexingRestClient.indexDocReferences(workItems);
            }
        };

        docIdsToRemove = new DedupedDelayWithCallbackQueue<Long>() {
            {
                setWorkItemDelay(WORK_DELAY_MILLIS);
            }

            @Override
            void processList(List<Long> workItems) {
                LOG.info("Remove items {}", workItems);
                indexingRestClient.remmoveDocIds(workItems);
            }
        };

        docDescendantsToAdd = new ConcurrentSkipListMap<>();
    }

    @Override
    public MessageHandler getMessageHandler(MessageHandler.HandlerCallback successCallback, MessageHandler.HandlerCallback errorCallback) {
        docsToIndex.setProcessingCompleteCallback(workItems -> {
            Map<String, Object> messageHeaders = new LinkedHashMap<>();
            messageHeaders.put(IndexingMessageHeaders.TYPE, "INDEX_DOCS");
            messageHeaders.put(IndexingMessageHeaders.OBJECT_REFS,
                    workItems.stream().map(Reference::toString).reduce((o1, o2) -> o1 + "," + o2).orElse("")
            );
            successCallback.callback(messageHeaders, null);
        });
        docIdsToRemove.setProcessingCompleteCallback(workItems -> {
            Map<String, Object> messageHeaders = new LinkedHashMap<>();
            messageHeaders.put(IndexingMessageHeaders.TYPE, "DELETE_DOCS");
            messageHeaders.put(IndexingMessageHeaders.OBJECT_IDS,
                    workItems.stream().map(Object::toString).reduce((o1, o2) -> o1 + "," + o2).orElse("")
            );
            successCallback.callback(messageHeaders, null);
        });
        return new IndexingHandler(
                docsToIndex,
                docIdsToRemove,
                docDescendantsToAdd,
                (ancestorId) -> new DedupedDelayQueue<Long>() {
                    {
                        setWorkItemDelay(WORK_DELAY_MILLIS);
                    }

                    @Override
                    void process(List<Long> workItems) {
                        super.process(workItems);
                        if (getQueueSize() == 0) {
                            // nothing left in the queue
                            docDescendantsToAdd.remove(ancestorId);
                        }
                    }

                    @Override
                    void processList(List<Long> workItems) {
                        LOG.info("Add ancestor {} to {}", ancestorId, workItems);
                        indexingRestClient.addAncestorToDocs(ancestorId, workItems);
                        Map<String, Object> messageHeaders = new LinkedHashMap<>();
                        messageHeaders.put(IndexingMessageHeaders.TYPE, "ADD_ANCESTOR");
                        messageHeaders.put(IndexingMessageHeaders.OBJECT_IDS,
                                workItems.stream().map(Object::toString).reduce((o1, o2) -> o1 + "," + o2).orElse("")
                        );
                        messageHeaders.put(IndexingMessageHeaders.ANCESTOR_ID, ancestorId);
                        successCallback.callback(messageHeaders, null);
                    }
                });
    }

    @Override
    public List<ScheduledTask> getScheduledTasks(MessageConnection messageConnection) {
        return Arrays.asList(
                getBackupQueueTask(messageConnection),
                getIncrementalIndexingTask()
        );
    }

    private ScheduledTask getIncrementalIndexingTask() {
        LOG.info("{} - configure incremental indexing task", adapterArgs.getAdapterName());

        Runnable command = () -> {
            docsToIndex.process(MAX_BATCH_SIZE);
            docIdsToRemove.process(MAX_BATCH_SIZE);
            synchronized (docDescendantsToAdd) {
                if (!docDescendantsToAdd.isEmpty()) {
                    Long ancestorId = docDescendantsToAdd.firstKey();
                    DedupedDelayQueue<Long> descendantsQueue = docDescendantsToAdd.get(ancestorId);
                    descendantsQueue.process(MAX_BATCH_SIZE);
                }
            }
        };
        ScheduledTask scheduledTask = new ScheduledTask();
        scheduledTask.command = command;
        scheduledTask.initialDelayInMillis = INITIAL_DELAY_IN_MILLIS;
        scheduledTask.intervalInMillis = INDEXING_INTERVAL_IN_MILLIS;
        return scheduledTask;
    }

}

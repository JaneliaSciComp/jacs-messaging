package org.janelia.messaging.broker.indexingadapter;

import org.janelia.messaging.broker.BrokerAdapter;
import org.janelia.messaging.broker.BrokerAdapterArgs;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class IndexingBrokerAdapter extends BrokerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(IndexingBrokerAdapter.class);
    private static final int MAX_BATCH_SIZE = 10000;
    private static final int WORK_DELAY_MILLIS = 5000;
    private static final int INITIAL_DELAY_IN_SECONDS = 60;
    private static final int INDEXING_INTERVAL_IN_SECONDS = 60;

    private final IndexingRestClient indexingRestClient;
    private final DedupedDelayQueue<Reference> docsToIndex;
    private final DedupedDelayQueue<Long> docIdsToRemove;
    private final NavigableMap<Long, DedupedDelayQueue<Long>> docDescendantsToAdd;

    public IndexingBrokerAdapter(BrokerAdapterArgs adapterArgs, String indexingServerURL) {
        super(adapterArgs);
        this.indexingRestClient = new IndexingRestClient(indexingServerURL);

        docsToIndex = new DedupedDelayQueue<Reference>() {
            {
                setWorkItemDelay(WORK_DELAY_MILLIS);
            }

            @Override
            void processList(List<Reference> workItems) {
                indexingRestClient.indexDocReferences(workItems);
            }
        };

        docIdsToRemove = new DedupedDelayQueue<Long>() {
            {
                setWorkItemDelay(WORK_DELAY_MILLIS);
            }

            @Override
            void processList(List<Long> workItems) {
                indexingRestClient.remmoveDocIds(workItems);
            }
        };

        docDescendantsToAdd = new ConcurrentSkipListMap<>();
    }

    @Override
    public MessageHandler getMessageHandler(MessageHandler.HandlerCallback successCallback, MessageHandler.HandlerCallback errorCallback) {
        return new IndexingHandler(
                new DedupedDelayQueueWrapper<Reference>(docsToIndex) {
                    @Override
                    void processList(List<Reference> workItems) {
                        LOG.info("Index items {}", workItems);
                        super.processList(workItems);
                        Map<String, Object> messageHeaders = new LinkedHashMap<>();
                        messageHeaders.put(IndexingMessageHeaders.TYPE, "INDEX_DOCS");
                        messageHeaders.put(IndexingMessageHeaders.OBJECT_REFS, workItems);
                        successCallback.callback(messageHeaders, null);
                    }
                },
                new DedupedDelayQueueWrapper<Long>(docIdsToRemove) {
                    @Override
                    void processList(List<Long> workItems) {
                        LOG.info("Remove items {}", workItems);
                        super.processList(workItems);
                        Map<String, Object> messageHeaders = new LinkedHashMap<>();
                        messageHeaders.put(IndexingMessageHeaders.TYPE, "DELETE_DOCS");
                        messageHeaders.put(IndexingMessageHeaders.OBJECT_IDS, workItems);
                        successCallback.callback(messageHeaders, null);
                    }
                }
                ,
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
                        messageHeaders.put(IndexingMessageHeaders.OBJECT_IDS, workItems);
                        messageHeaders.put(IndexingMessageHeaders.ANCESTOR_ID, ancestorId);
                        successCallback.callback(messageHeaders, null);

                    }
                });
    }

    @Override
    public void schedulePeriodicTasks(MessageConnection messageConnection, ScheduledExecutorService scheduledExecutorService) {
        super.schedulePeriodicTasks(messageConnection, scheduledExecutorService);
        ScheduledTask incrementalIndexingTask = getIncrementalIndexingTask();
        scheduledExecutorService.scheduleAtFixedRate(
                incrementalIndexingTask.command,
                incrementalIndexingTask.initialDelay,
                incrementalIndexingTask.interval,
                incrementalIndexingTask.timeUnit);
    }

    private ScheduledTask getIncrementalIndexingTask() {
        LOG.info("Configure incremental indexing task");

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
        scheduledTask.initialDelay = INITIAL_DELAY_IN_SECONDS;
        scheduledTask.interval = INDEXING_INTERVAL_IN_SECONDS;
        scheduledTask.timeUnit = TimeUnit.SECONDS;
        return scheduledTask;
    }

}

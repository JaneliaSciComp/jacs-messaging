package org.janelia.messaging.broker.indexingadapter;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.broker.BrokerAdapter;
import org.janelia.messaging.broker.BrokerAdapterArgs;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.core.MessageSender;
import org.janelia.messaging.core.impl.MessageSenderImpl;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingBrokerAdapter extends BrokerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(IndexingBrokerAdapter.class);
    private static final int MAX_BATCH_SIZE = 10000;
    private static final int WORK_DELAY_MILLIS = 5000;
    private static final long INITIAL_DELAY_IN_MILLIS = 15000L;
    private static final long INDEXING_INTERVAL_IN_MILLIS = 15000L;

    private final IndexingService indexingService;
    private final DedupedDelayWithCallbackQueue<Reference> docsToIndex;
    private final DedupedDelayWithCallbackQueue<Long> docIdsToRemove;
    private final NavigableMap<Long, DedupedDelayQueue<Long>> docDescendantsToAdd;

    IndexingBrokerAdapter(BrokerAdapterArgs adapterArgs) {
        super(adapterArgs);
        this.indexingService = createIndexingService(adapterArgs);

        docsToIndex = new DedupedDelayWithCallbackQueue<Reference>() {
            {
                setWorkItemDelay(WORK_DELAY_MILLIS);
            }

            @Override
            void processList(List<Reference> workItems) {
                LOG.info("Index items {}", workItems);
                indexingService.indexDocReferences(workItems);
            }
        };

        docIdsToRemove = new DedupedDelayWithCallbackQueue<Long>() {
            {
                setWorkItemDelay(WORK_DELAY_MILLIS);
            }

            @Override
            void processList(List<Long> workItems) {
                LOG.info("Remove items {}", workItems);
                indexingService.remmoveDocIds(workItems);
            }
        };

        docDescendantsToAdd = new ConcurrentSkipListMap<>();
    }

    private IndexingService createIndexingService(BrokerAdapterArgs adapterArgs) {
        return new RestIndexingService(
                adapterArgs.getAdapterConfig("indexingServer"),
                adapterArgs.getAdapterConfig("indexingApiKey"));
    }

    @Override
    public MessageHandler getMessageHandler(MessageConnection messageConnection) {
        MessageSender replySuccessSender;
        if (StringUtils.isNotBlank(adapterArgs.getSuccessResponseExchange())) {
            LOG.info("Forward indexing requests to '{}' using routing key '{}'",
                    adapterArgs.getSuccessResponseExchange(), adapterArgs.getSuccessResponseRouting());
            replySuccessSender = new MessageSenderImpl(messageConnection);
            replySuccessSender.connectTo(
                    adapterArgs.getSuccessResponseExchange(),
                    adapterArgs.getSuccessResponseRouting());
            docsToIndex.setProcessingCompleteCallback(workItems -> {
                Map<String, Object> messageHeaders = new LinkedHashMap<>();
                messageHeaders.put(IndexingMessageHeaders.TYPE, "INDEX_DOCS");
                replySuccessSender.sendMessage(
                        messageHeaders,
                        workItems.stream().map(Reference::toString).reduce((o1, o2) -> o1 + "," + o2).orElse("").getBytes()
                );
            });
            docIdsToRemove.setProcessingCompleteCallback(workItems -> {
                Map<String, Object> messageHeaders = new LinkedHashMap<>();
                messageHeaders.put(IndexingMessageHeaders.TYPE, "DELETE_DOCS");
                replySuccessSender.sendMessage(
                        messageHeaders,
                        workItems.stream().map(Object::toString).reduce((o1, o2) -> o1 + "," + o2).orElse("").getBytes()
                );
            });
        } else {
            LOG.info("Forwarding of the indexing requests has not been configured - success response exchange is empty");
            replySuccessSender = null;
        }

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
                        indexingService.addAncestorToDocs(ancestorId, workItems);
                        if (replySuccessSender != null) {
                            Map<String, Object> messageHeaders = new LinkedHashMap<>();
                            messageHeaders.put(IndexingMessageHeaders.TYPE, "ADD_ANCESTOR");
                            messageHeaders.put(IndexingMessageHeaders.ANCESTOR_ID, ancestorId);
                            replySuccessSender.sendMessage(
                                    messageHeaders,
                                    workItems.stream().map(Object::toString).reduce((o1, o2) -> o1 + "," + o2).orElse("").getBytes()
                            );
                        }
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

package org.janelia.messaging.broker.indexingadapter;

import java.util.List;

/**
 * A delayed processing queue which does not accept duplicates, thereby eliminating any duplicates that occur within
 * a certain predefined delay time.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class DedupedDelayQueueWrapper<T> extends DedupedDelayQueue<T> {
    private final DedupedDelayQueue<T> wrappedDedupedQueue;

    DedupedDelayQueueWrapper(DedupedDelayQueue<T> wrappedDedupedQueue) {
        this.wrappedDedupedQueue = wrappedDedupedQueue;
    }

    @Override
    public long getWorkItemDelay() {
        return wrappedDedupedQueue.getWorkItemDelay();
    }

    @Override
    public void setWorkItemDelay(long workItemDelay) {
        wrappedDedupedQueue.setWorkItemDelay(workItemDelay);
    }

    @Override
    void addWorkItem(final T workItem) {
        wrappedDedupedQueue.addWorkItem(workItem);
    }

    @Override
    int getQueueSize() {
        return wrappedDedupedQueue.getQueueSize();
    }

    @Override
    synchronized int process(int maxBatchSize) {
        return wrappedDedupedQueue.process(maxBatchSize);
    }

    @Override
    void process(List<T> workItems) {
        wrappedDedupedQueue.process(workItems);
    }

    @Override
    void processList(List<T> workItems) {
        wrappedDedupedQueue.processList(workItems);
    }
}

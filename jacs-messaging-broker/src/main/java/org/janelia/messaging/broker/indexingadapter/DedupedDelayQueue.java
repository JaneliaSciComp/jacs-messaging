package org.janelia.messaging.broker.indexingadapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.stream.Collectors;

/**
 * A delayed processing queue which does not accept duplicates, thereby eliminating any duplicates that occur within
 * a certain predefined delay time.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
abstract class DedupedDelayQueue<T> {
    private long workItemDelay = 5000; // Wait 5 seconds for each item by default

    private final BlockingQueue<DelayedItem<T>> delayed = new DelayQueue<DelayedItem<T>>();

    /**
     * Get the delay that is applied to each work item before it can be processed.
     * @return delay in milliseconds
     */
    public long getWorkItemDelay() {
        return workItemDelay;
    }

    /**
     * Set the delay that is applied to each work item before it can be processed.
     * @param workItemDelay delay in milliseconds
     */
    public void setWorkItemDelay(long workItemDelay) {
        this.workItemDelay = workItemDelay;
    }

    /**
     * Add a work item to the queue for processing at some later time.
     * @param workItem
     */
    void addWorkItem(final T workItem) {
        DelayedItem<T> postponed = new DelayedItem<T>(workItem, workItemDelay);
        synchronized (this) {
            if (!delayed.contains(postponed)) {
                delayed.offer(postponed);
            }
        }
    }

    /**
     * Returns the current size of the queue.
     * @return
     */
    int getQueueSize() {
        return delayed.size();
    }

    /**
     * Process as many expired work items in the queue as possible, limited only by the given batch size.
     * @param maxBatchSize max number of items to processList
     * @return the number of items that were processed
     */
    synchronized int process(int maxBatchSize) {
        // collect items that waited long enough
        Collection<DelayedItem<T>> expired = new ArrayList<>();
        synchronized (this) {
            if (delayed.isEmpty()) return 0;
            delayed.drainTo(expired, maxBatchSize);
        }
        List<T> workItems = expired.stream().map(delayedItem -> delayedItem.getItem()).collect(Collectors.toList());
        process(workItems);
        return expired.size();
    }

    void process(List<T> workItems) {
        if (!workItems.isEmpty()) {
            processList(workItems);
        }
    }

    /**
     * Override this method to provide logic for processing a batch of work items. These items are about to be
     * removed from the queue.
     * @param workItems list of work items to processList
     */
    abstract void processList(List<T> workItems);
}

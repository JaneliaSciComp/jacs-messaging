package org.janelia.messaging.broker.indexingadapter;

import java.util.List;
import java.util.function.Consumer;

/**
 * A delayed processing queue which does not accept duplicates, thereby eliminating any duplicates that occur within
 * a certain predefined delay time.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
abstract class DedupedDelayWithCallbackQueue<T> extends DedupedDelayQueue<T> {
    Consumer<List<T>> processingCompleteCallback = null;

    void setProcessingCompleteCallback(Consumer<List<T>> processingCompleteCallback) {
        this.processingCompleteCallback = processingCompleteCallback;
    }

    void process(List<T> workItems) {
        if (!workItems.isEmpty()) {
            processList(workItems);
            if (processingCompleteCallback != null) {
                processingCompleteCallback.accept(workItems);
            }
        }
    }

    /**
     * Override this method to provide logic for processing a batch of work items. These items are about to be
     * removed from the queue.
     * @param workItems list of work items to processList
     */
    abstract void processList(List<T> workItems);
}

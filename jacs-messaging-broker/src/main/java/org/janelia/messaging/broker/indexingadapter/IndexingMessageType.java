package org.janelia.messaging.broker.indexingadapter;

/**
 * Created by schauderd on 11/13/17.
 *
 * Note:
 * Any changes in this class must propagate to the applications
 * that consume or produce indexing events.
 */
public enum IndexingMessageType {
    UPDATE_DOC,
    ADD_ANCESTOR,
    DELETE_DOC
}

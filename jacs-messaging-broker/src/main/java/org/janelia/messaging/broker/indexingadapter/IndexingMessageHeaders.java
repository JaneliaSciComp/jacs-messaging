package org.janelia.messaging.broker.indexingadapter;

/**
 * Created by schauderd on 11/14/17.
 *
 * Note:
 * Any changes in this class must propagate to the applications
 * that consume or produce indexing events.
 */
public class IndexingMessageHeaders {
    public static final String OBJECT_CLASS = "objectClass";
    public static final String OBJECT_ID = "objectId";
    public static final String OBJECT_IDS = "objectIds";
    public static final String OBJECT_REFS = "objectRefs";
    public static final String ANCESTOR_ID = "ancestorId";
    public static final String TYPE = "msgType";
}

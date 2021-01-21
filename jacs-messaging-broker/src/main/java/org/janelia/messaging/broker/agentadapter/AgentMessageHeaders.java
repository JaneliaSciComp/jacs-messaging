package org.janelia.messaging.broker.agentadapter;

/**
 * Created by schauderd on 11/14/17.
 *
 * Note:
 * Any changes in this class must propagate to the applications
 * that consume or produce neuron handling events.
 */
public class AgentMessageHeaders {
    public static final String TYPE = "message_type";
    public static final String MESSAGE_ID = "message_id";
}

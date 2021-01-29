package org.janelia.messaging.broker.agentadapter;

/**
 * Created by schauderd on 1/18/2021.
 */
public enum AgentMessageType {
    CREATE_WORKSPACE,
    WORKSPACE,
    INIT,
    FIND_ROOT,
    PROCESS_DIFF,
    SUCCESS,
    ERROR
}

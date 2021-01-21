package org.janelia.messaging.broker.agentadapter;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.utils.MessagingUtils;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AgentHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AgentHandler.class);

    private final HandlerCallback successCallback;
    private final HandlerCallback errorCallback;
    private final ObjectMapper objectMapper;

    AgentHandler(HandlerCallback successCallback,
                 HandlerCallback errorCallback) {
        this.successCallback = successCallback;
        this.errorCallback = errorCallback;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handleMessage(Map<String, Object> messageHeaders, byte[] messageBody) {
        if (messageHeaders == null) {
            return; // won't be able to figure out what to do anyway
        }
        LOG.info("AGENT REQUEST {}", MessagingUtils.getHeaderAsString(messageHeaders, AgentMessageHeaders.TYPE));
        AgentMessageType action = AgentMessageType.valueOf(MessagingUtils.getHeaderAsString(messageHeaders, AgentMessageHeaders.TYPE));

        switch (action) {
            case WORKSTATION_NEURON_CHANGE:
                break;
        }

    }

    @Override
    public void cancelMessage(String routingTag) {

    }

    private void handleDiff () {

    }

    private void handleWorkspaceCreate () {

    }

    private void handleQueueAdd() {

    }

    private void handleQueueRemove() {

    }

    private void fireSuccessMessage(String message_id) {
        Map<String, Object> msgHeaders = new HashMap<>();
        msgHeaders.put(AgentMessageHeaders.TYPE, AgentMessageType.SUCCESS.toString());
        msgHeaders.put(AgentMessageHeaders.MESSAGE_ID, message_id);
        successCallback.callback(msgHeaders, null);
    }

    private void fireErrorMessage(String message_id) {
        Map<String, Object> msgHeaders = new HashMap<>();
        msgHeaders.put(AgentMessageHeaders.TYPE, AgentMessageType.FAILURE.toString());
        msgHeaders.put(AgentMessageHeaders.MESSAGE_ID, message_id);
        errorCallback.callback(msgHeaders, null);
    }

}

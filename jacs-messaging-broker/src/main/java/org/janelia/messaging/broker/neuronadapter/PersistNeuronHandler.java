package org.janelia.messaging.broker.neuronadapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.LongString;
import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.MessageSender;
import org.janelia.messaging.utils.MessagingUtils;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class PersistNeuronHandler implements DeliverCallback {
    private static final Logger LOG = LoggerFactory.getLogger(MessageDeliveryErrorHandler.class);

    private final TiledMicroscopeDomainMgr domainMgr;
    private final String sharedWorkspaceSystemOwner;
    private final MessageSender replySuccessSender;
    private final MessageSender replyErrorSender;
    private final ObjectMapper objectMapper;

    PersistNeuronHandler(TiledMicroscopeDomainMgr domainMgr,
                         @Nonnull String sharedWorkspaceSystemOwner,
                         MessageSender replySuccessSender,
                         MessageSender replyErrorSender) {
        this.domainMgr = domainMgr;
        this.sharedWorkspaceSystemOwner = sharedWorkspaceSystemOwner;
        this.replySuccessSender = replySuccessSender;
        this.replyErrorSender = replyErrorSender;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handle(String consumerTag, Delivery message) throws IOException {
        // grab neurons and double-check owner
        Map<String, Object> msgHeaders = message.getProperties().getHeaders();
        if (msgHeaders != null) {
            String user = MessagingUtils.convertLongString((LongString) msgHeaders.get(HeaderConstants.USER));
            LOG.info("Processing request from user {}", user);
            Long workspace = Long.parseLong(MessagingUtils.convertLongString((LongString) msgHeaders.get(HeaderConstants.WORKSPACE)));
            MessageType action = MessageType.valueOf(MessagingUtils.convertLongString((LongString) msgHeaders.get(HeaderConstants.TYPE)));
            TmNeuronMetadata neuronMetadata = extractNeuron(msgHeaders);
            if (neuronMetadata != null) {
                switch (action) {
                    case NEURON_DELETE:
                        handleDeleteNeuron(msgHeaders, neuronMetadata, user, neuron -> replySuccessSender.sendMessage(msgHeaders, message.getBody()));
                        break;
                    case NEURON_CREATE:
                    case NEURON_SAVE_NEURONDATA:
                        if (!user.equals(neuronMetadata.getOwnerKey())) {
                            // probably should fire off rejection message but for now just log the message
                            LOG.warn("User {} attempt to save neuron {} owned by {}", user, neuronMetadata, neuronMetadata.getOwnerKey());
                            return;
                        }
                        handleSaveNeuronWithBody(msgHeaders, neuronMetadata, message.getBody(), user, neuron -> {
                            try {
                                Map<String, Object> notifHeaders = newMessageHeaders(msgHeaders,
                                        ImmutableMap.of(HeaderConstants.METADATA, objectMapper.writeValueAsString(neuron)));
                                LOG.info("Sending out broadcast refresh for persisted neuron with body {}", neuron);
                                replySuccessSender.sendMessage(notifHeaders, message.getBody());
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        });
                        break;
                    case NEURON_SAVE_METADATA:
                        if (!user.equals(neuronMetadata.getOwnerKey())) {
                            // probably should fire off rejection message but for now just log the message
                            LOG.warn("User {} attempt to save neuron metadata {} owned by {}", user, neuronMetadata, neuronMetadata.getOwnerKey());
                            return;
                        }
                        handleSaveNeuronMetadata(msgHeaders, neuronMetadata, user, neuron -> {
                            try {
                                Map<String, Object> notifHeaders = newMessageHeaders(msgHeaders,
                                        ImmutableMap.of(HeaderConstants.METADATA, objectMapper.writeValueAsString(neuron)));
                                LOG.info("Sending out broadcast refresh for persisted neuron metadata {}", neuron);
                                replySuccessSender.sendMessage(notifHeaders, message.getBody());
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        });
                        break;
                    case REQUEST_NEURON_ASSIGNMENT:
                        handleReassignNeuron(msgHeaders, neuronMetadata, user, (neuron) -> {
                            try {
                                fireApprovalMessage(neuron, user, true, objectMapper.writeValueAsString(neuron));
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        });
                        break;
                    case NEURON_OWNERSHIP_DECISION:
                        // process response from neuron owner
                        handleOwnershipDecision(msgHeaders, user, (neuron, decision) -> {
                            try {
                                fireApprovalMessage(neuron, user, decision, objectMapper.writeValueAsString(neuron));
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        });
                        break;
                    case REQUEST_NEURON_OWNERSHIP:
                        handleOwnershipRequest(msgHeaders, user, (neuron, decision) -> {
                            try {
                                fireApprovalMessage(neuron, user, decision, objectMapper.writeValueAsString(neuron));
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        });
                        break;
                }
            }
        }
    }

    private TmNeuronMetadata extractNeuron(Map<String, Object> msgHeaders) {
        try {
            String metadata = MessagingUtils.convertLongString((LongString) msgHeaders.get(HeaderConstants.METADATA));
            if (StringUtils.isNotBlank(metadata)) {
                return objectMapper.readValue(metadata, TmNeuronMetadata.class);
            } else {
                LOG.info("No neuron metadata present in the message headers: {}", msgHeaders);
                return null;
            }
        } catch (Exception e) {
            LOG.error("Error unmarshalling neuron metadata from {}", msgHeaders, e);
            fireErrorMessage(msgHeaders, "Problems unmarshalling neuron data from " + msgHeaders.get(HeaderConstants.METADATA) + " -> " + e.getMessage());
        }
        return null;
    }

    private void fireErrorMessage(Map<String, Object> msgHeaders, String errorMessage) {
        Map<String, Object> errorNotifHeaders = newMessageHeaders(msgHeaders, ImmutableMap.of(HeaderConstants.TYPE, MessageType.ERROR_PROCESSING.name()));
        replySuccessSender.sendMessage(errorNotifHeaders, errorMessage.getBytes());
        replyErrorSender.sendMessage(errorNotifHeaders, errorMessage.getBytes());
    }

    private Map<String, Object> newMessageHeaders(Map<String, Object> msgHeaders, Map<String, Object> overridenHeaders) {
        Map<String, Object> newHeaders = msgHeaders.entrySet().stream()
                .filter(e -> !overridenHeaders.containsKey(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        overridenHeaders.forEach((k, v) -> newHeaders.put(k, v));
        return newHeaders;
    }

    private void handleDeleteNeuron(Map<String, Object> msgHeaders, TmNeuronMetadata neuronMetadata, String user,
                                    Consumer<TmNeuronMetadata> onSuccess) {
        try {
            domainMgr.remove(neuronMetadata, user);
            onSuccess.accept(neuronMetadata);
        } catch (Exception e) {
            LOG.error("Error removing neuron {} by {}", neuronMetadata, user, e);
            fireErrorMessage(msgHeaders, "Problems deleting neuron data: " + e.getMessage());
        }
    }

    private void handleSaveNeuronWithBody(Map<String, Object> msgHeaders, TmNeuronMetadata neuronMetadata, byte[] neuronBody, String user,
                                          Consumer<TmNeuronMetadata> onSuccess) {
        try {
            TmNeuronMetadata persistedNeuron = domainMgr.save(neuronMetadata, neuronBody, user);
            LOG.info("Persisted neuron {}", persistedNeuron);
            onSuccess.accept(persistedNeuron);
        } catch (Exception e) {
            LOG.error("Error persisting neuron {} by {}", neuronMetadata, user, e);
            fireErrorMessage(msgHeaders, "Problems persisting neuron data: " + e.getMessage());
        }
    }

    private void handleSaveNeuronMetadata(Map<String, Object> msgHeaders, TmNeuronMetadata neuronMetadata, String user,
                                          Consumer<TmNeuronMetadata> onSuccess) {
        try {
            TmNeuronMetadata persistedNeuron = domainMgr.saveMetadata(neuronMetadata, user);
            LOG.info("Persisted neuron {}", persistedNeuron);
            onSuccess.accept(persistedNeuron);
        } catch (Exception e) {
            LOG.error("Error persisting neuron {} by {}", neuronMetadata, user, e);
            fireErrorMessage(msgHeaders, "Problems persisting neuron data: " + e.getMessage());
        }
    }

    private void handleReassignNeuron(Map<String, Object> msgHeaders, TmNeuronMetadata neuronMetadata, String user,
                                      Consumer<TmNeuronMetadata> onSuccess) {
        try {
            String targetUser = MessagingUtils.convertLongString((LongString) msgHeaders.get(HeaderConstants.TARGET_USER));
            neuronMetadata.setOwnerKey(targetUser);
            neuronMetadata.getReaders().add(targetUser);
            neuronMetadata.getWriters().add(targetUser);
            TmNeuronMetadata persistedNeuron = domainMgr.saveMetadata(neuronMetadata, user);
            domainMgr.setPermissions(user, persistedNeuron, targetUser);
            onSuccess.accept(persistedNeuron);
        } catch (Exception e) {
            LOG.error("Error assigning new owner {} to neuron {} by {}", msgHeaders.get(HeaderConstants.TARGET_USER), neuronMetadata, user, e);
            fireErrorMessage(msgHeaders, "Problems assigning new owner to neuron: " + e.getMessage());
        }
    }

    private void handleOwnershipDecision(Map<String, Object> msgHeaders, String user, BiConsumer<TmNeuronMetadata, Boolean> onSuccess) {
        try {
            List<String> neuronIdList;
            String neuronIds = MessagingUtils.convertLongString((LongString) msgHeaders.get(HeaderConstants.NEURONIDS));
            if (StringUtils.isNotBlank(neuronIds)) {
                 neuronIdList = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(neuronIds);
            } else {
                neuronIdList = Collections.emptyList();
            }
            if (!neuronIdList.isEmpty()) {
                boolean decision = Boolean.parseBoolean(
                        MessagingUtils.convertLongString((LongString) msgHeaders.get(HeaderConstants.DECISION)));
                domainMgr.retrieve(neuronIdList, user)
                        .forEach(neuron -> {
                    if (decision) {
                        onSuccess.accept(updateOwnership(neuron, user), true);
                    } else {
                        onSuccess.accept(neuron, false);
                    }
                });
            }
        } catch (Exception e) {
            LOG.error("Error processing ownership decision {}", msgHeaders, e);
            fireErrorMessage(msgHeaders, "Problems processing ownership decision: " + e.getMessage());
        }
    }

    private TmNeuronMetadata updateOwnership(TmNeuronMetadata neuron, String user) {
        neuron.setOwnerKey(user);
        try {
            return domainMgr.saveMetadata(neuron, user);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void handleOwnershipRequest(Map<String, Object> msgHeaders, String user, BiConsumer<TmNeuronMetadata, Boolean> onSuccess) {
        try {
            List<String> neuronIdList;
            String neuronIds = MessagingUtils.convertLongString((LongString) msgHeaders.get(HeaderConstants.NEURONIDS));
            if (StringUtils.isNotBlank(neuronIds)) {
                neuronIdList = Splitter.on(',').trimResults()
                        .omitEmptyStrings()
                        .splitToList(neuronIds.replaceAll("\\[", "").replaceAll("]", ""));
            } else {
                neuronIdList = Collections.emptyList();
            }
            if (!neuronIdList.isEmpty()) {
                domainMgr.retrieve(neuronIdList, user)
                        .forEach(neuron -> {
                            if (neuron.getOwnerKey() != null) {
                                if (neuron.getOwnerKey().equals(sharedWorkspaceSystemOwner)) {
                                    onSuccess.accept(updateOwnership(neuron, user), true); // this neuron is owned by the system user so update the owner
                                } else if (neuron.getOwnerKey().equals(user)) {
                                    onSuccess.accept(neuron, true); // this neuron is already owned by this user
                                } else {
                                    onSuccess.accept(neuron, false); // the broker cannot make a decision
                                }
                            } else {
                                onSuccess.accept(neuron, false); // the broker cannot make a decision
                            }
                        });
            }

        } catch (Exception e) {
            LOG.error("Error processing ownership request {}", msgHeaders, e);
            fireErrorMessage(msgHeaders, "Problems processing ownership request: " + e.getMessage());
        }

    }

    private void fireApprovalMessage(TmNeuronMetadata neuron, String user, boolean approval, String metadata) {
        Map<String, Object> msgHeaders = new HashMap<>();
        msgHeaders.put(HeaderConstants.TYPE, MessageType.NEURON_OWNERSHIP_DECISION.toString());
        msgHeaders.put(HeaderConstants.NEURONIDS, ImmutableList.of(neuron.getId().toString()));
        msgHeaders.put(HeaderConstants.USER, user);
        msgHeaders.put(HeaderConstants.WORKSPACE, neuron.getWorkspaceId().toString());
        msgHeaders.put(HeaderConstants.DECISION, String.valueOf(approval));
        msgHeaders.put(HeaderConstants.DESCRIPTION, "Ownership approved by Neuron Owner");
        msgHeaders.put(HeaderConstants.METADATA, metadata);

        LOG.info("Sending out neuron ownership message for neuron ID: {} with {}", neuron.getId(), msgHeaders);
        replySuccessSender.sendMessage(msgHeaders, " ".getBytes());
    }

}

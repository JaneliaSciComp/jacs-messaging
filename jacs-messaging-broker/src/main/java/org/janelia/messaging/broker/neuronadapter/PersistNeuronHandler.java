package org.janelia.messaging.broker.neuronadapter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.utils.MessagingUtils;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PersistNeuronHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PersistNeuronHandler.class);

    private final TiledMicroscopeDomainMgr domainMgr;
    private final String sharedWorkspaceSystemOwner;
    private final HandlerCallback successCallback;
    private final HandlerCallback errorCallback;
    private final ObjectMapper objectMapper;

    PersistNeuronHandler(TiledMicroscopeDomainMgr domainMgr,
                         @Nonnull String sharedWorkspaceSystemOwner,
                         HandlerCallback successCallback,
                         HandlerCallback errorCallback) {
        this.domainMgr = domainMgr;
        this.sharedWorkspaceSystemOwner = sharedWorkspaceSystemOwner;
        this.successCallback = successCallback;
        this.errorCallback = errorCallback;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handleMessage(Map<String, Object> messageHeaders, byte[] messageBody) {
        if (messageHeaders == null) {
            return; // won't be able to figure out what to do anyway
        }
        LOG.debug("Processing request {}", messageHeaders);
        String user = MessagingUtils.getHeaderAsString(messageHeaders, NeuronMessageHeaders.USER);
        NeuronMessageType action = NeuronMessageType.valueOf(MessagingUtils.getHeaderAsString(messageHeaders, NeuronMessageHeaders.TYPE));
        TmNeuronMetadata neuronMetadata = extractNeuron(messageHeaders, messageBody);
        if (neuronMetadata != null) {
            switch (action) {
                case NEURON_DELETE:
                    handleDeleteNeuron(messageHeaders, neuronMetadata, user, neuron -> successCallback.callback(messageHeaders, messageBody));
                    break;
                case NEURON_CREATE:
                case NEURON_SAVE_NEURONDATA:
                    if (!user.equals(neuronMetadata.getOwnerKey())) {
                        // probably should fire off rejection message but for now just log the message
                        LOG.warn("User {} attempt to save neuron {} owned by {}", user, neuronMetadata, neuronMetadata.getOwnerKey());
                        return;
                    }
                    handleSaveNeuron(messageHeaders, neuronMetadata, user, neuron -> {
                        try {
                            LOG.info("Sending out broadcast refresh for persisted neuron with body {}", neuron);
                            successCallback.callback(messageHeaders, objectMapper.writeValueAsBytes(neuron));
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    });
                    break;
                case REQUEST_NEURON_ASSIGNMENT:
                    handleReassignNeuron(messageHeaders, neuronMetadata, user, (neuron) -> {
                        try {
                            fireApprovalMessage(neuron, user, true, objectMapper.writeValueAsBytes(neuron));
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    });
                    break;
                case NEURON_OWNERSHIP_DECISION:
                    // process response from neuron owner
                    handleOwnershipDecision(messageHeaders, user, (neuron, decision) -> {
                        try {
                            fireApprovalMessage(neuron, user, decision, objectMapper.writeValueAsBytes(neuron));
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    });
                    break;
                case REQUEST_NEURON_OWNERSHIP:
                    handleOwnershipRequest(messageHeaders, user, (neuron, decision) -> {
                        try {
                            fireApprovalMessage(neuron, user, decision, objectMapper.writeValueAsBytes(neuron));
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    });
                    break;
            }
        }
    }

    @Override
    public void cancelMessage(String routingTag) {
        LOG.error("Canceled message for {}", routingTag);
    }

    private TmNeuronMetadata extractNeuron(Map<String, Object> msgHeaders, byte[] messageBody) {
        String neuronIds = MessagingUtils.getHeaderAsString(msgHeaders, NeuronMessageHeaders.NEURONIDS);
        try {
            if (messageBody.length>0) {
                return objectMapper.readValue(messageBody, TmNeuronMetadata.class);
            } else {
                LOG.info("No neuron data present in the message body");
                return null;
            }
        } catch (Exception e) {
            LOG.error("Error unmarshalling neuron data from {}", neuronIds, e);
            fireErrorMessage(msgHeaders, "Problems unmarshalling neuron data from " + msgHeaders.get(NeuronMessageHeaders.NEURONIDS) + " -> " + e.getMessage());
        }
        return null;
    }

    private void fireErrorMessage(Map<String, Object> msgHeaders, String errorMessage) {
        Map<String, Object> errorNotifHeaders = newMessageHeaders(msgHeaders, ImmutableMap.of(NeuronMessageHeaders.TYPE, NeuronMessageType.ERROR_PROCESSING.name()));
        errorCallback.callback(errorNotifHeaders, errorMessage.getBytes());
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

    private void handleSaveNeuron(Map<String, Object> msgHeaders, TmNeuronMetadata neuronMetadata, String user,
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
            String targetUser = MessagingUtils.getHeaderAsString(msgHeaders, NeuronMessageHeaders.TARGET_USER);
            neuronMetadata.setOwnerKey(targetUser);
            neuronMetadata.getReaders().add(targetUser);
            neuronMetadata.getWriters().add(targetUser);
            TmNeuronMetadata persistedNeuron = domainMgr.saveMetadata(neuronMetadata, user);
           // domainMgr.setPermissions(user, persistedNeuron, targetUser);
            onSuccess.accept(persistedNeuron);
        } catch (Exception e) {
            LOG.error("Error assigning new owner {} to neuron {} by {}", msgHeaders.get(NeuronMessageHeaders.TARGET_USER), neuronMetadata, user, e);
            fireErrorMessage(msgHeaders, "Problems assigning new owner to neuron: " + e.getMessage());
        }
    }

    private void handleOwnershipDecision(Map<String, Object> msgHeaders, String user, BiConsumer<TmNeuronMetadata, Boolean> onSuccess) {
        try {
            List<String> neuronIdList;
            String neuronIds = MessagingUtils.getHeaderAsString(msgHeaders, NeuronMessageHeaders.NEURONIDS);
            String workspaceId = MessagingUtils.getHeaderAsString(msgHeaders, NeuronMessageHeaders.WORKSPACE);
            if (StringUtils.isNotBlank(neuronIds)) {
                 neuronIdList = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(neuronIds);
            } else {
                neuronIdList = Collections.emptyList();
            }
            if (!neuronIdList.isEmpty()) {
                boolean decision = Boolean.parseBoolean(
                        MessagingUtils.getHeaderAsString(msgHeaders, NeuronMessageHeaders.DECISION));
                domainMgr.retrieve(workspaceId, neuronIdList, user)
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
        LOG.info("Setting owner key locally {}",neuron.getId());
        neuron.setOwnerKey(user);
        try {
            LOG.info("Saving ownership change {}",neuron.getId());
            return domainMgr.saveMetadata(neuron, user);
        } catch (Exception e) {
            LOG.info("Problem saving Neuron{}",neuron.getId());
            throw new IllegalStateException(e);
        }
    }

    private void handleOwnershipRequest(Map<String, Object> msgHeaders, String user, BiConsumer<TmNeuronMetadata, Boolean> onSuccess) {
        try {
            List<String> neuronIdList;
            String neuronIds = MessagingUtils.getHeaderAsString(msgHeaders, NeuronMessageHeaders.NEURONIDS);
            String workspaceId = MessagingUtils.getHeaderAsString(msgHeaders, NeuronMessageHeaders.WORKSPACE);
            if (StringUtils.isNotBlank(neuronIds)) {
                neuronIdList = Splitter.on(',').trimResults()
                        .omitEmptyStrings()
                        .splitToList(neuronIds.replaceAll("\\[", "").replaceAll("]", ""));
            } else {
                neuronIdList = Collections.emptyList();
            }
            if (!neuronIdList.isEmpty()) {
                LOG.info("Retrieving neuron for ownership change {}",neuronIds);
                domainMgr.retrieve(workspaceId,neuronIdList, user)
                        .forEach(neuron -> {
                            LOG.info("Starting loop for ownership change {}",neuron.getId());
                            if (neuron.getOwnerKey() != null) {
                                if (neuron.getOwnerKey().equals(sharedWorkspaceSystemOwner)) {
                                    LOG.info("Mouselight neuron {}",neuron.getId());
                                    onSuccess.accept(updateOwnership(neuron, user), true); // this neuron is owned by the system user so update the owner
                                } else if (neuron.getOwnerKey().equals(user)) {
                                    LOG.info("Already own this neuron {}",neuron.getId());
                                    onSuccess.accept(neuron, true); // this neuron is already owned by this user
                                } else {
                                    LOG.info("Ownership request cannot be granted to {} because the neuron {} is owned by {} not by the tracers group user - {}",
                                            user, neuron.getId(), neuron.getOwnerKey(), sharedWorkspaceSystemOwner);
                                    onSuccess.accept(neuron, false); // the broker cannot make a decision
                                }
                            } else {
                                LOG.info("Ownership cannot be changed because the neuron {} does not have the owner set", neuron.getId());
                                onSuccess.accept(neuron, false); // the broker cannot make a decision
                            }
                        });
            }

        } catch (Exception e) {
            LOG.error("Error processing ownership request {}", msgHeaders, e);
            fireErrorMessage(msgHeaders, "Problems processing ownership request: " + e.getMessage());
        }

    }

    private void fireApprovalMessage(TmNeuronMetadata neuron, String user, boolean approval, byte[] neuronData) {
        Map<String, Object> msgHeaders = new HashMap<>();
        msgHeaders.put(NeuronMessageHeaders.TYPE, NeuronMessageType.NEURON_OWNERSHIP_DECISION.toString());
        msgHeaders.put(NeuronMessageHeaders.NEURONIDS, ImmutableList.of(neuron.getId().toString()));
        msgHeaders.put(NeuronMessageHeaders.USER, user);
        msgHeaders.put(NeuronMessageHeaders.WORKSPACE, neuron.getWorkspaceId().toString());
        msgHeaders.put(NeuronMessageHeaders.DECISION, String.valueOf(approval));
        msgHeaders.put(NeuronMessageHeaders.DESCRIPTION, "Ownership approved by Neuron Owner");
        LOG.info("Sending out neuron ownership message for neuron ID: {} with {}", neuron.getId(), msgHeaders);
        successCallback.callback(msgHeaders, neuronData);
    }

}

package org.janelia.messaging.broker.sharedworkspace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;
import org.apache.commons.cli.*;
import org.janelia.messaging.client.ConnectionManager;
import org.janelia.messaging.client.Receiver;
import org.janelia.messaging.client.Sender;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by schauderd on 11/2/17.
 */
public class NeuronBroker implements DeliverCallback, CancelCallback {
    private static final Logger log = LoggerFactory.getLogger(NeuronBroker.class);

    String messageServer;
    String persistenceServer;
    String receiveQueue;
    String sendQueue;
    String username;
    String password;
    Receiver incomingReceiver;
    Sender broadcastRefreshSender;

    String systemOwner = "group:mouselight";

    TiledMicroscopeDomainMgr domainMgr;
    final HashMap<Long, String> ownershipRequests = new HashMap<Long,String>();


    public NeuronBroker() {}


    public TiledMicroscopeDomainMgr getDomainMgr() {
        return domainMgr;
    }

    public void setDomainMgr(TiledMicroscopeDomainMgr domainMgr) {
        this.domainMgr = domainMgr;
    }

    public Sender getBroadcastRefreshSender() {
        return broadcastRefreshSender;
    }

    public void setBroadcastRefreshSender(Sender broadcastRefreshSender) {
        this.broadcastRefreshSender = broadcastRefreshSender;
    }

    public boolean parseArgs(String[] args) {
        // read off message server host and exchange
        Options options = new Options();
        options.addOption("ms", true, "Message Server Host");
        options.addOption("ps", true, "Persistence Server Host");
        options.addOption("rec", true, "Queue to listen to.");
        options.addOption("send", true, "Queue to send refreshes to.");
        options.addOption("u", true, "Username");
        options.addOption("p", true, "Password");
        options.addOption("systemOwner", true, "Workstation user that owns system neurons");


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
            messageServer = cmd.getOptionValue("ms");
            persistenceServer = cmd.getOptionValue("ps");
            receiveQueue = cmd.getOptionValue("rec");
            sendQueue = cmd.getOptionValue("send");
            username = cmd.getOptionValue("u");
            password = cmd.getOptionValue("p");
            systemOwner = cmd.getOptionValue("systemOwner");
            if (messageServer==null || receiveQueue==null || sendQueue==null || persistenceServer==null
                    || username==null || password==null || systemOwner==null)
                return help(options);
        } catch (ParseException e) {
            System.out.println ("Error trying to parse command-line arguments");
            return help(options);
        }
        return true;
    }

    public static void main (String args[]) {
        NeuronBroker nb = new NeuronBroker();
        if (nb.parseArgs(args)) {
            nb.startBroker();
        }




    }

    private boolean help (Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("sharedworkspaceBroker", options);
        return false;
    }

    public void startBroker() {
        domainMgr = new TiledMicroscopeDomainMgr(persistenceServer);

        // set up permanent receiver on queue
        ConnectionManager connManager = ConnectionManager.getInstance();
        connManager.configureTarget(messageServer, username, password);
        incomingReceiver = new Receiver();
        incomingReceiver.init(connManager, receiveQueue, false);
        incomingReceiver.setAutoAck(true);

        broadcastRefreshSender = new Sender();
        broadcastRefreshSender.init(connManager, sendQueue, "");
        try {
            incomingReceiver.setupReceiver(this);
        } catch (Exception e) {
            System.out.println ("error setting up broker to receive/send messages");
            e.printStackTrace();
        }

    }

    @Override
    // process failed message handling, redirecting to dead-letter queue
    public void handle(String consumerTag) {
        log.info("FAILED MESSAGE DELIVERY, {}", consumerTag);
    }

    private String convertLongString (LongString data) {
        if (data!=null)
            return LongStringHelper.asLongString(data.getBytes()).toString();
        return null;
    }

    private boolean checkAndUpdateRequestLog(Long neuronId, String user) {
        synchronized (ownershipRequests) {
            if (ownershipRequests.containsKey(neuronId)) {
                return false;
            } else {
                ownershipRequests.put(neuronId, user);
                return true;
            }
        }
    }

    private boolean removeRequestLog(Long neuronId, String user) {
        synchronized (ownershipRequests) {
            if (ownershipRequests.containsKey(neuronId) && ownershipRequests.get(neuronId).equals(user)) {
                ownershipRequests.remove(neuronId);
                return true;
            } else {
                return false;
            }
        }
    }

    private void updateOwnership (TmNeuronMetadata neuron, String user) throws Exception {
        neuron.setOwnerKey(user);
        domainMgr.saveMetadata(neuron, user);
    }

    private void fireApprovalMessage(TmNeuronMetadata neuron, String user, boolean approval) throws Exception {
        Map<String,Object> msgHeaders = new HashMap<String,Object>();
        msgHeaders.put(HeaderConstants.TYPE, MessageType.NEURON_OWNERSHIP_DECISION.toString());
        List<String> neuronIds = new ArrayList<String>();
        neuronIds.add(neuron.getId().toString());
        msgHeaders.put(HeaderConstants.NEURONIDS, neuronIds);
        msgHeaders.put(HeaderConstants.USER, user);
        msgHeaders.put(HeaderConstants.WORKSPACE, neuron.getWorkspaceId().toString());
        msgHeaders.put(HeaderConstants.DECISION, new Boolean(approval).toString());
        msgHeaders.put(HeaderConstants.DESCRIPTION, "Ownership approved by Neuron Owner");

        ObjectMapper mapper = new ObjectMapper();
        msgHeaders.put(HeaderConstants.METADATA, mapper.writeValueAsString(neuron));

        log.info("Sending out neuron ownership message for neuron ID: " + neuron.getId()
                + " with approval " + approval + msgHeaders.keySet());
        broadcastRefreshSender.sendMessage(msgHeaders, new String(" ").getBytes());
    }

    private void fireOwnershipRequestMessage(TmNeuronMetadata neuron, String user) throws Exception {
        Map<String,Object> msgHeaders = new HashMap<String,Object>();
        msgHeaders.put(HeaderConstants.TYPE, MessageType.REQUEST_NEURON_OWNERSHIP.toString());
        List<String> neuronIds = new ArrayList<String>();
        neuronIds.add(neuron.getId().toString());
        msgHeaders.put(HeaderConstants.NEURONIDS, neuronIds);
        msgHeaders.put(HeaderConstants.WORKSPACE, neuron.getWorkspaceId().toString());
        msgHeaders.put(HeaderConstants.USER, user);
        broadcastRefreshSender.sendMessage(msgHeaders, new String(" ").getBytes());
    }

    @Override
    // process message from clients regarding neurons
    public void handle(String consumerTag, Delivery message) {
        // grab neurons and double-check owner
        Map<String,Object> msgHeaders = message.getProperties().getHeaders();
        if (msgHeaders!=null) {
            String user = convertLongString((LongString) msgHeaders.get(HeaderConstants.USER));
            Long workspace = Long.parseLong(convertLongString((LongString) msgHeaders.get(HeaderConstants.WORKSPACE)));
            MessageType action =  MessageType.valueOf(convertLongString((LongString) msgHeaders.get(HeaderConstants.TYPE)));
            String metadata = convertLongString((LongString) msgHeaders.get(HeaderConstants.METADATA));

            if (metadata!=null) {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    TmNeuronMetadata metadataObj = mapper.readValue(metadata, TmNeuronMetadata.class);
                    // make sure user already owns this neuron by retrieving the latest (not sure if needed since performance hit occurs) - maybe cache
                    // parse request
                    switch (action) {
                        case NEURON_DELETE:
                            try {
                                domainMgr.remove(metadataObj, user);
                                byte[] msgBody = new byte[0];
                                broadcastRefreshSender.sendMessage(msgHeaders, msgBody);
                            } catch (Exception e) {
                                // TO DO: problem creating or saving neuron data, make sure to handle gracefully
                                e.printStackTrace();
                            }
                            break;
                        case NEURON_CREATE:
                        case NEURON_SAVE_NEURONDATA:
                            try {
                                if (!user.equals(metadataObj.getOwnerKey())) {
                                    // probably should fire off rejection message
                                    return;
                                }
                                byte[] protoBufStream = message.getBody();
                                TmNeuronMetadata newMetadataObj = domainMgr.save(metadataObj, protoBufStream, user);
                                log.info("New Metadata object: {}",newMetadataObj);

                                String serializedMetadata = mapper.writeValueAsString(newMetadataObj);
                                msgHeaders.put(HeaderConstants.METADATA, serializedMetadata);

                                broadcastRefreshSender.sendMessage(msgHeaders, protoBufStream);
                                log.info("Sending out broadcast refresh for neuron save, ID: }" + newMetadataObj.getId());
                            } catch (Exception e) {
                                // TO DO: problem creating or saving neuron data, make sure to handle gracefully
                                e.printStackTrace();
                            }
                            break;
                        case NEURON_SAVE_METADATA:
                            try {
                                // TO DO: depending on performance do a real check against database metadata to confirm user owns neuron
                                if (!user.equals(metadataObj.getOwnerKey())) {
                                    // probably should fire off rejection message
                                    return;
                                }
                                TmNeuronMetadata newMetadataObj = domainMgr.saveMetadata(metadataObj, user);
                                String serializedMetadata = mapper.writeValueAsString(newMetadataObj);
                                msgHeaders.put(HeaderConstants.METADATA, serializedMetadata);
                                byte[] msgBody = new byte[0];
                                broadcastRefreshSender.sendMessage(msgHeaders, msgBody);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            break;
                        case NEURON_OWNERSHIP_DECISION:
                            // process response from neuron owner;
                            try {
                                String neuronIds = (String)convertLongString((LongString) msgHeaders.get(HeaderConstants.NEURONIDS));
                                if (neuronIds!=null) {
                                    List<String> neuronIdList = Arrays.asList(neuronIds.split(","));
                                    List<TmNeuronMetadata> neuronMetadataList = domainMgr.retrieve(neuronIdList, user);
                                    if (neuronMetadataList.size()==1) {
                                        TmNeuronMetadata neuron = neuronMetadataList.get(0);
                                        boolean decision = Boolean.parseBoolean(
                                                convertLongString((LongString) msgHeaders.get(HeaderConstants.DECISION)));
                                        if (decision) {
                                            updateOwnership(neuron, user);
                                            fireApprovalMessage(neuron, user, true);
                                        } else {
                                            fireApprovalMessage(neuron, user, false);
                                        }

                                        // clear out log for future requests
                                        removeRequestLog(neuron.getId(), user);
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            break;
                        case REQUEST_NEURON_OWNERSHIP:
                            try {
                                String neuronIds = (String)convertLongString((LongString)msgHeaders.get(HeaderConstants.NEURONIDS));
                                if (neuronIds!=null) {
                                    neuronIds = neuronIds.replaceAll("\\[","");
                                    neuronIds = neuronIds.replaceAll("\\]","");
                                    log.info("Neurons requested for ownership change {}",neuronIds);
                                    List<String> neuronIdList = Arrays.asList(neuronIds.split(","));
                                    List<TmNeuronMetadata> neuronMetadataList = domainMgr.retrieve(neuronIdList, user);
                                    // go through list and check that these aren't owned by somebody else
                                    // if they are make request to that user for ownership of those neurons
                                    if (neuronIds!=null) {
                                        for (TmNeuronMetadata neuron: neuronMetadataList) {
                                            if (neuron.getOwnerKey()!=null && neuron.getOwnerKey().equals(systemOwner)) {
                                                if (checkAndUpdateRequestLog(neuron.getId(), user)) {
                                                    // set ownership to user request, save metadata and fire off approval
                                                    updateOwnership(neuron, user);
                                                    fireApprovalMessage(neuron, user, true);
                                                    // clear out log for future requests
                                                    removeRequestLog(neuron.getId(), user);
                                                } else {
                                                    // existing request is already out there, send rejection
                                                    fireApprovalMessage(neuron, user, false);
                                                }
                                            } else {
                                                // make request to user who owns neurons for neuron ownership
                                                fireOwnershipRequestMessage(neuron, user);
                                            }
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }


                            break;
                    }


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

    }
}

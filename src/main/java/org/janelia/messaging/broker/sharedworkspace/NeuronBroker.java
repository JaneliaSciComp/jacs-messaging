package org.janelia.messaging.broker.sharedworkspace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.LongString;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.janelia.messaging.client.BulkConsumer;
import org.janelia.messaging.client.ConnectionManager;
import org.janelia.messaging.client.Receiver;
import org.janelia.messaging.client.Sender;
import org.janelia.messaging.utility.UtilityMethods;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by schauderd on 11/2/17.
 */
public class NeuronBroker implements DeliverCallback, CancelCallback {
    private static final Logger log = LoggerFactory.getLogger(NeuronBroker.class);

    String messageServer;
    String persistenceServer;
    String receiveQueue;
    String sendQueue;
    String errorQueue;
    String backupQueue;
    String username;
    String password;
    int rotatingLogCount = 1;
    Receiver incomingReceiver;
    BulkConsumer backupConsumer;
    Sender broadcastRefreshSender;
    Sender errorSender;
    String backupLocation;
    long backupInterval = 86400000L;
    int connectRetries = 3;

    String systemOwner = "group:mouselight";

    TiledMicroscopeDomainMgr domainMgr;
    ConcurrentHashMap<Long,String> ownershipRequests = new ConcurrentHashMap<Long,String>();


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

    // this takes the backupQueue as a parameter and offloads the messages to a disk location
    // once a week
    public void startScheduledQueueBackups (ConnectionManager connManager) {
        // get next Saturday
        Calendar c=Calendar.getInstance();
        long startMillis = c.getTimeInMillis();
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.add(Calendar.DATE, 1);
        long endMillis = c.getTimeInMillis();
        long initDelay = endMillis-startMillis;

        log.info("Configured scheduled backups to run with initial delay {} and every day at midnight",initDelay);
        ScheduledExecutorService backupService = Executors.newScheduledThreadPool(5);
        backupService.scheduleAtFixedRate(()->{
            try {
                log.info ("starting scheduled backup at {} to {}", new Date(), backupLocation);
                backupConsumer = new BulkConsumer();
                backupConsumer.init(connManager, backupQueue, connectRetries);
                backupConsumer.setPurgeOnCopy(true);
                int msgCount = backupConsumer.copyQueue(new FileOutputStream(backupLocation + c.get(Calendar.DAY_OF_WEEK)));
                log.info("finished scheduled backup at {} after backing up {} messages", new Date(),msgCount);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Problem with backup, {}",e.getMessage());
            }
        }, initDelay, backupInterval, TimeUnit.MILLISECONDS);

    }

    public boolean parseArgs(String[] args) {
        // read off message server host and exchange
        Options options = new Options();
        options.addOption("ms", true, "Message Server Host");
        options.addOption("ps", true, "Persistence Server Host");
        options.addOption("rec", true, "Queue to listen to.");
        options.addOption("send", true, "Queue to send refreshes to.");
        options.addOption("error", true, "Queue for error messages.");
        options.addOption("u", true, "Username");
        options.addOption("p", true, "Password");
        options.addOption("backupQueue", true, "Queue to off backups from.");
        options.addOption("backupLocation", true, "Location(directory) to offload backups to.");
        options.addOption("backupInterval", false, "time between queue backups, in milliseconds");
        options.addOption("systemOwner", true, "Workstation user that owns system neurons");


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
            messageServer = cmd.getOptionValue("ms");
            persistenceServer = cmd.getOptionValue("ps");
            receiveQueue = cmd.getOptionValue("rec");
            sendQueue = cmd.getOptionValue("send");
            errorQueue = cmd.getOptionValue("error");
            username = cmd.getOptionValue("u");
            password = cmd.getOptionValue("p");
            systemOwner = cmd.getOptionValue("systemOwner");

            // backup stuff
            backupQueue = cmd.getOptionValue("backupQueue");
            backupLocation = cmd.getOptionValue("backupLocation");
            if (messageServer==null || receiveQueue==null || sendQueue==null || persistenceServer==null
                    || username==null || password==null || systemOwner==null || backupLocation==null)
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
        incomingReceiver.init(connManager, receiveQueue, false, connectRetries);
        incomingReceiver.setAutoAck(true);

        broadcastRefreshSender = new Sender();
        broadcastRefreshSender.init(connManager, sendQueue, "", connectRetries);

        errorSender = new Sender();
        errorSender.init(connManager, errorQueue, "", connectRetries);

        startScheduledQueueBackups(connManager);
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

    private void fireErrorMessage(TmNeuronMetadata neuron, String user, String errorMessage) throws Exception {
        log.info("Error message generated. Neuron/User/Error {}{}{}",neuron.getId(),user,errorMessage);
        Map<String,Object> msgHeaders = new HashMap<String,Object>();
        msgHeaders.put(HeaderConstants.TYPE, MessageType.ERROR_PROCESSING.toString());
        List<String> neuronIds = new ArrayList<String>();
        neuronIds.add(neuron.getId().toString());
        msgHeaders.put(HeaderConstants.NEURONIDS, neuronIds);
        msgHeaders.put(HeaderConstants.WORKSPACE, neuron.getWorkspaceId().toString());
        msgHeaders.put(HeaderConstants.USER, user);
        broadcastRefreshSender.sendMessage(msgHeaders, errorMessage.getBytes());
        errorSender.sendMessage(msgHeaders, errorMessage.getBytes());
    }

    private void fireErrorMessage(Map<String,Object> msgHeaders, String errorMessage) throws Exception {
        msgHeaders.put(HeaderConstants.TYPE, MessageType.ERROR_PROCESSING.toString());
        broadcastRefreshSender.sendMessage(msgHeaders, errorMessage.getBytes());
        errorSender.sendMessage(msgHeaders, errorMessage.getBytes());
    }

    @Override
    // process message from clients regarding neurons
    public void handle(String consumerTag, Delivery message) {
        // grab neurons and double-check owner
        Map<String,Object> msgHeaders = message.getProperties().getHeaders();
        if (msgHeaders!=null) {
            String user = UtilityMethods.convertLongString((LongString) msgHeaders.get(HeaderConstants.USER));
            log.info("Processing request from user {}",user);
            Long workspace = Long.parseLong(UtilityMethods.convertLongString((LongString) msgHeaders.get(HeaderConstants.WORKSPACE)));
            MessageType action =  MessageType.valueOf(UtilityMethods.convertLongString((LongString) msgHeaders.get(HeaderConstants.TYPE)));
            String metadata = UtilityMethods.convertLongString((LongString) msgHeaders.get(HeaderConstants.METADATA));

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
                                broadcastRefreshSender.sendMessage(msgHeaders, message.getBody());
                            } catch (Exception e) {
                                e.printStackTrace();
                                fireErrorMessage(metadataObj, user, "Problems deleting neuron data: stacktrace - "
                                        + e.getMessage());
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
                                log.error("Problems creating/saving neuron data", e);
                                fireErrorMessage(metadataObj, user, "Problems creating/saving neuron data: stacktrace - "
                                        + e.getMessage());
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
                                broadcastRefreshSender.sendMessage(msgHeaders, message.getBody());
                            } catch (Exception e) {
                                log.error("Problems saving metadata", e);
                                fireErrorMessage(metadataObj, user, "Problems saving metadata: stacktrace - "
                                        + e.getMessage());
                            }
                            break;
                        case REQUEST_NEURON_ASSIGNMENT:
                            try {
                                // for now, don't check whether they are admin
                                String targetuser = UtilityMethods.convertLongString((LongString) msgHeaders.get(HeaderConstants.TARGET_USER));
                                metadataObj.setOwnerKey(targetuser);
                                metadataObj.getReaders().add(targetuser);
                                metadataObj.getWriters().add(targetuser);
                                TmNeuronMetadata newMetadataObj1 = domainMgr.saveMetadata(metadataObj, user);
                                domainMgr.setPermissions(user, newMetadataObj1, targetuser);
                                fireApprovalMessage(newMetadataObj1,user,true);
                            } catch (Exception e) {
                                e.printStackTrace();
                                log.error("Problems assigning new owner to neuron",e);
                                fireErrorMessage(metadataObj, user, "Problems assigning new owner to neuron: stacktrace - "
                                        + e.getMessage());
                            }
                            break;
                        case NEURON_OWNERSHIP_DECISION:
                            // process response from neuron owner;
                            try {
                                String neuronIds = (String)UtilityMethods.convertLongString((LongString) msgHeaders.get(HeaderConstants.NEURONIDS));
                                if (neuronIds!=null) {
                                    List<String> neuronIdList = Arrays.asList(neuronIds.split(","));
                                    List<TmNeuronMetadata> neuronMetadataList = domainMgr.retrieve(neuronIdList, user);
                                    if (neuronMetadataList.size()==1) {
                                        TmNeuronMetadata neuron = neuronMetadataList.get(0);
                                        boolean decision = Boolean.parseBoolean(
                                                UtilityMethods.convertLongString((LongString) msgHeaders.get(HeaderConstants.DECISION)));
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
                                log.error("Problems processing ownership decision",e);
                                fireErrorMessage(metadataObj, user, "Problems processing ownership decision: stacktrace - "
                                        + e.getMessage());
                            }
                            break;
                        case REQUEST_NEURON_OWNERSHIP:
                            try {
                                String neuronIds = (String)UtilityMethods.convertLongString((LongString) msgHeaders.get(HeaderConstants.NEURONIDS));
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
                                            String requestOwner = ownershipRequests.putIfAbsent(neuron.getId(), user);
                                            if (requestOwner==null) {
                                                if (neuron.getOwnerKey()!=null &&
                                                        (neuron.getOwnerKey().equals(systemOwner) ||
                                                                neuron.getOwnerKey().equals(user))) {
                                                    // set ownership to user request, save metadata and fire off approval
                                                    updateOwnership(neuron, user);
                                                    fireApprovalMessage(neuron, user, true);
                                                    // clear out log for future requests
                                                    ownershipRequests.remove(neuron.getId());
                                                } else {
                                                    // existing request is already out there, send rejection
                                                    fireApprovalMessage(neuron, user, false);
                                                }
                                            } else {
                                                // make request to user who owns neurons for neuron ownership
                                                fireApprovalMessage(neuron, user, false);
                                            }
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                log.error("Problems processing ownership request",e);
                                fireErrorMessage(msgHeaders, "Problems processing ownership request: stacktrace - "
                                        + e.getMessage());
                            }


                            break;
                    }


                } catch (Exception e) {
                    try {
                        fireErrorMessage(msgHeaders, "Problems unmarshalling neuron data in request: stacktrace - "
                                + e.getMessage());
                    }
                    catch (Exception msgError) {
                        throw new RuntimeException("some issue sending out messages through AMQP. {}",msgError);
                    }
                }
            }

        }

    }
}

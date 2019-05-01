package org.janelia.messaging.neuronbroker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.janelia.messaging.core.ConnectionManager;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConsumer;
import org.janelia.messaging.core.MessageFilter;
import org.janelia.messaging.core.MessageSender;
import org.janelia.messaging.core.OnDemandMessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by schauderd on 11/2/17.
 */
public class NeuronBroker {
    private static final Logger LOG = LoggerFactory.getLogger(NeuronBroker.class);
    private static final int CONNECT_RETRIES = 3;
    private static final long DEFAULT_BACKUP_INTERVAL_IN_MILLIS = 86400000L;
    private static final String DEFAULT_SHARED_WORKSPACE_OWNER = "group:mouselight";

    private String messageServer;
    private String messageUser;
    private String messagePassword;
    private String persistenceServer;
    private String receiveQueue;
    private String replyQueue;
    private String errorQueue;
    private String backupQueue;
    private String backupLocation;
    private long backupIntervalInMillis;
    private String sharedSpaceOwner;

    private NeuronBroker() {
    }

    private boolean parseArgs(String[] args) {
        // read off message server host and exchange
        Options options = new Options();
        options.addRequiredOption("ms", "messaging-servername", true, "Message Server Host");
        options.addOption("ps", true, "Persistence Server Host");
        options.addOption("rec", true, "Queue to listen to.");
        options.addOption("send", true, "Queue to send refreshes to.");
        options.addOption("error", true, "Queue for error messages.");
        options.addOption("u", true, "Username");
        options.addOption("p", true, "Password");
        options.addOption("backupQueue", true, "Queue to off backups from.");
        options.addOption("backupLocation", true, "Location(directory) to offload backups to.");
        options.addOption("backupInterval", true, "time between queue backups, in milliseconds");
        options.addOption("systemOwner", true, "Workstation user that owns system neurons");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            messageServer = cmd.getOptionValue("ms");
            messageUser = cmd.getOptionValue("u");
            messagePassword = cmd.getOptionValue("p");
            persistenceServer = cmd.getOptionValue("ps");
            receiveQueue = cmd.getOptionValue("rec");
            replyQueue = cmd.getOptionValue("send");
            errorQueue = cmd.getOptionValue("error");
            sharedSpaceOwner = cmd.getOptionValue("systemOwner", DEFAULT_SHARED_WORKSPACE_OWNER);

            // backup stuff
            backupQueue = cmd.getOptionValue("backupQueue");
            backupLocation = cmd.getOptionValue("backupLocation");
            backupIntervalInMillis = Long.valueOf(cmd.getOptionValue("backupInterval", String.valueOf(DEFAULT_BACKUP_INTERVAL_IN_MILLIS)));

            // validate options
            if (messageServer == null ||
                    receiveQueue == null ||
                    replyQueue == null ||
                    persistenceServer == null ||
                    messageUser == null ||
                    messagePassword == null ||
                    sharedSpaceOwner == null ||
                    backupLocation == null) {
                help(options);
                return false;
            }
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            help(options);
            return false;
        }
        return true;
    }

    private void help(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("sharedworkspaceBroker", options);
    }

    private void startBroker() {
        ConnectionManager connManager = new ConnectionManager(this.messageServer, this.messageUser, this.messagePassword, 0);

        scheduleQueueBackups(connManager, 5);

        MessageSender broadcastRefreshSender = new MessageSender(connManager);
        broadcastRefreshSender.connect(replyQueue, "", CONNECT_RETRIES);

        MessageSender errorProcessingSender = new MessageSender(connManager);
        errorProcessingSender.connect(errorQueue, "", CONNECT_RETRIES);

        MessageConsumer messageConsumer = new MessageConsumer(connManager);
        messageConsumer.setAutoAck(true);
        messageConsumer.connect(receiveQueue, receiveQueue, CONNECT_RETRIES);
        messageConsumer.setupConsumerHandlers(
                new PersistNeuronHandler(new TiledMicroscopeDomainMgr(persistenceServer), sharedSpaceOwner, broadcastRefreshSender, errorProcessingSender),
                new MessageDeliveryErrorHandler());
    }

    /**
     * this takes the backupQueue as a parameter and offloads the messages to a disk location once a week
     *
     * @param connManager
     * @param threadPoolSize
     */
    private void scheduleQueueBackups(ConnectionManager connManager, int threadPoolSize) {
        // get next Saturday
        Calendar c = Calendar.getInstance();
        long startMillis = c.getTimeInMillis();
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.add(Calendar.DATE, 1);
        long endMillis = c.getTimeInMillis();
        long initDelay = endMillis - startMillis;

        LOG.info("Configured scheduled backups to run with initial delay {} and every day at midnight", initDelay);
        ScheduledExecutorService backupService = Executors.newScheduledThreadPool(threadPoolSize);
        backupService.scheduleAtFixedRate(()->{
            try {
                LOG.info ("starting scheduled backup at {} to {}", new Date(), backupLocation);
                ObjectMapper mapper = new ObjectMapper();
                OnDemandMessageConsumer consumer = new OnDemandMessageConsumer(connManager);
                consumer.connect(backupQueue, backupQueue, CONNECT_RETRIES);
                consumer.setAutoAck(true);
                List<GenericMessage> messageList = consumer.retrieveMessages(new MessageFilter() {
                    @Override
                    public Set<String> getHeaderNames() {
                        return new LinkedHashSet<>(Arrays.asList(
                                NeuronMessageHeaders.USER,
                                NeuronMessageHeaders.WORKSPACE,
                                NeuronMessageHeaders.TYPE,
                                NeuronMessageHeaders.METADATA));
                    }

                    @Override
                    public boolean acceptMessage(GenericMessage message) {
                        return true;
                    }
                }, -1);
                String currentBackupLocation = backupLocation + c.get(Calendar.DAY_OF_WEEK);
                LOG.info("Retrieved {} messages to backup at {}", messageList.size(), currentBackupLocation);
                try (OutputStream backupStream = new FileOutputStream(currentBackupLocation)) {
                    mapper.writeValue(backupStream, consumer);
                    LOG.info("Finished scheduled backup at {} after backing up {} messages", new Date(), messageList.size());
                }
            } catch (Exception e) {
                LOG.error("Error writing to {}", backupLocation, e);
                LOG.error("Problem with backup, {}", e);
            }
        }, initDelay, backupIntervalInMillis, TimeUnit.MILLISECONDS);
    }

    public static void main(String args[]) {
        NeuronBroker nb = new NeuronBroker();
        if (nb.parseArgs(args)) {
            nb.startBroker();
        }
    }


}

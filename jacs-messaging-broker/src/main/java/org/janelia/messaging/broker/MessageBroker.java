package org.janelia.messaging.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.messaging.broker.indexingadapter.IndexingBrokerAdapter;
import org.janelia.messaging.broker.neuronadapter.NeuronBrokerAdapter;
import org.janelia.messaging.core.BulkMessageConsumer;
import org.janelia.messaging.core.ConnectionManager;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConsumer;
import org.janelia.messaging.core.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by schauderd on 11/2/17.
 */
@CommandLine.Command
public class MessageBroker {
    private static final Logger LOG = LoggerFactory.getLogger(MessageBroker.class);
    private static final int CONSUMERS_THREADPOOL_SIZE = 0;

    @CommandLine.Option(names = {"-ms"}, description = "Messaging server", required = true)
    String messagingServer;
    @CommandLine.Option(names = {"-u"}, description = "Messaging user")
    String messagingUser;
    @CommandLine.Option(names = {"-p"}, description = "Messaging password")
    String messagingPassword;
    @CommandLine.Option(names = {"-consumerThreads"}, description = "Consumers thread pool size")
    Integer consumerThreads = CONSUMERS_THREADPOOL_SIZE;

    private void startBroker(ConnectionManager connManager,
                             BrokerAdapter brokerAdapter) {

        scheduleQueueBackups(connManager, brokerAdapter, 5);

        MessageSender replySuccessSender = new MessageSender(connManager);
        replySuccessSender.connect(brokerAdapter.adapterArgs.replySuccessQueue, "", brokerAdapter.adapterArgs.connectRetries);

        MessageSender replyErrorSender = new MessageSender(connManager);
        replyErrorSender.connect(brokerAdapter.adapterArgs.replyErrorQueue, "", brokerAdapter.adapterArgs.connectRetries);

        MessageConsumer messageConsumer = new MessageConsumer(connManager);
        messageConsumer.setAutoAck(true);
        messageConsumer.connect(brokerAdapter.adapterArgs.receiveQueue, brokerAdapter.adapterArgs.receiveQueue, brokerAdapter.adapterArgs.connectRetries);
        messageConsumer.setupMessageHandler(brokerAdapter.getMessageHandler(
                (messageHeaders, messageBody) -> {
                    replySuccessSender.sendMessage(messageHeaders, messageBody);
                },
                (messageHeaders, messageBody) -> {
                    // the error handler broadcasts it to all "known" senders
                    replySuccessSender.sendMessage(messageHeaders, messageBody);
                    replyErrorSender.sendMessage(messageHeaders, messageBody);
                }
                ));
        System.out.println("!!!!!!!!!!");
    }

    /**
     * this takes the backupQueue as a parameter and offloads the messages to a disk location once a week
     *
     * @param connManager
     * @param threadPoolSize
     */
    private void scheduleQueueBackups(ConnectionManager connManager,
                                      BrokerAdapter brokerAdapter,
                                      int threadPoolSize) {
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
            String currentBackupLocation = brokerAdapter.adapterArgs.backupLocation + c.get(Calendar.DAY_OF_WEEK);
            try {
                LOG.info ("starting scheduled backup to {}", currentBackupLocation);
                ObjectMapper mapper = new ObjectMapper();
                BulkMessageConsumer consumer = new BulkMessageConsumer(connManager);
                consumer.connect(
                        brokerAdapter.adapterArgs.backupQueue,
                        brokerAdapter.adapterArgs.backupQueue,
                        brokerAdapter.adapterArgs.connectRetries);
                consumer.setAutoAck(true);
                List<GenericMessage> messageList = consumer.retrieveMessages(brokerAdapter.getMessageHeaders())
                        .collect(Collectors.toList());
                LOG.info("Retrieved {} messages to backup at {}", messageList.size(), currentBackupLocation);
                try (OutputStream backupStream = new FileOutputStream(currentBackupLocation)) {
                    mapper.writeValue(backupStream, messageList);
                    LOG.info("Finished scheduled backup at {} after backing up {} messages", new Date(), messageList.size());
                }
            } catch (Exception e) {
                LOG.error("Error writing to {}", currentBackupLocation, e);
                LOG.error("Problem with backup, {}", e);
            }
        }, initDelay, brokerAdapter.adapterArgs.backupIntervalInMillis, TimeUnit.MILLISECONDS);
    }

    private BrokerAdapter parseArgs(String[] args) {
        CommandLine commandLine = new CommandLine(this)
                .addSubcommand("neuronBroker", new NeuronBrokerAdapter())
                .addSubcommand("indexingBroker", new IndexingBrokerAdapter())
                  ;
        List<CommandLine> commandLines = commandLine.parse(args);
        System.out.println("!1LEN " + commandLines.size());
        for (CommandLine cmd : commandLines) {
            System.out.println("!!! " + cmd.getCommandName() + " " + cmd.getCommand());
        }
        if (commandLine.isUsageHelpRequested() || commandLines.size() < 2) {
            commandLine.usage(System.out);
            return null;
        } else {
            return commandLines.get(1).getCommand();
        }
    }

    public static void main(String[] args) {
        MessageBroker mb = new MessageBroker();

        BrokerAdapter ba = mb.parseArgs(args);
        if (ba != null) {
            ConnectionManager connManager = new ConnectionManager(
                    mb.messagingServer,
                    mb.messagingUser,
                    mb.messagingPassword,
                    mb.consumerThreads);
            mb.startBroker(connManager, ba);
        }

    }

}

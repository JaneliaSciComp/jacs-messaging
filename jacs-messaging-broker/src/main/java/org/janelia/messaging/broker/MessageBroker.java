package org.janelia.messaging.broker;

import org.janelia.messaging.broker.indexingadapter.IndexingBrokerAdapterFactory;
import org.janelia.messaging.broker.neuronadapter.NeuronBrokerAdapterFactory;
import org.janelia.messaging.core.ConnectionManager;
import org.janelia.messaging.core.MessageConsumer;
import org.janelia.messaging.core.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(threadPoolSize);
        brokerAdapter.schedulePeriodicTasks(connManager, scheduledExecutorService);
    }

    private BrokerAdapterFactory<?> parseArgs(String[] args) {
        CommandLine commandLine = new CommandLine(this)
                .addSubcommand("neuronBroker", new NeuronBrokerAdapterFactory())
                .addSubcommand("indexingBroker", new IndexingBrokerAdapterFactory())
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

        BrokerAdapterFactory<?> ba = mb.parseArgs(args);
        if (ba != null) {
            ConnectionManager connManager = new ConnectionManager(
                    mb.messagingServer,
                    mb.messagingUser,
                    mb.messagingPassword,
                    mb.consumerThreads);
            mb.startBroker(connManager, ba.createBrokerAdapter());
        }
    }

}

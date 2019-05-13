package org.janelia.messaging.broker;

import org.janelia.messaging.broker.indexingadapter.IndexingBrokerAdapterFactory;
import org.janelia.messaging.broker.neuronadapter.NeuronBrokerAdapterFactory;
import org.janelia.messaging.core.impl.AsyncMessageConsumerImpl;
import org.janelia.messaging.core.impl.ConnectionManager;
import org.janelia.messaging.core.impl.MessageSenderImpl;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by schauderd on 11/2/17.
 */
@CommandLine.Command
public class MessageBroker {
    private void startBroker(ConnectionManager connManager,
                             BrokerAdapter brokerAdapter) {

        scheduleQueueBackups(connManager, brokerAdapter, 5);

        MessageSenderImpl replySuccessSender = new MessageSenderImpl(connManager);
        replySuccessSender.connect(
                brokerAdapter.adapterArgs.messagingServer,
                brokerAdapter.adapterArgs.messagingUser,
                brokerAdapter.adapterArgs.messagingPassword,
                brokerAdapter.adapterArgs.replySuccessQueue,
                "",
                brokerAdapter.adapterArgs.connectRetries);

        MessageSenderImpl replyErrorSender = new MessageSenderImpl(connManager);
        replyErrorSender.connect(
                brokerAdapter.adapterArgs.messagingServer,
                brokerAdapter.adapterArgs.messagingUser,
                brokerAdapter.adapterArgs.messagingPassword,
                brokerAdapter.adapterArgs.replyErrorQueue,
                "",
                brokerAdapter.adapterArgs.connectRetries);

        AsyncMessageConsumerImpl messageConsumer = new AsyncMessageConsumerImpl(connManager);
        messageConsumer.setAutoAck(true);
        messageConsumer.connect(
                brokerAdapter.adapterArgs.messagingServer,
                brokerAdapter.adapterArgs.messagingUser,
                brokerAdapter.adapterArgs.messagingPassword,
                brokerAdapter.adapterArgs.receiveQueue,
                brokerAdapter.adapterArgs.consumerThreads,
                brokerAdapter.adapterArgs.connectRetries);
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
            mb.startBroker(ConnectionManager.getInstance(), ba.createBrokerAdapter());
        }
    }

}

package org.janelia.messaging.broker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.janelia.messaging.broker.indexingadapter.IndexingBrokerAdapterFactory;
import org.janelia.messaging.broker.neuronadapter.NeuronBrokerAdapterFactory;
import org.janelia.messaging.config.ApplicationConfig;
import org.janelia.messaging.config.ApplicationConfigProvider;
import org.janelia.messaging.core.ConnectionManager;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.impl.AsyncMessageConsumerImpl;
import org.janelia.messaging.core.impl.MessageSenderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageBroker {
    private static final Logger LOG = LoggerFactory.getLogger(MessageBroker.class);
    private static final int CONSUMERS_THREADPOOL_SIZE = 0;

    @Parameter(names = {"-ms"}, description = "Messaging server", required = true)
    String messagingServer;
    @Parameter(names = {"-u"}, description = "Messaging user")
    String messagingUser;
    @Parameter(names = {"-p"}, description = "Messaging password")
    String messagingPassword;
    @Parameter(names = {"-consumerThreads"}, description = "Consumers thread pool size")
    Integer consumerThreads = CONSUMERS_THREADPOOL_SIZE;
    @Parameter(names = {"-config"}, description = "Config file")
    String configFile;
    @Parameter(names = {"-h"}, description = "Display usage message")
    boolean usageRequested = false;
    @DynamicParameter(names = "-D", description = "Dynamic application parameters that could override application properties")
    Map<String, String> appDynamicConfig = new HashMap<>();

    private void startBroker(MessageConnection messageConnection, BrokerAdapter brokerAdapter, int scheduledTasksPoolSize) {
        ScheduledExecutorService scheduledAdapterTaskExecutorService = Executors.newScheduledThreadPool(scheduledTasksPoolSize);
        brokerAdapter.getScheduledTasks(messageConnection).stream()
                .filter(st -> st.command != null)
                .forEach(st -> scheduledAdapterTaskExecutorService.scheduleAtFixedRate(
                        st.command,
                        st.initialDelayInMillis,
                        st.intervalInMillis,
                        TimeUnit.MILLISECONDS
                ));

        MessageSenderImpl replySuccessSender = new MessageSenderImpl(messageConnection);
        replySuccessSender.connectTo(
                brokerAdapter.adapterArgs.getSuccessResponseExchange(),
                brokerAdapter.adapterArgs.getSuccessResponseRouting());

        MessageSenderImpl replyErrorSender = new MessageSenderImpl(messageConnection);
        replyErrorSender.connectTo(
                brokerAdapter.adapterArgs.getErrorResponseExchange(),
                brokerAdapter.adapterArgs.getErrorResponseRouting());

        AsyncMessageConsumerImpl messageConsumer = new AsyncMessageConsumerImpl(messageConnection);
        messageConsumer.setAutoAck(brokerAdapter.useAutoAck());
        messageConsumer.connectTo(brokerAdapter.adapterArgs.getReceiveQueue());
        messageConsumer.subscribe(brokerAdapter.getMessageHandler(
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

    private boolean parseArgs(String[] args) {
        JCommander cmdlineParser = new JCommander(this);
        cmdlineParser.parse(args);
        if (this.usageRequested) {
            cmdlineParser.usage();
            return false;
        } else {
            return true;
        }
    }

    public static void main(String[] args) {
        MessageBroker mb = new MessageBroker();
        if (!mb.parseArgs(args)) {
            return;
        }
        MessageConnection messageConnection = ConnectionManager.getInstance()
                .getConnection(mb.messagingServer, mb.messagingUser, mb.messagingPassword, mb.consumerThreads);
        ApplicationConfig config = new ApplicationConfigProvider()
                .fromDefaultResources()
                .fromEnvVar("JACSBROKER_CONFIG")
                .fromFile(mb.configFile)
                .fromMap(mb.appDynamicConfig)
                .build();

        BrokerAdapterFactory<?>[] brokerAdapterFactories = new BrokerAdapterFactory<?>[] {
                new NeuronBrokerAdapterFactory(),
                new IndexingBrokerAdapterFactory()
        };

        for (BrokerAdapterFactory<?> brokerAdapterFactory : brokerAdapterFactories) {
            BrokerAdapter ba = brokerAdapterFactory.createBrokerAdapter(brokerAdapterFactory.getBrokerAdapterArgs(config));
            if (ba.isEnabled()) {
                LOG.info("Start broker {}", brokerAdapterFactory.getName());
                mb.startBroker(messageConnection, ba, 5);
            }
        }
    }

}

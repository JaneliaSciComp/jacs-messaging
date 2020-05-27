package org.janelia.messaging.broker;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.janelia.messaging.broker.indexingadapter.IndexingBrokerAdapterFactory;
import org.janelia.messaging.broker.neuronadapter.NeuronBrokerAdapterFactory;
import org.janelia.messaging.config.ApplicationConfig;
import org.janelia.messaging.config.ApplicationConfigProvider;
import org.janelia.messaging.core.ConnectionManager;
import org.janelia.messaging.core.ConnectionParameters;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.impl.AsyncMessageConsumerImpl;
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

        AsyncMessageConsumerImpl messageConsumer = new AsyncMessageConsumerImpl(messageConnection);
        messageConsumer.setAutoAck(brokerAdapter.useAutoAck());
        messageConsumer.connectTo(brokerAdapter.adapterArgs.getReceiveQueue());
        messageConsumer.subscribe(brokerAdapter.getMessageHandler(messageConnection));
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
        ApplicationConfig config = new ApplicationConfigProvider()
                .fromEnv()
                .fromEnvVar("JACSBROKER_CONFIG")
                .fromFile(mb.configFile)
                .fromMap(mb.appDynamicConfig)
                .build();
        String brokerList = config.getStringPropertyValue("brokers", "Neuron,Indexing");
        List<BrokerAdapterFactory> factories = new ArrayList<>();
        if (brokerList.trim().length()>0) {
            String[] brokers = brokerList.split(",");
            for (int i=0; i<brokers.length; i++) {
                // need to collate annotations or something for this
                if (brokers[i].equals("Indexing")) {
                    factories.add(new IndexingBrokerAdapterFactory());
                } else if (brokers[i].equals("Neuron")) {
                    factories.add(new NeuronBrokerAdapterFactory());
                }
            }
        }

        BrokerAdapterFactory<?>[] brokerAdapterFactories = factories.stream().toArray(BrokerAdapterFactory[]::new);

        for (BrokerAdapterFactory<?> brokerAdapterFactory : brokerAdapterFactories) {
            BrokerAdapter ba = brokerAdapterFactory.createBrokerAdapter(brokerAdapterFactory.getBrokerAdapterArgs(config));
            if (ba.isEnabled()) {
                AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);
                MessageConnection messageConnection = ConnectionManager.getInstance()
                        .getConnection(new ConnectionParameters()
                                .setHost(mb.messagingServer)
                                .setUser(mb.messagingUser)
                                .setPassword(mb.messagingPassword)
                                .setMaxRetries(config.getIntegerPropertyValue("connection.maxRetries", 1))
                                .setPauseBetweenRetriesInMillis(config.getLongPropertyValue("connection.pauseBetweenRetriesInMillis", 100L))
                                .setConsumerThreads(config.getIntegerPropertyValue("connection.consumerThreads", 1)),
                                (error) -> {
                                    errorHolder.set(error);
                                }
                        )
                        ;
                if (errorHolder.get() == null) {
                    LOG.info("Start broker {}", brokerAdapterFactory.getName());
                    mb.startBroker(messageConnection, ba, 5);
                } else {
                    LOG.error("Could not connect to {} using {}", mb.messagingServer, mb.messagingUser, errorHolder.get());
                }
            }
        }
    }

}

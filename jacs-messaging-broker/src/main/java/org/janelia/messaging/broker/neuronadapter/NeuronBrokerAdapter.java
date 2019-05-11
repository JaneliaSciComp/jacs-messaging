package org.janelia.messaging.broker.neuronadapter;

import com.google.common.collect.ImmutableSet;
import org.janelia.messaging.broker.BrokerAdapter;
import org.janelia.messaging.core.MessageHandler;
import picocli.CommandLine;

import java.util.Set;

public class NeuronBrokerAdapter extends BrokerAdapter {

    private static final String DEFAULT_SHARED_WORKSPACE_OWNER = "group:mouselight";

    @CommandLine.Option(names = {"-ps"}, description = "Persistence server", required = true)
    String persistenceServer;
    @CommandLine.Option(names = {"-systemOwner"}, description = "Shared workspace user key")
    String sharedSpaceOwner = DEFAULT_SHARED_WORKSPACE_OWNER;

    @Override
    public MessageHandler getMessageHandler(MessageHandler.HandlerCallback successCallback, MessageHandler.HandlerCallback errorCallback) {
        return new PersistNeuronHandler(
                new TiledMicroscopeDomainMgr(persistenceServer),
                sharedSpaceOwner,
                successCallback,
                errorCallback
        );
    }

    @Override
    public Set<String> getMessageHeaders() {
        return ImmutableSet.of(
                NeuronMessageHeaders.USER,
                NeuronMessageHeaders.WORKSPACE,
                NeuronMessageHeaders.TYPE,
                NeuronMessageHeaders.METADATA
        );
    }
}

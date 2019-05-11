package org.janelia.messaging.broker.neuronadapter;

import org.janelia.messaging.broker.BrokerAdapterFactory;
import picocli.CommandLine;

public class NeuronBrokerAdapterFactory extends BrokerAdapterFactory<NeuronBrokerAdapter> {
    private static final String DEFAULT_SHARED_WORKSPACE_OWNER = "group:mouselight";

    @CommandLine.Option(names = {"-ps"}, description = "Persistence server", required = true)
    String persistenceServer;
    @CommandLine.Option(names = {"-systemOwner"}, description = "Shared workspace user key")
    String sharedSpaceOwner = DEFAULT_SHARED_WORKSPACE_OWNER;

    @Override
    public NeuronBrokerAdapter createBrokerAdapter() {
        return new NeuronBrokerAdapter(adapterArgs, persistenceServer, sharedSpaceOwner);
    }
}

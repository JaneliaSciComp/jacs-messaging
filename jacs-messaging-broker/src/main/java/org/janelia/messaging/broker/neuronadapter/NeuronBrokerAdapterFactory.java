package org.janelia.messaging.broker.neuronadapter;

import org.janelia.messaging.broker.BrokerAdapterArgs;
import org.janelia.messaging.broker.BrokerAdapterFactory;

import javax.annotation.Nonnull;

public class NeuronBrokerAdapterFactory extends BrokerAdapterFactory<NeuronBrokerAdapter> {
    private static final String DEFAULT_SHARED_WORKSPACE_OWNER = "group:mouselight";

    @Nonnull
    @Override
    public String getName() {
        return "neuronBroker";
    }

    @Override
    public NeuronBrokerAdapter createBrokerAdapter(BrokerAdapterArgs brokerAdapterArgs) {
        return new NeuronBrokerAdapter(
                brokerAdapterArgs,
                brokerAdapterArgs.getAdapterConfig("persistenceServer"),
                brokerAdapterArgs.getAdapterConfigOrDefault("sharedSpaceOwner", DEFAULT_SHARED_WORKSPACE_OWNER));
    }
}

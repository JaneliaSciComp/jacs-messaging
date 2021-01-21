package org.janelia.messaging.broker.agentadapter;

import org.janelia.messaging.broker.BrokerAdapterArgs;
import org.janelia.messaging.broker.BrokerAdapterFactory;

import javax.annotation.Nonnull;

public class AgentBrokerAdapterFactory extends BrokerAdapterFactory<AgentsBrokerAdapter> {
    @Nonnull
    @Override
    public String getName() {
        return "agentBroker";
    }

    @Override
    public AgentsBrokerAdapter createBrokerAdapter(BrokerAdapterArgs brokerAdapterArgs) {
        return new AgentsBrokerAdapter(
                brokerAdapterArgs,
                brokerAdapterArgs.getAdapterConfig("persistenceServer"));
    }
}

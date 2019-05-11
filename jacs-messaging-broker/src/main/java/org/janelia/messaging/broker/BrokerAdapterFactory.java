package org.janelia.messaging.broker;

import picocli.CommandLine;

public abstract class BrokerAdapterFactory<T extends BrokerAdapter> {
    @CommandLine.Mixin
    public BrokerAdapterArgs adapterArgs;

    public abstract T createBrokerAdapter();
}

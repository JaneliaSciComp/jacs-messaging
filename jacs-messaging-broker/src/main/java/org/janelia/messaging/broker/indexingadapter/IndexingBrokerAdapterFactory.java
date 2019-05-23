package org.janelia.messaging.broker.indexingadapter;

import org.janelia.messaging.broker.BrokerAdapterArgs;
import org.janelia.messaging.broker.BrokerAdapterFactory;

import javax.annotation.Nonnull;

public class IndexingBrokerAdapterFactory extends BrokerAdapterFactory<IndexingBrokerAdapter> {

    @Nonnull
    @Override
    public String getName() {
        return "indexingBroker";
    }

    @Override
    public IndexingBrokerAdapter createBrokerAdapter(BrokerAdapterArgs brokerAdapterArgs) {
        return new IndexingBrokerAdapter(brokerAdapterArgs);
    }
}

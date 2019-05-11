package org.janelia.messaging.broker.indexingadapter;

import org.janelia.messaging.broker.BrokerAdapterFactory;
import picocli.CommandLine;

public class IndexingBrokerAdapterFactory extends BrokerAdapterFactory<IndexingBrokerAdapter> {
    @CommandLine.Option(names = {"-indexingServer"}, description = "indexing server URL")
    String indexingServer;

    @Override
    public IndexingBrokerAdapter createBrokerAdapter() {
        return new IndexingBrokerAdapter(adapterArgs, indexingServer);
    }
}

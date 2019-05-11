package org.janelia.messaging.broker.indexingadapter;

import com.google.common.collect.ImmutableSet;
import org.janelia.messaging.broker.BrokerAdapter;
import org.janelia.messaging.core.MessageHandler;
import picocli.CommandLine;

import java.util.Set;

public class IndexingBrokerAdapter extends BrokerAdapter {
    private static final int WORK_DELAY_MILLIS = 5000;

    @CommandLine.Option(names = {"-indexingServer"}, description = "indexing server URL")
    String indexingServer;

    @Override
    public MessageHandler getMessageHandler(MessageHandler.HandlerCallback successCallback, MessageHandler.HandlerCallback errorCallback) {
        return new IndexingHandler(indexingServer, WORK_DELAY_MILLIS);
    }

    @Override
    public Set<String> getMessageHeaders() {
        return ImmutableSet.of(
                IndexingMessageHeaders.OBJECT_CLASS,
                IndexingMessageHeaders.OBJECT_ID,
                IndexingMessageHeaders.ANCESTOR_ID
        );
    }
}

package org.janelia.messaging.broker.indexingadapter;

import com.beust.jcommander.Parameter;
import org.janelia.messaging.broker.BrokerAdapter;
import org.janelia.messaging.core.MessageHandler;

import java.util.Set;

public class IndexingBrokerAdapter extends BrokerAdapter {
    private static final int WORK_DELAY_MILLIS = 5000;

    @Parameter(names = {"-indexingServer"}, description = "indexing server URL")
    String indexingServer;

    @Override
    public MessageHandler getMessageHandler(MessageHandler.HandlerCallback successCallback, MessageHandler.HandlerCallback errorCallback) {
        return new IndexingHandler(indexingServer, WORK_DELAY_MILLIS);
    }

    @Override
    public Set<String> getMessageHeaders() {
        return null; // !!!!!!!!!! FIXME
    }
}

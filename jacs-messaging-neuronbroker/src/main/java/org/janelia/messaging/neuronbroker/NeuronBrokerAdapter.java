package org.janelia.messaging.neuronbroker;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableSet;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;
import org.janelia.messaging.BrokerAdapter;
import org.janelia.messaging.core.MessageSender;

import java.util.Set;

public class NeuronBrokerAdapter extends BrokerAdapter {
    private static final String DEFAULT_SHARED_WORKSPACE_OWNER = "group:mouselight";

    @Parameter(names = {"-ps"}, description = "Persistence server", required = true)
    String persistenceServer;
    @Parameter(names = {"-systemOwner"}, description = "Shared workspace user key")
    String sharedSpaceOwner = DEFAULT_SHARED_WORKSPACE_OWNER;

    @Override
    public DeliverCallback getDeliveryHandler(MessageSender replySuccessSender, MessageSender replyErrorSender) {
        return new PersistNeuronHandler(
                new TiledMicroscopeDomainMgr(persistenceServer),
                sharedSpaceOwner,
                replySuccessSender,
                replyErrorSender
        );
    }

    @Override
    public CancelCallback getErrorHandler(MessageSender replyErrorSender) {
        return new MessageDeliveryErrorHandler();
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

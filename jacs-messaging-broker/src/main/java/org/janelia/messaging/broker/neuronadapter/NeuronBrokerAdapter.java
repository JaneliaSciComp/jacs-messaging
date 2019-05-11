package org.janelia.messaging.broker.neuronadapter;

import com.google.common.collect.ImmutableSet;
import org.janelia.messaging.broker.BrokerAdapter;
import org.janelia.messaging.broker.BrokerAdapterArgs;
import org.janelia.messaging.core.MessageHandler;

import java.util.Set;

public class NeuronBrokerAdapter extends BrokerAdapter {

    final String persistenceServer;
    final String sharedSpaceOwner;

    public NeuronBrokerAdapter(BrokerAdapterArgs adapterArgs, String persistenceServer, String sharedSpaceOwner) {
        super(adapterArgs);
        this.persistenceServer = persistenceServer;
        this.sharedSpaceOwner = sharedSpaceOwner;
    }

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

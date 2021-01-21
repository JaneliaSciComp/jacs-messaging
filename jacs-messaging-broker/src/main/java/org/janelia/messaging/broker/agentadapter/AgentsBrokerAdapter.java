package org.janelia.messaging.broker.agentadapter;

import org.janelia.messaging.broker.BrokerAdapter;
import org.janelia.messaging.broker.BrokerAdapterArgs;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.core.MessageSender;
import org.janelia.messaging.core.impl.MessageSenderImpl;

public class AgentsBrokerAdapter extends BrokerAdapter {

    final String persistenceServer;

    AgentsBrokerAdapter(BrokerAdapterArgs adapterArgs, String persistenceServer) {
        super(adapterArgs);
        this.persistenceServer = persistenceServer;
    }

    @Override
    public MessageHandler getMessageHandler(MessageConnection messageConnection) {
        MessageSender replySuccessSender = new MessageSenderImpl(messageConnection);
        replySuccessSender.connectTo(
                adapterArgs.getSuccessResponseExchange(),
                adapterArgs.getSuccessResponseRouting());

        MessageSender replyErrorSender = new MessageSenderImpl(messageConnection);
        replyErrorSender.connectTo(
                adapterArgs.getErrorResponseExchange(),
                adapterArgs.getErrorResponseRouting());

        MessageHandler.HandlerCallback successCallback = ((messageHeaders, messageBody) -> replySuccessSender.sendMessage(messageHeaders, messageBody));
        MessageHandler.HandlerCallback errorCallback = successCallback.andThen(((messageHeaders, messageBody) -> replyErrorSender.sendMessage(messageHeaders, messageBody)));

        return new AgentHandler(
                successCallback,
                errorCallback
        );
    }

}

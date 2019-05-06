package org.janelia.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.messaging.core.ConnectionManager;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConsumer;
import org.janelia.messaging.core.MessageFilter;
import org.janelia.messaging.core.MessageSender;
import org.janelia.messaging.core.OnDemandMessageConsumer;
import org.janelia.messaging.neuronbroker.NeuronBrokerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by schauderd on 11/2/17.
 */
public class MessageBroker {
    private static final Logger LOG = LoggerFactory.getLogger(MessageBroker.class);

    private final BrokerAdapter brokerAdapter;

    private MessageBroker(BrokerAdapter brokerAdapter) {
        this.brokerAdapter = brokerAdapter;
    }

    private void startBroker() {
        ConnectionManager connManager = new ConnectionManager(
                brokerAdapter.getMessagingServer(),
                brokerAdapter.getMessagingUser(),
                brokerAdapter.getMessagingPassword(),
                brokerAdapter.getThreadPoolSize());

        scheduleQueueBackups(connManager, 5);

        MessageSender replySuccessSender = new MessageSender(connManager);
        replySuccessSender.connect(brokerAdapter.getReplySuccessQueue(), "", brokerAdapter.getConnectRetries());

        MessageSender replyErrorSender = new MessageSender(connManager);
        replyErrorSender.connect(brokerAdapter.getReplyErrorQueue(), "", brokerAdapter.getConnectRetries());

        MessageConsumer messageConsumer = new MessageConsumer(connManager);
        messageConsumer.setAutoAck(true);
        messageConsumer.connect(brokerAdapter.getReceiveQueue(), brokerAdapter.getReceiveQueue(), brokerAdapter.getConnectRetries());
        messageConsumer.setupConsumerHandlers(
                brokerAdapter.getDeliveryHandler(replySuccessSender, replyErrorSender),
                brokerAdapter.getErrorHandler(replyErrorSender));
    }

    /**
     * this takes the backupQueue as a parameter and offloads the messages to a disk location once a week
     *
     * @param connManager
     * @param threadPoolSize
     */
    private void scheduleQueueBackups(ConnectionManager connManager, int threadPoolSize) {
        // get next Saturday
        Calendar c = Calendar.getInstance();
        long startMillis = c.getTimeInMillis();
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.add(Calendar.DATE, 1);
        long endMillis = c.getTimeInMillis();
        long initDelay = endMillis - startMillis;

        LOG.info("Configured scheduled backups to run with initial delay {} and every day at midnight", initDelay);
        ScheduledExecutorService backupService = Executors.newScheduledThreadPool(threadPoolSize);
        backupService.scheduleAtFixedRate(()->{
            String currentBackupLocation = brokerAdapter.getBackupLocation() + c.get(Calendar.DAY_OF_WEEK);
            try {
                LOG.info ("starting scheduled backup to {}", currentBackupLocation);
                ObjectMapper mapper = new ObjectMapper();
                OnDemandMessageConsumer consumer = new OnDemandMessageConsumer(connManager);
                consumer.connect(
                        brokerAdapter.getBackupQueue(),
                        brokerAdapter.getBackupQueue(),
                        brokerAdapter.getConnectRetries());
                consumer.setAutoAck(true);
                List<GenericMessage> messageList = consumer.retrieveMessages(new MessageFilter() {
                    @Override
                    public Set<String> getHeaderNames() {
                        return brokerAdapter.getMessageHeaders();
                    }

                    @Override
                    public boolean acceptMessage(GenericMessage message) {
                        return true;
                    }
                }, -1);
                LOG.info("Retrieved {} messages to backup at {}", messageList.size(), currentBackupLocation);
                try (OutputStream backupStream = new FileOutputStream(currentBackupLocation)) {
                    mapper.writeValue(backupStream, consumer);
                    LOG.info("Finished scheduled backup at {} after backing up {} messages", new Date(), messageList.size());
                }
            } catch (Exception e) {
                LOG.error("Error writing to {}", currentBackupLocation, e);
                LOG.error("Problem with backup, {}", e);
            }
        }, initDelay, brokerAdapter.getBackupIntervalInMillis(), TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        BrokerAdapter brokerAdapter = new NeuronBrokerAdapter();
        MessageBroker nb = new MessageBroker(brokerAdapter);
        if (brokerAdapter.parseArgs(args)) {
            nb.startBroker();
        }
    }

}

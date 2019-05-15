package org.janelia.messaging.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.core.impl.BulkMessageConsumerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class BrokerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerAdapter.class);

    public static class ScheduledTask {
        public Runnable command;
        public long initialDelay;
        public long interval;
        public TimeUnit timeUnit;
    }

    final BrokerAdapterArgs adapterArgs;

    protected BrokerAdapter(BrokerAdapterArgs adapterArgs) {
        this.adapterArgs = adapterArgs;
    }

    public boolean useAutoAck() {
        return true;
    }

    public abstract MessageHandler getMessageHandler(MessageHandler.HandlerCallback successCallback,
                                                     MessageHandler.HandlerCallback errorCallback);

    public void schedulePeriodicTasks(MessageConnection messageConnection, ScheduledExecutorService scheduledExecutorService) {
        ScheduledTask queueBackTask = getBackupQueueTask(messageConnection);
        scheduledExecutorService.scheduleAtFixedRate(
                queueBackTask.command,
                queueBackTask.initialDelay,
                queueBackTask.interval,
                queueBackTask.timeUnit);
    }

    private ScheduledTask getBackupQueueTask(MessageConnection messageConnection) {
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

        Runnable command = () -> {
            String currentBackupLocation = adapterArgs.backupLocation + c.get(Calendar.DAY_OF_WEEK);
            try {
                LOG.info ("starting scheduled backup to {}", currentBackupLocation);
                ObjectMapper mapper = new ObjectMapper();
                BulkMessageConsumerImpl consumer = new BulkMessageConsumerImpl(messageConnection);
                consumer.connectTo(adapterArgs.backupQueue);
                consumer.setAutoAck(true);
                List<GenericMessage> messageList = consumer.retrieveMessages().collect(Collectors.toList());
                LOG.info("Retrieved {} messages to backup at {}", messageList.size(), currentBackupLocation);
                try (OutputStream backupStream = new FileOutputStream(currentBackupLocation)) {
                    mapper.writeValue(backupStream, messageList);
                    LOG.info("Finished scheduled backup at {} after backing up {} messages", new Date(), messageList.size());
                }
            } catch (Exception e) {
                LOG.error("Error writing to {}", currentBackupLocation, e);
                LOG.error("Problem with backup, {}", e);
            }
        };
        ScheduledTask scheduledTask = new ScheduledTask();
        scheduledTask.command = command;
        scheduledTask.initialDelay = initDelay;
        scheduledTask.interval = adapterArgs.backupIntervalInMillis;
        scheduledTask.timeUnit = TimeUnit.MILLISECONDS;
        return scheduledTask;
    }
}

package org.janelia.messaging.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.core.impl.BulkMessageConsumerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BrokerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerAdapter.class);

    public static class ScheduledTask {
        public Runnable command;
        public long initialDelayInMillis;
        public long intervalInMillis;
    }

    final BrokerAdapterArgs adapterArgs;

    protected BrokerAdapter(BrokerAdapterArgs adapterArgs) {
        this.adapterArgs = adapterArgs;
    }

    public boolean useAutoAck() {
        return true;
    }

    public boolean isEnabled() {
        return StringUtils.isNotBlank(adapterArgs.getReceiveQueue());
    }

    public abstract MessageHandler getMessageHandler(MessageHandler.HandlerCallback successCallback,
                                                     MessageHandler.HandlerCallback errorCallback);

    public List<ScheduledTask> getScheduledTasks(MessageConnection messageConnection) {
        return Arrays.asList(getBackupQueueTask(messageConnection));
    }

    protected ScheduledTask getBackupQueueTask(MessageConnection messageConnection) {
        // get next Saturday
        Calendar c = Calendar.getInstance();
        long startMillis = c.getTimeInMillis();
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.add(Calendar.DATE, 1);
        long endMillis = c.getTimeInMillis();
        long initDelayInMillis = endMillis - startMillis;

        LOG.info("Configured scheduled backups to run with initial delay {} and every day at midnight", initDelayInMillis);

        Runnable command;
        if (StringUtils.isNotBlank(adapterArgs.getBackupLocation())) {
            command = () -> {
                String currentBackupLocation = adapterArgs.getBackupLocation() + c.get(Calendar.DAY_OF_WEEK);
                try {
                    LOG.info ("starting scheduled backup to {}", currentBackupLocation);
                    ObjectMapper mapper = new ObjectMapper();
                    BulkMessageConsumerImpl consumer = new BulkMessageConsumerImpl(messageConnection);
                    consumer.connectTo(adapterArgs.getBackupQueue());
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
        } else {
            command = null;
        }
        ScheduledTask scheduledBackupTask = new ScheduledTask();
        scheduledBackupTask.command = command;
        scheduledBackupTask.initialDelayInMillis = initDelayInMillis;
        scheduledBackupTask.intervalInMillis = adapterArgs.getBackupIntervalInMillis();
        return scheduledBackupTask;
    }
}

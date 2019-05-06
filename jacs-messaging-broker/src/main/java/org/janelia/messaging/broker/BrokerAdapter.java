package org.janelia.messaging.broker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;
import org.janelia.messaging.core.MessageSender;

import java.util.Set;

public abstract class BrokerAdapter {
    private static final int CONNECT_RETRIES = 3;
    private static final int DEFAULT_THREADPOOL_SIZE = 0;
    private static final long DEFAULT_BACKUP_INTERVAL_IN_MILLIS = 86400000L;

    @Parameter(names = {"-ms"}, description = "Messaging server", required = true)
    String messagingServer;
    @Parameter(names = {"-u"}, description = "Messaging user")
    String messagingUser;
    @Parameter(names = {"-p"}, description = "Messaging password")
    String messagingPassword;
    @Parameter(names = {"-rec"}, description = "Receiving queue", required = true)
    String receiveQueue;
    @Parameter(names = {"-send"}, description = "Response queue to reply success", required = true)
    String replySuccessQueue;
    @Parameter(names = {"-error"}, description = "Response queue to reply error", required = true)
    String replyErrorQueue;
    @Parameter(names = {"-backupQueue"}, description = "Backup queue")
    String backupQueue;
    @Parameter(names = {"-backupInterval"}, description = "Interval between two consecutive backups in milliseconds")
    Long backupIntervalInMillis = DEFAULT_BACKUP_INTERVAL_IN_MILLIS;
    @Parameter(names = {"-backupLocation"}, description = "Backup queue")
    String backupLocation;
    @Parameter(names = "-h", description = "Display help", arity = 0)
    boolean displayUsage = false;

    public String getMessagingServer() {
        return messagingServer;
    }

    public String getMessagingUser() {
        return messagingUser;
    }

    public String getMessagingPassword() {
        return messagingPassword;
    }

    public int getThreadPoolSize() {
        return DEFAULT_THREADPOOL_SIZE;
    }

    public int getConnectRetries() {
        return CONNECT_RETRIES;
    }

    public String getReceiveQueue() {
        return receiveQueue;
    }

    public String getReplySuccessQueue() {
        return replySuccessQueue;
    }

    public String getReplyErrorQueue() {
        return replyErrorQueue;
    }

    public String getBackupQueue() {
        return backupQueue;
    }

    public String getBackupLocation() {
        return backupLocation;
    }

    public abstract DeliverCallback getDeliveryHandler(MessageSender replySuccessSender, MessageSender replyErrorSender);

    public abstract CancelCallback getErrorHandler(MessageSender replyErrorSender);

    public abstract Set<String> getMessageHeaders();

    public long getBackupIntervalInMillis() {
        return backupIntervalInMillis;
    }

    public boolean parseArgs(String[] args) {
        JCommander cmdlineParser = new JCommander(this);
        cmdlineParser.parse(args);
        if (displayUsage) {
            cmdlineParser.usage(new StringBuilder());
            return false;
        } else {
            return true;
        }
    }

}

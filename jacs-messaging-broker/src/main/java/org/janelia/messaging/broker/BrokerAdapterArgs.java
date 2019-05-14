package org.janelia.messaging.broker;

import picocli.CommandLine;

public class BrokerAdapterArgs {
    private static final long DEFAULT_BACKUP_INTERVAL_IN_MILLIS = 86400000L;

    @CommandLine.Option(names = {"-rec"}, description = "Receiving queue", required = true)
    String receiveQueue;
    @CommandLine.Option(names = {"-send"}, description = "Response queue to reply success")
    String replySuccessExchange;
    @CommandLine.Option(names = {"-error"}, description = "Response queue to reply error")
    String replyErrorExchange;
    @CommandLine.Option(names = {"-backupQueue"}, description = "Backup queue")
    String backupQueue;
    @CommandLine.Option(names = {"-backupInterval"}, description = "Interval between two consecutive backups in milliseconds")
    Long backupIntervalInMillis = DEFAULT_BACKUP_INTERVAL_IN_MILLIS;
    @CommandLine.Option(names = {"-backupLocation"}, description = "Backup location")
    String backupLocation;
    @CommandLine.Option(names = "-h", description = "Display help", usageHelp = true)
    boolean displayUsage = false;
}

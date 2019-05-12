package org.janelia.messaging.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.messaging.core.impl.ConnectionManager;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.impl.BulkMessageConsumerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class QueueBackupTool {
    private static final Logger LOG = LoggerFactory.getLogger(QueueBackupTool.class);
    private static final int DEFAULT_CONNECT_RETRIES = 1;

    @CommandLine.Option(names = {"-ms"}, description = "Messaging server", required = true)
    String messagingServer;
    @CommandLine.Option(names = {"-u"}, description = "Messaging user")
    String messagingUser;
    @CommandLine.Option(names = {"-p"}, description = "Messaging password")
    String messagingPassword;
    @CommandLine.Option(names = {"-filter"}, description = "Message filter")
    String filter;
    @CommandLine.Option(names = {"-queueName"}, description = "Name of the queue to download")
    String queueName;
    @CommandLine.Option(names = {"-backupLocation"}, description = "Backup location")
    String backupLocation;
    @CommandLine.Option(names = "-h", description = "Display help", usageHelp = true)
    boolean displayUsage = false;
    int connectRetries = DEFAULT_CONNECT_RETRIES;

    private QueueBackupTool() {
    }

    private boolean parseArgs(String[] args) {
        CommandLine cmdlineParser = new CommandLine(this);
        cmdlineParser.parse(args);
        if (cmdlineParser.isUsageHelpRequested()) {
            cmdlineParser.usage(System.out);
            return false;
        } else {
            return true;
        }
    }

    private void backupQueue() {
        try {
            Path backupLocationPath = Paths.get(backupLocation);
            if (Files.notExists(backupLocationPath) && backupLocationPath.getParent() != null) {
                Files.createDirectories(backupLocationPath.getParent());
            }
            ConnectionManager connManager = new ConnectionManager(0);

            BulkMessageConsumerImpl messageConsumer = new BulkMessageConsumerImpl(connManager);
            messageConsumer.setAutoAck(false);
            messageConsumer.connect(messagingServer, messagingUser, messagingPassword, queueName, queueName, connectRetries);

            List<GenericMessage> messageList = messageConsumer.retrieveMessages(null)
                    .collect(Collectors.toList());
            LOG.info("Retrieved {} messages to backup at {}", messageList.size(), backupLocation);

            ObjectMapper mapper = new ObjectMapper();
            try (OutputStream backupStream = new FileOutputStream(backupLocation)) {
                mapper.writeValue(backupStream, messageList);
                LOG.info("Finished scheduled backup at {} after backing up {} messages", new Date(), messageList.size());
            }
        } catch (Exception e) {
            LOG.error("Error while backing up queue {} to {}", queueName, backupLocation, e);
        }
    }

    public static void main(String[] args) {
        QueueBackupTool queueBackupToolTool = new QueueBackupTool();
        if (queueBackupToolTool.parseArgs(args)) {
            queueBackupToolTool.backupQueue();
        }
    }

}

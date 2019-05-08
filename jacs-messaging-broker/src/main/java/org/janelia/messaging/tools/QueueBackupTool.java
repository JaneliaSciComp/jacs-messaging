package org.janelia.messaging.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.messaging.core.ConnectionManager;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.BulkMessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Parameter(names = {"-ms"}, description = "Messaging server", required = true)
    String messagingServer;
    @Parameter(names = {"-u"}, description = "Messaging user")
    String messagingUser;
    @Parameter(names = {"-p"}, description = "Messaging password")
    String messagingPassword;
    @Parameter(names = {"-filter"}, description = "Message filter")
    String filter;
    @Parameter(names = {"-queueName"}, description = "Name of the queue to download")
    String queueName;
    @Parameter(names = {"-backupLocation"}, description = "Backup location")
    String backupLocation;
    @Parameter(names = "-h", description = "Display help", arity = 0)
    boolean displayUsage = false;
    int connectRetries = DEFAULT_CONNECT_RETRIES;

    private QueueBackupTool() {
    }

    private boolean parseArgs(String[] args) {
        JCommander cmdlineParser = new JCommander(this);
        cmdlineParser.parse(args);
        if (displayUsage) {
            cmdlineParser.usage(new StringBuilder());
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
            ConnectionManager connManager = new ConnectionManager(
                    messagingServer,
                    messagingUser,
                    messagingPassword,
                    0);

            BulkMessageConsumer messageConsumer = new BulkMessageConsumer(connManager);
            messageConsumer.setAutoAck(false);
            messageConsumer.connect(queueName, queueName, connectRetries);

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

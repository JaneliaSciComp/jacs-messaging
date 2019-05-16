package org.janelia.messaging.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.janelia.messaging.core.GenericMessage;
import org.janelia.messaging.core.impl.BulkMessageConsumerImpl;
import org.janelia.messaging.core.impl.MessageConnectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    @Parameter(names = {"-queueName"}, description = "Name of the queue to download")
    String queueName;
    @Parameter(names = {"-backupLocation"}, description = "Backup location")
    String backupLocation;
    @Parameter(names = "-h", description = "Display help")
    boolean usageRequested = false;

    private QueueBackupTool() {
    }

    private boolean parseArgs(String[] args) {
        JCommander cmdlineParser = new JCommander(this);
        cmdlineParser.parse(args);
        if (usageRequested) {
            cmdlineParser.usage();
            return false;
        } else {
            return true;
        }
    }

    private void backupQueue() {
        try {
            if (StringUtils.isNotBlank(backupLocation)) {
                Path backupLocationPath = Paths.get(backupLocation);
                if (Files.notExists(backupLocationPath) && backupLocationPath.getParent() != null) {
                    Files.createDirectories(backupLocationPath.getParent());
                }
            }
            MessageConnectionImpl messageConnection = new MessageConnectionImpl();
            messageConnection.openConnection(messagingServer, messagingUser, messagingPassword, 0);

            BulkMessageConsumerImpl messageConsumer = new BulkMessageConsumerImpl(messageConnection);
            messageConsumer.setAutoAck(false);
            messageConsumer.connectTo(queueName);

            List<GenericMessage> messageList = messageConsumer.retrieveMessages().collect(Collectors.toList());
            LOG.info("Retrieved {} messages to backup at {}", messageList.size(), backupLocation);

            ObjectMapper mapper = new ObjectMapper();
            OutputStream backupStream;
            if (StringUtils.isNotBlank(backupLocation)) {
                backupStream = new FileOutputStream(backupLocation);
            } else {
                backupStream = System.out;
            }
            try {
                mapper.writeValue(backupStream, messageList);
                LOG.info("Finished retrieving {} from queue {} ", messageList.size(), queueName);
            } finally {
                if (StringUtils.isNotBlank(backupLocation)) {
                    backupStream.close();
                }
            }
            messageConnection.closeConnection();
        } catch (Exception e) {
            LOG.error("Error while backing up queue {} to {}", queueName, backupLocation, e);
        }
    }

    public static void main(String[] args) {
        QueueBackupTool queueBackupToolTool = new QueueBackupTool();
        if (queueBackupToolTool.parseArgs(args)) {
            queueBackupToolTool.backupQueue();
        }
        System.exit(0);
    }

}

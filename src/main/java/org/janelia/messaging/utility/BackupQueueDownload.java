package org.janelia.messaging.utility;

import org.apache.commons.cli.*;
import org.janelia.messaging.client.BulkConsumer;
import org.janelia.messaging.client.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by schauderd on 3/8/18.
 * Class used to download all data from a queue and extract messages for a user
 */
public class BackupQueueDownload {

    private static final Logger log = LoggerFactory.getLogger(BackupQueueDownload.class);
    String messageServer;
    String backupQueue;
    String username;
    String password;
    String filter;
    BulkConsumer backupConsumer;
    File backupLocation;

    public BackupQueueDownload() {
    }


    public boolean parseArgs(String[] args) {
        // read off message server host and exchange
        Options options = new Options();
        options.addOption("ms", true, "Message Server Host");
        options.addOption("u", true, "Username");
        options.addOption("p", true, "Password");
        options.addOption("filter", true, "User to filter on");
        options.addOption("backupQueue", true, "Queue to process.");
        options.addOption("backupLocation", true, "Location(directory) to write metadata to.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
            messageServer = cmd.getOptionValue("ms");
            backupQueue = cmd.getOptionValue("backupQueue");
            String backupLocParam = cmd.getOptionValue("backupLocation");
            username = cmd.getOptionValue("u");
            password = cmd.getOptionValue("p");
            filter = cmd.getOptionValue("filter");

            // backup stuff
            if (backupLocParam != null) {
                if (!Files.exists(Paths.get(backupLocParam))) {
                    Files.createFile(Paths.get(backupLocParam));
                }
                backupLocation = new File(backupLocParam);
            }
            if (messageServer == null || backupQueue == null || backupLocation == null
                    || username == null || password == null)
                return help(options);
        } catch (ParseException e) {
            System.out.println("Error trying to parse command-line arguments");
            return help(options);
        } catch (IOException e) {
            System.out.println("Error trying to setup backup file");
            return help(options);
        }
        return true;
    }

    public static void main(String args[]) {
        BackupQueueDownload mp = new BackupQueueDownload();
        if (mp.parseArgs(args)) {
            mp.processQueue();
        }
    }

    private boolean help(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("BackupQueueDownload", options);
        return false;
    }

    public void processQueue() {
        try {
            // set up permanent receiver on queue
            ConnectionManager connManager = ConnectionManager.getInstance();
            connManager.configureTarget(messageServer, username, password);

            log.info ("starting processing queue to {}", new Date(), backupLocation);
            backupConsumer = new BulkConsumer();
            backupConsumer.init(connManager, backupQueue);
            backupConsumer.setPurgeOnCopy(false);
            int msgCount = backupConsumer.copyMessagesForUser(new FileOutputStream(backupLocation),
                    filter);
            log.info("finished processing queue backup after processing {} messages", new Date(), msgCount);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem with processing queue, {}", e.getMessage());
        }
    }
}
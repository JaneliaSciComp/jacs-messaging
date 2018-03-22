package org.janelia.messaging.utility;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;
import javassist.bytecode.ByteArray;
import org.apache.commons.cli.*;
import org.janelia.messaging.broker.sharedworkspace.HeaderConstants;
import org.janelia.messaging.utility.swc.MatrixDrivenSWCExchanger;
import org.janelia.messaging.utility.swc.SWCData;
import org.janelia.messaging.utility.swc.SWCDataConverter;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmProtobufExchanger;

import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Created by schauderd on 3/8/18.
 * Class used to download all data from a queue and extract useful metadata
 */
public class NeuronRecoveryUtility {
    Long workspace;
    String neuronName;
    String backupFile;
    String persistenceServer;
    enum Action {LIST, LATEST, TIMESTAMP};
    Action action;

    public NeuronRecoveryUtility() {
    }

    public boolean parseArgs(String[] args) {
        // read off message server host and exchange
        Options options = new Options();
        options.addOption("backupFile", true, "Backup File");
        options.addOption("persistenceServer", true, "Persistence Server");
        options.addOption("workspace", true, "Workspace Id");
        options.addOption("neuron", true, "Neuron Name");
        options.addOption("action", true, "Action to take with neuron history");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
            workspace = Long.parseLong(cmd.getOptionValue("workspace"));
            neuronName = cmd.getOptionValue("neuron");
            backupFile = cmd.getOptionValue("backupFile");
            persistenceServer = cmd.getOptionValue("persistenceServer");
            action = Action.valueOf(cmd.getOptionValue("action"));

            if (workspace == null || neuronName == null || action == null
                    || backupFile == null || persistenceServer==null)
                return help(options);
        } catch (ParseException e) {
            System.out.println("Error trying to parse command-line arguments");
            return help(options);
        }
        return true;
    }

    public void recoverNeuron() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createParser(new File(backupFile));

            JsonToken token = parser.nextToken();
            if (token == null) {
                // return or throw exception
            }

            List<TmNeuronMetadata> neuronList = new ArrayList<>();
            List<byte[]> protoList = new ArrayList<>();
            TmProtobufExchanger exchanger = new TmProtobufExchanger();
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                Map message = mapper.readValue(parser, Map.class);
                Map headers = (Map) message.get("headers");
                if (headers!=null && headers.get("workspace")!=null &&
                        ((Long)headers.get("workspace")).longValue()==workspace) {

                    String metadata = (String)headers.get(HeaderConstants.METADATA);
                    TmNeuronMetadata neuron = mapper.readValue(metadata, TmNeuronMetadata.class);
                    byte[] msgBody = Base64.getDecoder().decode((String) message.get("body"));
                    protoList.add(msgBody);
                    neuronList.add(neuron);
                }
            }

           if (action==Action.LATEST) {
               TmNeuronMetadata neuron = neuronList.get(neuronList.size()-1);
               byte[] protoData = protoList.get(neuronList.size()-1);
               exchanger.deserializeNeuron(new ByteArrayInputStream(protoData), neuron);
               SWCDataConverter converter = new SWCDataConverter();
               MatrixDrivenSWCExchanger matrixCalcs = new MatrixDrivenSWCExchanger(workspace);
               matrixCalcs.init(persistenceServer, neuron.getOwnerKey());
               converter.setSWCExchanger(matrixCalcs);
               SWCData swcData = converter.fromTmNeuron(neuron);
               swcData.write(new File("/Users/schauderd/recovery/output"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static byte[] getBytes(Object obj) throws java.io.IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        oos.close();
        bos.close();
        byte [] data = bos.toByteArray();
        return data;
    }

    private String convertLongString (LongString data) {
        return LongStringHelper.asLongString(data.getBytes()).toString();
    }

    private boolean help(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("NeuronRecoveryUtility", options);
        return false;
    }

    public static void main(String args[]) {
        NeuronRecoveryUtility nru = new NeuronRecoveryUtility();
        if (nru.parseArgs(args)) {
            nru.recoverNeuron();
            System.exit(1);
        }

    }
}
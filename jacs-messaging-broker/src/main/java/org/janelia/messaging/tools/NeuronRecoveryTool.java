package org.janelia.messaging.tools;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.messaging.broker.neuronadapter.NeuronMessageHeaders;
import org.janelia.messaging.tools.swc.MatrixDrivenSWCExchanger;
import org.janelia.messaging.tools.swc.SWCData;
import org.janelia.messaging.tools.swc.SWCDataConverter;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmProtobufExchanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class NeuronRecoveryTool {
    private static final Logger LOG = LoggerFactory.getLogger(NeuronRecoveryTool.class);

    enum Action {
        LIST, LATEST, TIMESTAMP
    }

    @CommandLine.Option(names = {"-backupFile"}, description = "Backup file", required = true)
    String backupFile;
    @CommandLine.Option(names = {"-swcLocation"}, description = "SWC file location")
    String swcLocation;
    @CommandLine.Option(names = {"-ps", "-persistenceServer"}, description = "Persistence server")
    String persistenceServer;
    @CommandLine.Option(names = {"-workspace"}, description = "Workspace ID", required = true)
    Long workspaceId;
    @CommandLine.Option(names = {"-neuron"}, description = "Neuron name", required = true)
    String neuronName;
    @CommandLine.Option(names = {"-action"}, description = "Action", required = true)
    Action action;
    @CommandLine.Option(names = "-h", description = "Display help", usageHelp = true)
    boolean displayUsage = false;

    private NeuronRecoveryTool() {
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

    private void recoverNeuron() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createParser(new File(backupFile));

            JsonToken token = parser.nextToken();
            if (token == null) {
                throw new IllegalArgumentException("Invalid backup file");
            }

            List<TmNeuronMetadata> neuronList = new ArrayList<>();
            List<byte[]> protoList = new ArrayList<>();
            TmProtobufExchanger exchanger = new TmProtobufExchanger();
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                Map message = mapper.readValue(parser, Map.class);
                Map headers = (Map) message.get("headers");
                if (headers != null && headers.get("workspace") != null &&
                        ((Long) headers.get("workspace")).longValue() == workspaceId) {
                    String metadata = (String) headers.get(NeuronMessageHeaders.METADATA);
                    TmNeuronMetadata neuron = mapper.readValue(metadata, TmNeuronMetadata.class);
                    if (neuron.getName().equals(neuronName)) {
                        byte[] msgBody = Base64.getDecoder().decode((String) message.get("body"));
                        protoList.add(msgBody);
                        neuronList.add(neuron);
                    }
                }
            }

            if (action == Action.LATEST) {
                TmNeuronMetadata neuron = neuronList.get(neuronList.size() - 1);
                System.out.println(neuron.getName());
                byte[] protoData = protoList.get(neuronList.size() - 1);
                exchanger.deserializeNeuron(new ByteArrayInputStream(protoData), neuron);
                SWCDataConverter converter = new SWCDataConverter();
                MatrixDrivenSWCExchanger matrixCalcs = new MatrixDrivenSWCExchanger(workspaceId);
                matrixCalcs.init(persistenceServer, neuron.getOwnerKey());
                converter.setSWCExchanger(matrixCalcs);
                SWCData swcData = converter.fromTmNeuron(neuron);
                swcData.write(new File(swcLocation));
            }

        } catch (Exception e) {
            LOG.error("Error running neuron recovery", e);
        }
    }

    public static void main(String[] args) {
        NeuronRecoveryTool queueBackupToolTool = new NeuronRecoveryTool();
        if (queueBackupToolTool.parseArgs(args)) {
            queueBackupToolTool.recoverNeuron();
        }
    }

}

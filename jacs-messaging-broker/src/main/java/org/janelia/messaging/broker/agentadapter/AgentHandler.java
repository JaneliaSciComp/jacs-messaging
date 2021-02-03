package org.janelia.messaging.broker.agentadapter;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import ch.qos.logback.core.encoder.EchoEncoder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.janelia.messaging.broker.neuronadapter.NeuronMessageHeaders;
import org.janelia.messaging.broker.neuronadapter.NeuronMessageType;
import org.janelia.messaging.core.MessageHandler;
import org.janelia.messaging.tools.swc.MatrixDrivenSWCExchanger;
import org.janelia.messaging.tools.swc.SWCDataConverter;
import org.janelia.messaging.utils.MessagingUtils;
import org.janelia.model.access.domain.TimebasedIdentifierGenerator;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.tiledMicroscope.TmAgentMetadata;
import org.janelia.model.domain.tiledMicroscope.TmGeoAnnotation;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AgentHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AgentHandler.class);

    private AgentDomainMgr agentDomainMgr;
    private final HandlerCallback successCallback;
    private final HandlerCallback errorCallback;
    private final HandlerCallback forwardCallback;
    private final ObjectMapper objectMapper;
    // for now hard code this but extend it for multiple agents
    private final String agentSubject = "group:mouselight";
    private final TimebasedIdentifierGenerator generator =
            new TimebasedIdentifierGenerator(0);


    AgentHandler(AgentDomainMgr domainMgr,
                 HandlerCallback successCallback,
                 HandlerCallback errorCallback,
                 HandlerCallback forwardCallback) {
        this.successCallback = successCallback;
        this.errorCallback = errorCallback;
        this.forwardCallback = forwardCallback;
        this.agentDomainMgr = domainMgr;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handleMessage(Map<String, Object> messageHeaders, byte[] messageBody) {
        if (messageHeaders == null) {
            return; // won't be able to figure out what to do anyway
        }
        LOG.info("AGENT REQUEST {}", MessagingUtils.getHeaderAsString(messageHeaders, AgentMessageHeaders.TYPE));
        AgentMessageType action = AgentMessageType.valueOf(MessagingUtils.getHeaderAsString(messageHeaders, AgentMessageHeaders.TYPE));

        try {
            switch (action) {
                case CREATE_WORKSPACE:
                    Map<String,Object> agentRequest = extractJsonPayload(messageHeaders, messageBody);
                    String messageId = MessagingUtils.getHeaderAsString(messageHeaders, NeuronMessageHeaders.USER);

                    handleWorkspaceCreate(messageHeaders, agentRequest,
                            (payload) -> {
                                try {
                                    fireSuccessMessage(AgentMessageType.WORKSPACE,
                                            "", objectMapper.writeValueAsBytes(payload));
                                } catch (Exception e) {
                                    throw new IllegalStateException(e);
                                }
                            });
                    break;
                case INIT:
                    agentRequest = extractJsonPayload(messageHeaders, messageBody);
                    handlePredictionsCreate(messageHeaders, agentRequest);
                    break;
                case PROCESS_DIFF:
                    TmNeuronMetadata newNeuron = extractNeuron(messageHeaders, messageBody);
                    if (newNeuron==null || newNeuron.getId()==null)
                       // fire error message
                        return;
                    TmNeuronMetadata savedNeuron = agentDomainMgr.retrieve(newNeuron.getWorkspaceId().toString(),
                            newNeuron.getId().toString(), agentSubject);
                    if (savedNeuron!=null)
                        return;
                    break;
            }
        } catch (Exception e) {
            LOG.error("Runtime exception processing agent payload {}", messageHeaders, e);
            fireErrorMessage(messageHeaders,
                    "Runtime Exception handling processing agent payload");
        }

    }

    @Override
    public void cancelMessage(String routingTag) {

    }

    private TmNeuronMetadata extractNeuron(Map<String, Object> msgHeaders, byte[] messageBody) {
        String neuronIds = MessagingUtils.getHeaderAsString(msgHeaders, NeuronMessageHeaders.NEURONIDS);
        try {
            if (messageBody.length>0) {
                return objectMapper.readValue(messageBody, TmNeuronMetadata.class);
            } else {
                LOG.info("No neuron data present in the message body");
                return null;
            }
        } catch (Exception e) {
            LOG.error("Error unmarshalling neuron data from {}", neuronIds, e);
            fireErrorMessage(msgHeaders, "Problems unmarshalling neuron data from " + msgHeaders.get(NeuronMessageHeaders.NEURONIDS) + " -> " + e.getMessage());
        }
        return null;
    }

    private Map<String,Object> extractJsonPayload (Map<String, Object> messageHeaders,
                                                   byte[] messageBody) {
        try {
            TypeReference<HashMap<String, Object>> typeRef
                    = new TypeReference<HashMap<String, Object>>() {};
            return objectMapper.readValue(messageBody, typeRef);

        } catch (Exception e) {
            LOG.error("Error unmarshalling agent payload {}", messageHeaders, e);
            fireErrorMessage(null, "Problems unmarshalling agent payload " +
                    messageHeaders.get(NeuronMessageHeaders.TYPE) + " -> " + e.getMessage());
        }
        return null;
    }

    private void handleDiff () {

    }

    private void handlePredictionsCreate (Map<String, Object> msgHeaders,
                                        Map<String, Object> payload) {
        // process list of nodes into a map first
        // pick random node and trace all connected components
        // repeat until no more nodes left
        try {
            String messageIdString = (String)payload.get("message_id");
            String workspaceIdStr = (String)payload.get("workspace_id");
            Long workspaceId = Long.parseLong(workspaceIdStr);
            List<String> nodeIds = (List<String>)payload.get("nodes");
            if (nodeIds == null || workspaceId == null|| messageIdString == null) {
                fireErrorMessage(msgHeaders,
                        "required items missing for init predictions");
            } else {
                Map<String, String> idMappings = new HashMap<>();

                // create a multimap to store all the child branches
                Multimap<String, String> edges = ArrayListMultimap.create();
                List<HashMap<String,String>> connections = (List<HashMap<String,String>>)payload.get("edges");

                for (HashMap<String,String> connection: connections) {
                    String parent = connection.keySet().iterator().next();
                    String child = connection.get(parent);
                    edges.put(parent, child);
                }

                // link the nodeIds to locations to speed up reconstruction
                List<Float[]> locations = ( List<Float[]>)payload.get("locations");
                Map<String,Object> nodeLocMap = new HashMap<>();
                for (int i=0; i<nodeIds.size(); i++) {
                    String nodeId = nodeIds.get(i);
                    nodeLocMap.put(nodeId, locations.get(i));
                }

                // find the root nodes by iterating through the edges looking for a connection
                // when a key isn't in the pool of values, it is a parent
                Collection<String> edgeValues = edges.values();
                Set<String> edgeKeys = edges.keySet();
                List<String> rootNodes = new ArrayList<>();
                for (String key: edgeKeys) {
                    if (!edgeValues.contains(key)) {
                        rootNodes.add(key);
                    }
                }

                // now we have the roots, iterate through the roots to build up
                // the neurons
                SWCDataConverter converter = new SWCDataConverter();
                MatrixDrivenSWCExchanger matrixCalcs = new MatrixDrivenSWCExchanger(workspaceId);
                matrixCalcs.init(agentDomainMgr, agentSubject);
                converter.setSWCExchanger(matrixCalcs);

                int neuroncount = 1;

                for (String root: rootNodes) {
                    Map<Integer, TmGeoAnnotation> annotations = new HashMap<>();

                    TmNeuronMetadata neuron = new TmNeuronMetadata();
                    neuron.setWorkspaceRef(Reference.createFor(TmWorkspace.class, workspaceId));
                    neuron.setName("Neuron" + neuroncount++);

                    neuron = agentDomainMgr.createNeuron(neuron, agentSubject);
                    neuron.setOwnerKey(agentSubject);
                    // start at the root and recursively generate annotations
                    if (neuron!=null && neuron.getId()!=null) {
                        addChildNodes(root, neuron.getId(), nodeLocMap, edges,
                                idMappings, converter, neuron);
                    }

                    fireNeuronForwardMessage(neuron);
                }

                // save the mappings so we can send the correct ids back for neuron updates
                TmAgentMetadata agentMetadata = new TmAgentMetadata();
                agentMetadata.setAgentIdMapping(idMappings);
                agentDomainMgr.createAgentMetadataForWorkspace(workspaceId, agentMetadata, agentSubject);
            }
        } catch (Exception e) {
            LOG.error("Error processing workspace create {}", msgHeaders, e);
            fireErrorMessage(msgHeaders, "Problems processing ownership request: " + e.getMessage());
        }
    }

    private TmGeoAnnotation addChildNodes(String id, Long parentId, Map<String, Object> nodeLocMap,
                               Multimap<String, String> edges, Map<String,String> idMappings,
                               SWCDataConverter converter,
                               TmNeuronMetadata neuron) {
        List loc = (List)nodeLocMap.get(id);
        Long internalId = generator.generateId();
        idMappings.put(internalId.toString(), id);
        double[] internalPoint = converter.internalFromExternal(
                new double[]{(double)loc.get(0), (double)loc.get(1), (double)loc.get(2)});
        Date now = new Date();
        // generate new id and store mapping
        TmGeoAnnotation newAnnotation = new TmGeoAnnotation(
                internalId, parentId, neuron.getId(),
                internalPoint[0], internalPoint[1], internalPoint[2], 1.0,
                now, now
        );
        neuron.addGeometricAnnotation(newAnnotation);
        for (String childId: edges.get(id)) {
            TmGeoAnnotation childAnn = addChildNodes(childId, internalId, nodeLocMap,
                    edges, idMappings, converter, neuron);
            if (childAnn!=null) {
                newAnnotation.addChild(childAnn);
            }
        }
        return newAnnotation;
    }

    private void handleWorkspaceCreate (Map<String, Object> msgHeaders,
                                        Map<String, Object> payload,
                                        Consumer<Map<String,Object>> processor) {
        try {
            String sampleIdString = (String)payload.get("sample");
            String workspaceName = (String)payload.get("name");
            if (sampleIdString == null || workspaceName == null) {
                fireErrorMessage(msgHeaders,
                        "required items missing for workspace creation");
            } else {
                Long sampleId = Long.parseLong(sampleIdString);
                TmWorkspace workspace = agentDomainMgr.createWorkspaceForSample(sampleId,
                        workspaceName, agentSubject);
                if (workspace!=null) {
                    msgHeaders.put(AgentMessageHeaders.TYPE, AgentMessageType.WORKSPACE.toString());
                    Map<String, Object> payloadBody = new HashMap<String, Object>();
                    payloadBody.put("workspace_id", workspace.getId().toString());
                    processor.accept(payloadBody);
                }
            }
        } catch (Exception e) {
            LOG.error("Error processing workspace create {}", msgHeaders, e);
            fireErrorMessage(msgHeaders, "Problems processing ownership request: " + e.getMessage());
        }
    }

    private void handleQueueAdd() {

    }

    private void handleQueueRemove() {

    }

    private void fireSuccessMessage (AgentMessageType type, String message_id,
                                    byte[] payload) {
        Map<String, Object> msgHeaders = new HashMap<>();
        msgHeaders.put(AgentMessageHeaders.TYPE, type.toString());
        msgHeaders.put(AgentMessageHeaders.MESSAGE_ID, message_id);
        successCallback.callback(msgHeaders, payload);
    }

    private void fireNeuronForwardMessage(TmNeuronMetadata neuron) throws Exception {
        List<Long> neuronIds = new ArrayList<Long>();
        neuronIds.add(neuron.getId());
        Map<String, Object> updateHeaders = new HashMap<String, Object>();
        updateHeaders.put(NeuronMessageHeaders.TYPE, NeuronMessageType.NEURON_SAVE_NEURONDATA.toString());
        updateHeaders.put(NeuronMessageHeaders.USER, agentSubject);
        updateHeaders.put(NeuronMessageHeaders.WORKSPACE, neuron.getWorkspaceId().toString());
        updateHeaders.put(NeuronMessageHeaders.NEURONIDS, neuronIds.toString());
        updateHeaders.put(NeuronMessageHeaders.OPERATION, "AGENT_CREATION");
        updateHeaders.put(NeuronMessageHeaders.TIMESTAMP, new Date());
        forwardCallback.callback(updateHeaders, objectMapper.writeValueAsBytes(neuron));
    }

    private void fireErrorMessage(Map<String, Object> msgHeaders, String errorMessage) {
        Map<String, Object> errorNotifHeaders = newMessageHeaders(msgHeaders, ImmutableMap.of(AgentMessageHeaders.TYPE,
                AgentMessageType.ERROR.name()));
        errorCallback.callback(errorNotifHeaders, errorMessage.getBytes());
    }

    private Map<String, Object> newMessageHeaders(Map<String, Object> msgHeaders, Map<String, Object> overridenHeaders) {
        Map<String, Object> newHeaders = msgHeaders.entrySet().stream()
                .filter(e -> !overridenHeaders.containsKey(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        overridenHeaders.forEach((k, v) -> newHeaders.put(k, v));
        return newHeaders;
    }
}

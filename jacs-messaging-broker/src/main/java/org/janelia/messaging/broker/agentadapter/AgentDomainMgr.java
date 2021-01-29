package org.janelia.messaging.broker.agentadapter;

import org.janelia.messaging.tools.persistence.DomainMgr;
import org.janelia.model.domain.tiledMicroscope.TmAgentMetadata;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Singleton for managing the Tiled Microscope Domain Model and related data access.
 */
public class AgentDomainMgr implements DomainMgr {
    private static final Logger log = LoggerFactory.getLogger(AgentDomainMgr.class);
    private final AgentTrackingClient client;

    public AgentDomainMgr(String remoteUrl, String apiKey) {
        client = new AgentTrackingClient(remoteUrl, apiKey);
    }

    public TmWorkspace createWorkspaceForSample(Long sampleId, String workspaceName, String subjectKey) throws Exception {
        return client.createWorkspaceForSample(sampleId, workspaceName,subjectKey);
    }

    public TmAgentMetadata getAgentMapping(Long workspaceId, String subjectKey) throws Exception {
        return client.getAgentMetadata(workspaceId,subjectKey);
    }

    public TmAgentMetadata createAgentMetadataForWorkspace(Long workspaceId, TmAgentMetadata metadata,
                                                           String subjectKey) throws Exception {
        return client.createAgentMapping(workspaceId, metadata,subjectKey);
    }

    TmNeuronMetadata createNeuron(TmNeuronMetadata neuronMetadata, String subjectKey) {
        log.debug("create({})", neuronMetadata);
        return client.create(neuronMetadata, subjectKey);
    }

    void updateNeuron(TmNeuronMetadata neuronMetadata, String subjectKey) {
        client.update(neuronMetadata, subjectKey);
    }

    public TmSample getSampleByWorkspaceId(Long workspaceId, String subjectKey) throws Exception {
        return client.getSampleForWorkspace(workspaceId, subjectKey);
    }

    public TmNeuronMetadata retrieve(String workspaceId, String neuronId, String subjectKey) {
        List<String> neuronIds = new ArrayList<String>();
        neuronIds.add(neuronId);
        log.debug("retrieve({})", neuronIds);
        List<TmNeuronMetadata> neuronMetadataList = client.getNeuronMetadata(workspaceId, neuronIds, subjectKey);
        if (neuronMetadataList != null && neuronMetadataList.size() == 1) {
            return neuronMetadataList.get(0);
        }
        return null;
    }
}

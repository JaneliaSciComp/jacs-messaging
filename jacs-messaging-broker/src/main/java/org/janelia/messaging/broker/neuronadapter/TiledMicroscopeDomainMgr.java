package org.janelia.messaging.broker.neuronadapter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmProtobufExchanger;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton for managing the Tiled Microscope Domain Model and related data access.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class TiledMicroscopeDomainMgr {
    private static final Logger log = LoggerFactory.getLogger(TiledMicroscopeDomainMgr.class);
    private final TiledMicroscopeRestClient client;

    public TiledMicroscopeDomainMgr(String remoteUrl, String apiKey) {
        client = new TiledMicroscopeRestClient(remoteUrl, apiKey);
    }

    public TmSample getSampleByWorkspaceId(Long workspaceId, String subjectKey) throws Exception {
        return client.getSampleForWorkspace(workspaceId, subjectKey);
    }

    public TmNeuronMetadata saveMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        log.debug("save({})", neuronMetadata);
        TmNeuronMetadata savedMetadata;
        if (neuronMetadata.getId() == null) {
            savedMetadata = client.create(neuronMetadata, subjectKey);
        } else {
            savedMetadata = client.update(neuronMetadata, subjectKey);
        }
        return savedMetadata;
    }

    public List<TmNeuronMetadata> saveMetadata(List<TmNeuronMetadata> neuronList, String subjectKey) throws Exception {
        log.debug("save({})", neuronList);
        for (TmNeuronMetadata tmNeuronMetadata : neuronList) {
            if (tmNeuronMetadata.getId() == null) {
                throw new IllegalArgumentException("Bulk neuron creation is currently unsupported");
            }
        }
        List<TmNeuronMetadata> updatedMetadata = client.updateNeurons(neuronList, subjectKey);
        return updatedMetadata;
    }

    public List<TmNeuronMetadata> retrieve(List<String> neuronIds, String subjectKey) throws Exception {
        log.debug("retrieve({})", neuronIds);
        List<TmNeuronMetadata> neuronMetadataList = client.getNeuronMetadata(neuronIds, subjectKey);

        return neuronMetadataList;
    }

    public TmNeuronMetadata retrieve(String neuronId, String subjectKey) throws Exception {
        List<String> neuronIds = new ArrayList<String>();
        neuronIds.add(neuronId);
        log.debug("retrieve({})", neuronIds);
        List<TmNeuronMetadata> neuronMetadataList = client.getNeuronMetadata(neuronIds, subjectKey);
        if (neuronMetadataList != null && neuronMetadataList.size() == 1) {
            return neuronMetadataList.get(0);
        }
        return null;
    }

    public TmWorkspace save(TmWorkspace workspace, String subjectKey) throws Exception {
        log.debug("save({})", workspace.getId());
        workspace = client.save(workspace, subjectKey);
        return workspace;
    }

    public void remove(TmWorkspace workspace, String subjectKey) throws Exception {
        log.debug("remove({})", workspace);
        client.remove(workspace, subjectKey);
    }

    public TmNeuronMetadata save(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        log.debug("save({})", neuronMetadata);
        TmNeuronMetadata savedMetadata;
        if (neuronMetadata.getId() == null) {
            savedMetadata = client.create(neuronMetadata, subjectKey);
        } else {
            savedMetadata = client.update(neuronMetadata, subjectKey);
        }
        return savedMetadata;
    }

    public void remove(TmNeuronMetadata tmNeuron, String subjectKey) throws Exception {
        log.debug("remove({})", tmNeuron);
        TmNeuronMetadata neuronMetadata = new TmNeuronMetadata();
        neuronMetadata.setId(tmNeuron.getId());
        client.remove(neuronMetadata, subjectKey);
    }

    public TmNeuronMetadata setPermissions(String subjectKey, TmNeuronMetadata neuron, String newOwner) throws Exception {
        return client.setPermissions(subjectKey, neuron, newOwner);
    }
}

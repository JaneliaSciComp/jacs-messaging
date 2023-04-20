package org.janelia.messaging.broker.neuronadapter;

import java.util.ArrayList;
import java.util.List;

import org.janelia.messaging.tools.persistence.DomainMgr;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton for managing the Tiled Microscope Domain Model and related data access.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class TiledMicroscopeDomainMgr implements DomainMgr {
    private static final Logger log = LoggerFactory.getLogger(TiledMicroscopeDomainMgr.class);
    private final TiledMicroscopeRestClient client;

    public TiledMicroscopeDomainMgr(String remoteUrl, String apiKey) {
        client = new TiledMicroscopeRestClient(remoteUrl, apiKey);
    }

    @Override
    public TmSample getSampleByWorkspaceId(Long workspaceId, String subjectKey) throws Exception {
        return client.getSampleForWorkspace(workspaceId, subjectKey);
    }

    public void saveOperationLog(Long workspaceId, Long neuronId,
                                      String operationType, String timestamp, String subjectKey ) {
        log.info("operationlog({},{},{},{},{},{})", subjectKey,timestamp,workspaceId, neuronId,operationType);
        client.createOperationLog(workspaceId,neuronId,
                operationType,timestamp,subjectKey);
    }

    TmNeuronMetadata saveMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) {
        log.debug("save({})", neuronMetadata);
        TmNeuronMetadata savedMetadata;
        if (neuronMetadata.getId() == null) {
            savedMetadata = client.create(neuronMetadata, subjectKey);
        } else {
            savedMetadata = client.update(neuronMetadata, subjectKey);
        }
        return savedMetadata;
    }

    public List<TmNeuronMetadata> retrieve(String workspaceId, List<String> neuronIds, String subjectKey) {
        log.info("retrieve({})", neuronIds);
        List<TmNeuronMetadata> neuronMetadataList = client.getNeuronMetadata(workspaceId, neuronIds, subjectKey);

        return neuronMetadataList;
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

    void remove(TmNeuronMetadata tmNeuron, String subjectKey) throws Exception {
        log.info("remove({})", tmNeuron);
        client.remove(tmNeuron, subjectKey);
    }

}

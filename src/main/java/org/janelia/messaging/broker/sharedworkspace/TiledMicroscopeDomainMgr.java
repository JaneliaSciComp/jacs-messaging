package org.janelia.messaging.broker.sharedworkspace;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmProtobufExchanger;
import org.janelia.model.domain.tiledMicroscope.TmSample;
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
    
    public TiledMicroscopeDomainMgr(String remoteUrl) {
        client = new TiledMicroscopeRestClient(remoteUrl);
    }

    public TmSample getSampleByWorkspaceId(Long workspaceId, String subjectKey) throws Exception {
        return client.getSampleForWorkspace(workspaceId, subjectKey);
    }

    public TmNeuronMetadata saveMetadata(TmNeuronMetadata neuronMetadata, String subjectKey) throws Exception {
        log.debug("save({})", neuronMetadata);
        TmNeuronMetadata savedMetadata;
        if (neuronMetadata.getId()==null) {
            savedMetadata = client.createMetadata(neuronMetadata, subjectKey);
        }
        else {
            savedMetadata = client.updateMetadata(neuronMetadata, subjectKey);
        }
        return savedMetadata;
    }

    public List<TmNeuronMetadata> saveMetadata(List<TmNeuronMetadata> neuronList, String subjectKey) throws Exception {
        log.debug("save({})", neuronList);
        for(TmNeuronMetadata tmNeuronMetadata : neuronList) {
            if (tmNeuronMetadata.getId()==null) {
                throw new IllegalArgumentException("Bulk neuron creation is currently unsupported");
            }
        }
        List<TmNeuronMetadata> updatedMetadata = client.updateMetadata(neuronList, subjectKey);
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
        if (neuronMetadataList!=null && neuronMetadataList.size()==1) {
            return neuronMetadataList.get(0);
        }
        return null;
    }
    
    public TmNeuronMetadata save(TmNeuronMetadata neuronMetadata, byte[] protoBytes, String subjectKey) throws Exception {
        log.debug("save({})", neuronMetadata);
        TmProtobufExchanger exchanger = new TmProtobufExchanger();
        InputStream protobufStream = new ByteArrayInputStream(protoBytes);
        TmNeuronMetadata savedMetadata;
        if (neuronMetadata.getId()==null) {
            savedMetadata = client.create(neuronMetadata, protobufStream, subjectKey);
        }
        else {
            savedMetadata = client.update(neuronMetadata, protobufStream, subjectKey);
        }
        // We assume that the neuron data was saved on the server, but it only returns metadata for efficiency. We
        // already have the data, so let's copy it over into the new object.
        exchanger.copyNeuronData(neuronMetadata, savedMetadata);
        return savedMetadata;
    }

    public void remove(TmNeuronMetadata tmNeuron, String subjectKey) throws Exception {
        log.debug("remove({})", tmNeuron);
        TmNeuronMetadata neuronMetadata = new TmNeuronMetadata();
        neuronMetadata.setId(tmNeuron.getId());
        client.remove(neuronMetadata, subjectKey);
    }
}

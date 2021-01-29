package org.janelia.messaging.tools.persistence;

import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmSample;

import java.util.ArrayList;
import java.util.List;

public interface DomainMgr {
    public TmSample getSampleByWorkspaceId(Long workspaceId, String subjectKey) throws Exception;
}

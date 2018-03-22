package org.janelia.messaging.utility.swc;

/**
 * Created by schauderd on 3/22/18.
 */
public interface ImportExportSWCExchanger {
    double[] getInternal( double[] external );
    double[] getExternal( double[] internal );
}


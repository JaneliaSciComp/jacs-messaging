package org.janelia.messaging.tools.swc;

/**
 * Created by schauderd on 3/22/18.
 */
public interface ImportExportSWCExchanger {
    double[] getInternal(double[] external);

    double[] getExternal(double[] internal);
}


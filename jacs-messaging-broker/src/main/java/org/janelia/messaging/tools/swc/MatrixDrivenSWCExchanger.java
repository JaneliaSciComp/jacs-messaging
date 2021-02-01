package org.janelia.messaging.tools.swc;

import java.io.File;

import javax.media.jai.RenderedImageAdapter;

import com.sun.media.jai.codec.FileSeekableStream;
import com.sun.media.jai.codec.ImageCodec;
import com.sun.media.jai.codec.ImageDecoder;
import com.sun.media.jai.codec.SeekableStream;

import Jama.Matrix;
import org.janelia.messaging.broker.neuronadapter.TiledMicroscopeDomainMgr;
import org.janelia.messaging.tools.geom.BoundingBox3d;
import org.janelia.messaging.tools.geom.Vec3;
import org.janelia.messaging.tools.persistence.DomainMgr;
import org.janelia.model.domain.tiledMicroscope.TmSample;

/**
 * Uses matrices (based on JAMA package), to convert between internal and
 * external SWC coordinate systems.
 *
 * @author fosterl
 */
public class MatrixDrivenSWCExchanger implements ImportExportSWCExchanger {
    private static final int EXPECTED_ARRAY_SIZE = 3;

    private Matrix micronToVoxMatrix;
    private Matrix voxToMicronMatrix;
    private Long workspaceId;
    private double[] scale;

    public MatrixDrivenSWCExchanger(Long workspaceId) {
        this.workspaceId = workspaceId;
    }

    public void init(String persistenceServer, String user) throws Exception {
        TiledMicroscopeDomainMgr domainMgr = new TiledMicroscopeDomainMgr(persistenceServer, null);
        init(domainMgr, user);
    }

    public void init(DomainMgr domainMgr, String user) throws Exception {
        // fetch the sample and extract the origin and scale attributes

        TmSample sample = domainMgr.getSampleByWorkspaceId(workspaceId, user);

        // if no origin or scale attributes, stop (old sample)
        if (sample == null || sample.getOrigin() == null || sample.getScaling() == null)
            return;

        // get origin and scale from Sample
        int[] origin = new int[3];
        for (int i = 0; i < 3; i++) {
            origin[i] = sample.getOrigin().get(i);
        }
        scale = new double[3];
        for (int i = 0; i < 3; i++) {
            scale[i] = sample.getScaling().get(i);
        }

        double divisor = Math.pow(2.0, sample.getNumImageryLevels().intValue() - 1);
        for (int i = 0; i < scale.length; i++) {
            scale[i] /= divisor; // nanometers to micrometers
        }

        for (int i = 0; i < scale.length; i++) {
            scale[i] /= 1000; // nanometers to micrometers
        }
        for (int i = 0; i < origin.length; i++) {
            origin[i] = (int) (origin[i] / (1000 * scale[i])); // nanometers to voxels
        }

        micronToVoxMatrix = MatrixUtilities.buildMicronToVox(scale, origin);
        voxToMicronMatrix = MatrixUtilities.buildVoxToMicron(scale, origin);
    }

    @Override
    public double[] getInternal(double[] external) {
        Matrix externalMatrix = inputMatrix(external);
        Matrix internalMatrix = micronToVoxMatrix.times(externalMatrix);
        return outputMatrix(internalMatrix);
    }

    @Override
    public double[] getExternal(double[] internal) {
        Matrix internalMatrix = inputMatrix(internal);
        Matrix externalMatrix = voxToMicronMatrix.times(internalMatrix);
        return outputMatrix(externalMatrix);
    }

    private Matrix inputMatrix(double[] input) {
        if (input.length != 3) {
            throw new IllegalArgumentException("Very specific matrix requirements.");
        }
        Matrix matrix = new Matrix(EXPECTED_ARRAY_SIZE + 1, 1);
        for (int i = 0; i < input.length; i++) {
            matrix.set(i, 0, input[i]);
        }
        matrix.set(EXPECTED_ARRAY_SIZE, 0, 1.0);
        return matrix;
    }

    private double[] outputMatrix(Matrix output) {
        double[] result = new double[EXPECTED_ARRAY_SIZE];
        for (int i = 0; i < result.length; i++) {
            result[i] = output.get(i, 0);
        }
        return result;
    }

}


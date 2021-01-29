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

    static BoundingBox3d boundingBox;

    private Matrix micronToVoxMatrix;
    private Matrix voxToMicronMatrix;
    private Long workspaceId;
    private double[] scale;

    public MatrixDrivenSWCExchanger(Long workspaceId) {
        this.workspaceId = workspaceId;
    }

    private int[] calculateVolumeSize(TmSample sample) throws Exception {
        // replace this if running on mac
        File topFolderParam = new File(sample.getLargeVolumeOctreeFilepath().replaceAll("nrs", "Volumes"));

        int octreeDepth = (int) sample.getNumImageryLevels().longValue();
        int zoomFactor = (int) Math.pow(2, octreeDepth - 1);

        // Deduce other parameters from first image file contents
        File tiff = new File(topFolderParam, "default.0.tif");
        SeekableStream s = new FileSeekableStream(tiff);
        ImageDecoder decoder = ImageCodec.createImageDecoder("tiff", s, null);
        // Z dimension is related to number of tiff pages
        int sz = decoder.getNumPages();

        // Get X/Y dimensions from first image
        RenderedImageAdapter ria = new RenderedImageAdapter(decoder.decodeAsRenderedImage(0));
        int sx = ria.getWidth();
        int sy = ria.getHeight();

        // Full volume could be much larger than this downsampled tile
        int[] volumeSize = new int[3];
        volumeSize[2] = zoomFactor * sz;
        volumeSize[0] = zoomFactor * sx;
        volumeSize[1] = zoomFactor * sy;
        return volumeSize;
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

        // calculate volumeSize
        int[] volumeSize = calculateVolumeSize(sample);
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

        // calculate bounding box using scale, origin, and volume
        Vec3 b0 = new Vec3(0, 0, 0);
        Vec3 b1 = new Vec3(volumeSize[0], volumeSize[1], volumeSize[2]);
        boundingBox = new BoundingBox3d();
        boundingBox.setMin(b0);
        boundingBox.setMax(b1);

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


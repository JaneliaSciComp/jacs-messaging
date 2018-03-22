package org.janelia.messaging.utility.swc;


import Jama.Matrix;

public class MatrixUtilities {
    public static final int EXPECTED_MATRIX_SIZE = 4;
    public static final String ROW_SEP = ";";
    public static final String COL_SEP = ",";
    public static final int X_OFFS = 0;
    public static final int Y_OFFS = 1;
    public static final int Z_OFFS = 2;

    public MatrixUtilities() {
    }

    public static String serializeMatrix(Matrix matrix, String matrixName) {
        StringBuilder rtnVal = new StringBuilder();
        if(matrix != null && matrix.getRowDimension() == 4 && matrix.getColumnDimension() == 4) {
            double[][] matrixArr = matrix.getArray();
            double[][] var4 = matrixArr;
            int var5 = matrixArr.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                double[] row = var4[var6];
                if(rtnVal.length() > 0) {
                    rtnVal.append(";");
                }

                int colCount = 0;
                double[] var9 = row;
                int var10 = row.length;

                for(int var11 = 0; var11 < var10; ++var11) {
                    double col = var9[var11];
                    if(colCount > 0) {
                        rtnVal.append(",");
                    }

                    rtnVal.append(col);
                    ++colCount;
                }
            }

            return rtnVal.toString();
        } else {
            System.err.println("Serialization of " + matrixName + " failed.  Unexpected dimensions.");
            return null;
        }
    }

    public static Matrix deserializeMatrix(String matrixString, String matrixName) {
        Matrix rtnVal = null;
        String[] rowMatrixStr = matrixString.split(";");
        boolean x = false;
        int y = 0;
        double[][] accumulator = new double[4][4];
        if(rowMatrixStr.length == 4) {
            String[] var7 = rowMatrixStr;
            int var8 = rowMatrixStr.length;

            for(int var9 = 0; var9 < var8; ++var9) {
                String row = var7[var9];
                String[] columnStr = row.split(",");
                int var19 = 0;

                try {
                    String[] nfe = columnStr;
                    int var13 = columnStr.length;

                    for(int var14 = 0; var14 < var13; ++var14) {
                        String column = nfe[var14];
                        double colDouble = Double.parseDouble(column);
                        accumulator[y][var19] = colDouble;
                        ++var19;
                    }
                } catch (NumberFormatException var18) {
                    System.err.println("Serialized value " + columnStr[var19] + " at position " + var19 + "," + y + " of matrix " + matrixName + "value {" + matrixString + "}, could not be deserialized.");
                }

                ++y;
            }

            rtnVal = new Matrix(accumulator);
        } else {
            System.err.println("Serialized matrix: " + matrixName + "value {" + matrixString + "}, could not be deserialized.");
        }

        return rtnVal;
    }

    public static String createSerializableMicronToVox(double[] voxelMicrometers, int[] origin) {
        Matrix micronToVoxMatrix = buildMicronToVox(voxelMicrometers, origin);
        String micronToVoxString = serializeMatrix(micronToVoxMatrix, "Micron to Voxel Matrix");
        return micronToVoxString;
    }

    public static Matrix buildMicronToVox(double[] voxelMicrometers, int[] origin) {
        double[][] micronToVoxArr = new double[][]{{1.0D / voxelMicrometers[0], 0.0D, 0.0D, (double)(-origin[0])}, {0.0D, 1.0D / voxelMicrometers[1], 0.0D, (double)(-origin[1])}, {0.0D, 0.0D, 1.0D / voxelMicrometers[2], (double)(-origin[2])}, {0.0D, 0.0D, 0.0D, 1.0D}};
        Matrix micronToVoxMatrix = new Matrix(micronToVoxArr);
        return micronToVoxMatrix;
    }

    public static String createSerializableVoxToMicron(double[] voxelMicrometers, int[] origin) {
        Matrix voxToMicronMatrix = buildVoxToMicron(voxelMicrometers, origin);
        String voxToMicronString = serializeMatrix(voxToMicronMatrix, "Voxel to Micron Matrix");
        return voxToMicronString;
    }

    public static Matrix buildVoxToMicron(double[] voxelMicrometers, int[] origin) {
        double[][] voxToMicronArr = new double[][]{{voxelMicrometers[0], 0.0D, 0.0D, (double)origin[0] * voxelMicrometers[0]}, {0.0D, voxelMicrometers[1], 0.0D, (double)origin[1] * voxelMicrometers[1]}, {0.0D, 0.0D, voxelMicrometers[2], (double)origin[2] * voxelMicrometers[2]}, {0.0D, 0.0D, 0.0D, 1.0D}};
        Matrix voxToMicronMatrix = new Matrix(voxToMicronArr);
        return voxToMicronMatrix;
    }
}

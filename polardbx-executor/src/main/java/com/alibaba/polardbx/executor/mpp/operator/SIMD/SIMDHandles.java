package com.alibaba.polardbx.executor.mpp.operator.SIMD;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorSpecies;

import static java.lang.Integer.numberOfTrailingZeros;

public class SIMDHandles {
    public static boolean activate(int batchSize) {
        return batchSize >= SIMDHandles.LONG_VECTOR_LANE_SIZE;
    }

    public static final VectorSpecies<Byte> BYTE_VECTOR_SPECIES = ByteVector.SPECIES_PREFERRED;
    public static final int BYTE_VECTOR_LANE_SIZE = BYTE_VECTOR_SPECIES.length();
    public static final int BYTE_VECTOR_LANE_SIZE_SHIFT = numberOfTrailingZeros(BYTE_VECTOR_LANE_SIZE);

    public static int roundOffByteVectorLaneSize(int size) {
        return (size >>> BYTE_VECTOR_LANE_SIZE_SHIFT) << BYTE_VECTOR_LANE_SIZE_SHIFT;
    }

    public static final VectorSpecies<Integer> INTEGER_VECTOR_SPECIES = IntVector.SPECIES_PREFERRED;
    public static final int INT_VECTOR_LANE_SIZE = INTEGER_VECTOR_SPECIES.length();
    public static final int INT_VECTOR_LANE_SIZE_SHIFT = numberOfTrailingZeros(INT_VECTOR_LANE_SIZE);

    /**
     * round off the size to the closet multiply of INT_VECTOR_LANE_SIZE
     * assert the parameter is 2 power based
     */
    public static int roundOffIntVectorLaneSize(int size) {
        return (size >>> INT_VECTOR_LANE_SIZE_SHIFT) << INT_VECTOR_LANE_SIZE_SHIFT;
    }

    public static final VectorSpecies<Long> LONG_VECTOR_SPECIES = LongVector.SPECIES_PREFERRED;
    public static final int LONG_VECTOR_LANE_SIZE = LONG_VECTOR_SPECIES.length();
    public static final int LONG_VECTOR_LANE_SIZE_SHIFT = numberOfTrailingZeros(LONG_VECTOR_LANE_SIZE);

    public static final VectorSpecies<Short> SHORT_VECTOR_SPECIES = ShortVector.SPECIES_PREFERRED;
    public static final int SHORT_VECTOR_LANE_SIZE = SHORT_VECTOR_SPECIES.length();
    public static final int SHORT_VECTOR_LANE_SIZE_SHIFT = numberOfTrailingZeros(SHORT_VECTOR_LANE_SIZE);

    /**
     * round off the size to the closet multiply of LONG_VECTOR_LANE_SIZE
     * assert the parameter is 2 power based
     */
    public static int roundOffLongVectorLaneSize(int size) {
        return (size >>> LONG_VECTOR_LANE_SIZE_SHIFT) << LONG_VECTOR_LANE_SIZE_SHIFT;
    }

    public static void main(String[] args) {
    }
}

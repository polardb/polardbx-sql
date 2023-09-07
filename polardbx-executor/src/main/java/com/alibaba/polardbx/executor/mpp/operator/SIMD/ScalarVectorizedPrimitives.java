package com.alibaba.polardbx.executor.mpp.operator.SIMD;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

class ScalarVectorizedPrimitives
    implements VectorizedPrimitives {
    @Override
    public void gather(byte[] source, int[] indexes, int start, int count, byte[] target, int targetOffset) {
        for (int i = 0; i < count; i++) {
            target[targetOffset + i] = source[indexes[start + i]];
        }
    }

    @Override
    public void gather(int[] source, int[] indexes, int start, int count, int[] target, int targetOffset) {
        for (int i = 0; i < count; i++) {
            target[targetOffset + i] = source[indexes[start + i]];
        }
    }

    @Override
    public void gather(long[] source, int[] indexes, int start, int count, long[] target, int targetOffset) {
        for (int i = 0; i < count; i++) {
            target[targetOffset + i] = source[indexes[start + i]];
        }
    }

    @Override
    public void scatter(long[] source, long[] target, int[] scatterMap, int offset, int copySize) {
        for (int i = 0; i < copySize; i++) {
            target[scatterMap[offset + i]] = source[offset + i];
        }
    }

    @Override
    public void scatter(int[] source, int[] target, int[] scatterMap, int offset, int copySize) {
        for (int i = 0; i < copySize; i++) {
            target[scatterMap[offset + i]] = source[offset + i];
        }
    }

    @Override
    public void scatter(short[] source, short[] target, int[] scatterMap, int offset, int copySize) {
        for (int i = 0; i < copySize; i++) {
            target[scatterMap[offset + i]] = source[offset + i];
        }
    }

    @Override
    public void scatter(byte[] source, byte[] target, int[] scatterMap, int offset, int copySize) {
        for (int i = 0; i < copySize; i++) {
            target[scatterMap[offset + i]] = source[offset + i];
        }
    }

    @Override
    public void scatter(boolean[] source, boolean[] target, int[] scatterMap, int offset, int copySize) {
        for (int i = 0; i < copySize; i++) {
            target[scatterMap[offset + i]] = source[offset + i];
        }
    }

    public static List<Pair<Integer, Integer>> partitionIndexInSegment(int[] indexes, int indexOffset, int indexCnt,
                                                                       int entrySize, int segmentSize, int segmentCnt) {
        List<Pair<Integer, Integer>> partitions = new ArrayList<>();

        final int posSizeLimitInSegment = segmentSize / entrySize;
        int indexPos = indexOffset;
        int indexStart = indexPos;
        int indexLimit = indexOffset + indexCnt;

        for (int i = 0; i < segmentCnt; i++) {
            // the index limit of the current segment
            int limit = posSizeLimitInSegment * (i + 1);

            // we assume that the indexes are  in the asc ordering
            while (indexPos < indexLimit) {
                // found the first index not belonging the current segment
                if (indexes[indexPos] >= limit) {
                    int count = indexPos - indexStart;
                    if (count > 0) {
                        partitions.add(Pair.of(indexStart, count));
                        // found the first index not belonging the current segment
                        indexStart = indexPos;
                    } // else there are no indexes of the following segment at all, break the loop and try next segment
                    break;
                }
                // try next index checking it if belongs to the current segment
                indexPos++;
            }

            // check if we reached the end of the array, and deal with the left over indexes
            if (indexPos == indexLimit) {
                int count = indexPos - indexStart;
                if (count > 0) {
                    partitions.add(Pair.of(indexStart, count));
                }
                break;
            }
        }
        return partitions;
    }

}

package com.alibaba.polardbx.executor.mpp.operator.SIMD;

import com.alibaba.polardbx.common.exception.NotSupportException;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorMask;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.executor.mpp.operator.SIMD.SIMDHandles.BYTE_VECTOR_SPECIES;
import static com.alibaba.polardbx.executor.mpp.operator.SIMD.SIMDHandles.INTEGER_VECTOR_SPECIES;
import static com.alibaba.polardbx.executor.mpp.operator.SIMD.SIMDHandles.LONG_VECTOR_SPECIES;
import static com.alibaba.polardbx.executor.mpp.operator.SIMD.SIMDHandles.SHORT_VECTOR_SPECIES;

public class SIMDVectorizedPrimitives
    implements VectorizedPrimitives {
    private final VectorizedPrimitives scalarPrimitives;

    SIMDVectorizedPrimitives(VectorizedPrimitives scalarPrimitives) {
        this.scalarPrimitives = scalarPrimitives;
    }

    @Override
    public void gather(byte[] source, int[] indexes, int start, int count, byte[] target, int targetOffset) {
        final int laneSize = BYTE_VECTOR_SPECIES.length();
        final int indexLimit = start + count;
        final int indexVectorLimit = indexLimit / laneSize * laneSize;
        int indexPos = start;
        int targetPos = targetOffset;
        for (; indexPos < indexVectorLimit; indexPos += laneSize, targetPos += laneSize) {
            ByteVector av = ByteVector.fromArray(BYTE_VECTOR_SPECIES, source, 0, indexes, indexPos);
            av.intoArray(target, targetPos);
        }
        if (indexPos < indexLimit) {
            scalarPrimitives.gather(source, indexes, indexPos, indexLimit - indexPos, target, targetPos);
        }
    }

    @Override
    public void gather(int[] source, int[] indexes, int start, int count, int[] target, int targetOffset) {
        final int laneSize = INTEGER_VECTOR_SPECIES.length();
        final int indexLimit = start + count;
        final int indexVectorLimit = indexLimit / laneSize * laneSize;
        int indexPos = start;
        int targetPos = targetOffset;
        for (; indexPos < indexVectorLimit; indexPos += laneSize, targetPos += laneSize) {
            IntVector av = IntVector.fromArray(INTEGER_VECTOR_SPECIES, source, 0, indexes, indexPos);
            av.intoArray(target, targetPos);
        }
        if (indexPos < indexLimit) {
            scalarPrimitives.gather(source, indexes, indexPos, indexLimit - indexPos, target, targetPos);
        }
    }

    @Override
    public void gather(long[] source, int[] indexes, int start, int count, long[] target, int targetOffset) {
        final int laneSize = LONG_VECTOR_SPECIES.length();
        final int indexLimit = start + count;
        final int indexVectorLimit = indexLimit / laneSize * laneSize;
        int indexPos = start;
        int targetPos = targetOffset;
        for (; indexPos < indexVectorLimit; indexPos += laneSize, targetPos += laneSize) {
            LongVector av = LongVector.fromArray(LONG_VECTOR_SPECIES, source, 0, indexes, indexPos);
            av.intoArray(target, targetPos);
        }
        if (indexPos < indexLimit) {
            scalarPrimitives.gather(source, indexes, indexPos, indexLimit - indexPos, target, targetPos);
        }
    }

    @Override
    public void scatter(long[] source, long[] target, int[] scatterMap, int offset, int copySize) {
        int laneSize = SIMDHandles.LONG_VECTOR_LANE_SIZE;
        int index = 0;
        for (; index <= copySize - laneSize; index += laneSize) {
            LongVector dataLongVector = LongVector.fromArray(LONG_VECTOR_SPECIES, source, index);
            dataLongVector.intoArray(target, 0, scatterMap, index);
        }

        if (index < copySize) {
            int left = copySize - index;
            scalarPrimitives.scatter(source, target, scatterMap, index, left);
        }
    }

    @Override
    public void scatter(int[] source, int[] target, int[] scatterMap, int offset, int copySize) {
        int laneSize = SIMDHandles.INT_VECTOR_LANE_SIZE; //每次SIMD能处理的位数
        int index = 0;
        for (; index <= copySize - laneSize; index += laneSize) {
            IntVector dataInVector =
                IntVector.fromArray(INTEGER_VECTOR_SPECIES, source, index); //从source[index]位置取出K个数字
            dataInVector.intoArray(target, 0, scatterMap, index); //将scatterMap[index + 0]开始的元素scatter到
        }
        if (index < copySize) {
            int left = copySize - index; //剩余的部分用scalar的方式实现
            scalarPrimitives.scatter(source, target, scatterMap, index, left);
        }
    }

    @Override
    public void scatter(short[] source, short[] target, int[] scatterMap, int offset, int copySize) {
        int laneSize = SIMDHandles.SHORT_VECTOR_LANE_SIZE;
        int index = 0;
        for (; index <= copySize - laneSize; index += laneSize) {
            ShortVector dataShortVector = ShortVector.fromArray(SHORT_VECTOR_SPECIES, source, index);
            dataShortVector.intoArray(target, 0, scatterMap, index);
        }

        if (index < copySize) {
            int left = copySize - index;
            scalarPrimitives.scatter(source, target, scatterMap, index, left);
        }
    }

    @Override
    public void scatter(byte[] source, byte[] target, int[] scatterMap, int offset, int copySize) {
        int nullLaneSize = SIMDHandles.BYTE_VECTOR_LANE_SIZE;
        int index = 0;
        for (; index <= copySize - nullLaneSize; index += nullLaneSize) {
            ByteVector dataByteVector = ByteVector.fromArray(BYTE_VECTOR_SPECIES, source, index);
            dataByteVector.intoArray(target, 0, scatterMap, index);
        }
        if (index < copySize) {
            int left = copySize - index;
            scalarPrimitives.scatter(source, target, scatterMap, index, left);
        }
    }

    @Override
    public void scatter(boolean[] source, boolean[] target, int[] scatterMap, int offset, int copySize) {
        throw new NotSupportException("scatter doesn't support boolean");
    }

    public static List<Pair<Integer, Integer>> partitionIndexInSegment(int[] indexes, int indexOffset, int indexCnt,
                                                                       int entrySize, int segmentSize, int segmentCnt) {
        List<Pair<Integer, Integer>> partitions = new ArrayList<>();

        final int posSizeLimitInSegment = segmentSize / entrySize;
        int indexPos = indexOffset;
        int indexStart = indexPos;
        int indexLimit = indexOffset + indexCnt;
        int laneSize = INTEGER_VECTOR_SPECIES.length();
        int indexVectorLimit = (indexOffset + indexCnt) / laneSize * laneSize;

        boolean tryNextSegment = false;
        for (int i = 0; i < segmentCnt; i++) {
            // the index limit of the current segment
            tryNextSegment = false;
            int limit = posSizeLimitInSegment * (i + 1);
            Vector<Integer> limitInVector = INTEGER_VECTOR_SPECIES.broadcast(limit - 1);
            // we assume that the indexes are in the asc ordering
            while (indexVectorLimit - indexPos + 1 >= laneSize) {
                // try to find the first index not belonging to the current segment in a vectorization way
                Vector<Integer> indexInVector = INTEGER_VECTOR_SPECIES.fromArray(indexes, indexPos);
                // (i0, i1, i2 ... i7) > (limit, limit, limit, ..., limit)
                VectorMask<Integer> mask = limitInVector.lt(indexInVector);
                if (mask.anyTrue()) {
                    // found, and the first index laneIndex is mask.firstTrue()
                    int count = indexPos + mask.firstTrue() - indexStart;
                    if (count > 0) {
                        partitions.add(Pair.of(indexStart, count));
                        indexPos += mask.firstTrue();
                        indexStart = indexPos;
                    } // mask.firstTrue() == 0 which means all true, try next segment
                    tryNextSegment = true;
                    break;
                }
                // try next vector
                indexPos += laneSize;
            }

            if (tryNextSegment) {
                continue;
            }

            // check in the scalar way
            while (indexPos < indexLimit) {
                if (indexes[indexPos] >= limit) {
                    int count = indexPos - indexStart;
                    if (count > 0) {
                        partitions.add(Pair.of(indexStart, count));
                        indexStart = indexPos;
                    }
                    break;
                }
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

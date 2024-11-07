/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.accumulator.state.NullableLongGroupState;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMapping;
import com.alibaba.polardbx.executor.operator.util.DriverObjectPool;
import com.alibaba.polardbx.executor.operator.util.TypedList;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.accumulator.state.NullableLongGroupState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Integer Block
 */
public class IntegerBlock extends AbstractBlock {
    private static final int NULL_VALUE = 0;
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(IntegerBlock.class).instanceSize();

    private int[] values;
    private int[] selection;

    public IntegerBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new int[slotLen];
        updateSizeInfo();
    }

    // for object pool
    public IntegerBlock(DataType dataType, int slotLen, DriverObjectPool<int[]> objectPool, int chunkLimit) {
        super(dataType, slotLen);
        int[] pooled = objectPool.poll();
        if (pooled != null && pooled.length >= slotLen) {
            this.values = pooled;
        } else {
            this.values = new int[slotLen];
        }

        updateSizeInfo();
        setRecycler(objectPool.getRecycler(chunkLimit));
    }

    public IntegerBlock(DataType dataType, int[] values, boolean[] nulls, boolean hasNull, int length,
                        int[] selection) {
        super(dataType, length, nulls, hasNull);
        this.values = values;
        this.selection = selection;
        updateSizeInfo();
    }

    public IntegerBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        updateSizeInfo();
    }

    public static IntegerBlock from(IntegerBlock other, int selSize, int[] selection, boolean useSelection) {
        if (useSelection) {
            return new IntegerBlock(other.getType(), other.intArray(), other.nulls(),
                other.hasNull(), selSize, selection);
        }
        boolean[] targetNulls = BlockUtils.copyNullArray(other.nulls(), selection, selSize);
        return new IntegerBlock(other.getType(),
            BlockUtils.copyIntArray(other.intArray(), selection, selSize),
            targetNulls,
            targetNulls != null,
            selSize, null);
    }

    @Override
    public void addLongToBloomFilter(int totalPartitionCount, RFBloomFilter[] RFBloomFilters) {
        for (int pos = 0; pos < positionCount; pos++) {

            // calc physical partition id.
            long hashVal = hashCodeUseXxhash(pos);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);

            // put hash code.
            RFBloomFilters[partition].putLong(values[pos]);
        }
    }

    @Override
    public void addLongToBloomFilter(RFBloomFilter RFBloomFilter) {
        for (int pos = 0; pos < positionCount; pos++) {
            // put hash code.
            RFBloomFilter.putLong(values[pos]);
        }
    }

    @Override
    public int mightContainsLong(RFBloomFilter RFBloomFilter, boolean[] bitmap, boolean isConjunctive) {
        int hitCount = 0;
        final int positionCount = getPositionCount();
        if (isConjunctive) {

            for (int pos = 0; pos < positionCount; pos++) {

                // Base on the original status in bitmap.
                if (bitmap[pos]) {
                    bitmap[pos] &= RFBloomFilter.mightContainLong(values[pos]);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        } else {

            for (int pos = 0; pos < positionCount; pos++) {
                bitmap[pos] = RFBloomFilter.mightContainLong(values[pos]);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }

        }

        return hitCount;
    }

    @Override
    public int mightContainsLong(int totalPartitionCount, RFBloomFilter[] RFBloomFilters, boolean[] bitmap,
                                 boolean isPartitionConsistent) {
        int hitCount = 0;
        final int positionCount = getPositionCount();

        if (isPartitionConsistent) {
            // Find the consistent partition number.
            long hashVal = hashCodeUseXxhash(0);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
            RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

            for (int pos = 0; pos < positionCount; pos++) {
                bitmap[pos] = rfBloomFilter.mightContainLong(values[pos]);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        } else {

            for (int pos = 0; pos < positionCount; pos++) {
                long hashVal = hashCodeUseXxhash(pos);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                bitmap[pos] = rfBloomFilter.mightContainLong(values[pos]);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        }

        return hitCount;
    }

    @Override
    public void addIntToBloomFilter(int totalPartitionCount, RFBloomFilter[] RFBloomFilters) {
        for (int pos = 0; pos < positionCount; pos++) {

            // calc physical partition id.
            long hashVal = XxhashUtils.finalShuffle(values[pos]);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);

            // put hash code.
            RFBloomFilters[partition].putInt(values[pos]);
        }
    }

    @Override
    public void addIntToBloomFilter(RFBloomFilter RFBloomFilter) {
        final int positionCount = getPositionCount();
        for (int pos = 0; pos < positionCount; pos++) {
            // put hash code.
            RFBloomFilter.putInt(values[pos]);
        }
    }

    @Override
    public int mightContainsInt(RFBloomFilter RFBloomFilter, boolean[] bitmap, boolean isConjunctive) {
        int hitCount = 0;

        if (isConjunctive) {
            for (int pos = 0; pos < positionCount; pos++) {
                // Base on the original status in bitmap.
                if (bitmap[pos]) {
                    int hashCode = values[pos];
                    bitmap[pos] &= RFBloomFilter.mightContainInt(hashCode);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        } else {
            for (int pos = 0; pos < positionCount; pos++) {
                int hashCode = values[pos];
                bitmap[pos] = RFBloomFilter.mightContainInt(hashCode);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        }

        return hitCount;
    }

    @Override
    public int mightContainsInt(int totalPartitionCount, RFBloomFilter[] RFBloomFilters, boolean[] bitmap,
                                boolean isPartitionConsistent) {
        int hitCount = 0;
        final int positionCount = getPositionCount();

        if (isPartitionConsistent) {
            // Find the consistent partition number.
            long hashVal = XxhashUtils.finalShuffle(values[0]);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
            RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

            for (int pos = 0; pos < positionCount; pos++) {
                int hashCode = values[pos];
                bitmap[pos] = rfBloomFilter.mightContainInt(hashCode);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        } else {

            for (int pos = 0; pos < positionCount; pos++) {
                long hashVal = XxhashUtils.finalShuffle(values[pos]);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                int hashCode = values[pos];
                bitmap[pos] = rfBloomFilter.mightContainInt(hashCode);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        }

        return hitCount;
    }

    @Override
    public int mightContainsInt(int totalPartitionCount, RFBloomFilter[] RFBloomFilters, boolean[] bitmap,
                                boolean isPartitionConsistent, boolean isConjunctive) {
        int hitCount = 0;
        final int positionCount = getPositionCount();

        if (isConjunctive) {

            if (isPartitionConsistent) {
                // Find the consistent partition number.
                long hashVal = hashCodeUseXxhash(0);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                for (int pos = 0; pos < positionCount; pos++) {

                    if (bitmap[pos]) {
                        bitmap[pos] &= rfBloomFilter.mightContainInt(values[pos]);
                        if (bitmap[pos]) {
                            hitCount++;
                        }
                    }

                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {

                    if (bitmap[pos]) {
                        long hashVal = hashCodeUseXxhash(pos);
                        int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                        RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                        bitmap[pos] &= rfBloomFilter.mightContainInt(values[pos]);
                        if (bitmap[pos]) {
                            hitCount++;
                        }

                    }

                }
            }

        } else {

            if (isPartitionConsistent) {
                // Find the consistent partition number.
                long hashVal = hashCodeUseXxhash(0);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                for (int pos = 0; pos < positionCount; pos++) {
                    bitmap[pos] = rfBloomFilter.mightContainInt(values[pos]);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {
                    long hashVal = hashCodeUseXxhash(pos);
                    int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                    RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                    bitmap[pos] = rfBloomFilter.mightContainInt(values[pos]);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        }

        return hitCount;
    }

    @Override
    public int mightContainsLong(int totalPartitionCount, RFBloomFilter[] RFBloomFilters, boolean[] bitmap,
                                 boolean isPartitionConsistent, boolean isConjunctive) {
        int hitCount = 0;

        if (isConjunctive) {

            if (isPartitionConsistent) {
                // Find the consistent partition number.
                long hashVal = XxhashUtils.finalShuffle(values[0]);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                for (int pos = 0; pos < positionCount; pos++) {

                    if (bitmap[pos]) {
                        bitmap[pos] &= rfBloomFilter.mightContainLong(values[pos]);
                        if (bitmap[pos]) {
                            hitCount++;
                        }
                    }

                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {

                    if (bitmap[pos]) {
                        long hashVal = XxhashUtils.finalShuffle(values[pos]);
                        int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                        RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                        bitmap[pos] &= rfBloomFilter.mightContainLong(values[pos]);
                        if (bitmap[pos]) {
                            hitCount++;
                        }

                    }

                }
            }

        } else {

            if (isPartitionConsistent) {
                // Find the consistent partition number.
                long hashVal = XxhashUtils.finalShuffle(values[0]);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                for (int pos = 0; pos < positionCount; pos++) {
                    bitmap[pos] = rfBloomFilter.mightContainLong(values[pos]);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {
                    long hashVal = XxhashUtils.finalShuffle(values[pos]);
                    int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                    RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                    bitmap[pos] = rfBloomFilter.mightContainLong(values[pos]);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        }

        return hitCount;
    }

    @Override
    public void recycle() {
        if (recycler != null) {
            recycler.recycle(values);
        }
    }

    private int realPositionOf(int position) {
        if (selection == null) {
            return position;
        }
        return selection[position];
    }

    @Override
    public boolean isNull(int position) {
        position = realPositionOf(position);
        return isNullInner(position);
    }

    @Override
    public int getInt(int position) {
        position = realPositionOf(position);
        return getIntInner(position);
    }

    @Override
    public long getLong(int position) {
        position = realPositionOf(position);
        return values[position + arrayOffset];
    }

    @Override
    public Object getObject(int position) {
        position = realPositionOf(position);
        return isNullInner(position) ? null : getIntInner(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        position = realPositionOf(position);
        writePositionToInner(position, blockBuilder);
    }

    @Override
    public void writePositionTo(int[] selection, final int offsetInSelection, final int positionCount,
                                BlockBuilder blockBuilder) {
        if (this.selection != null || !(blockBuilder instanceof IntegerBlockBuilder)) {
            // don't support it when selection in use.
            super.writePositionTo(selection, offsetInSelection, positionCount, blockBuilder);
            return;
        }

        // best case
        if (!mayHaveNull()) {
            ((IntegerBlockBuilder) blockBuilder).values
                .add(this.values, selection, offsetInSelection, positionCount);

            ((IntegerBlockBuilder) blockBuilder).valueIsNull
                .add(false, positionCount);

            return;
        }

        // normal case
        for (int i = 0; i < positionCount; i++) {
            int position = selection[i + offsetInSelection];

            if (isNull != null && isNull[position + arrayOffset]) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeInt(values[position + arrayOffset]);
            }
        }
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        position = realPositionOf(position);
        addToHasherInner(sink, position);
    }

    @Override
    public int hashCode(int position) {
        position = realPositionOf(position);
        return hashCodeInner(position);
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        int realPos = realPositionOf(pos);
        if (isNullInner(realPos)) {
            return NULL_HASH_CODE;
        } else {
            return XxhashUtils.finalShuffle(getIntInner(realPos));
        }
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        position = realPositionOf(position);
        return equalsInner(position, other, otherPosition);
    }

    /**
     * Designed for test purpose
     */
    public static IntegerBlock of(Integer... values) {
        final int len = values.length;
        boolean[] valueIsNull = new boolean[len];
        int[] intValues = new int[len];
        for (int i = 0; i < len; i++) {
            if (values[i] != null) {
                intValues[i] = values[i];
            } else {
                valueIsNull[i] = true;
            }
        }
        return new IntegerBlock(0, len, valueIsNull, intValues);
    }

    public static IntegerBlock wrap(int[] values) {
        return new IntegerBlock(0, values.length, null, values);
    }

    @VisibleForTesting
    public static IntegerBlock wrapWithNull(int[] values, boolean[] nulls) {
        return new IntegerBlock(0, values.length, nulls, values);
    }

    @Override
    public int[] hashCodeVector() {
        if (mayHaveNull()) {
            return super.hashCodeVector();
        }
        if (selection != null) {
            return super.hashCodeVector();
        }
        final int n = getPositionCount();
        int[] hashes = new int[n];
        System.arraycopy(values, arrayOffset, hashes, 0, n);
        return hashes;
    }

    @Override
    public void hashCodeVector(int[] results, int positionCount) {

        if (selection != null) {
            for (int position = 0; position < positionCount; position++) {
                results[position] = values[selection[position]];
            }

            if (mayHaveNull()) {
                for (int position = 0; position < positionCount; position++) {
                    if (isNull[selection[position]]) {
                        results[position] = 0;
                    }
                }
            }
        } else {
            System.arraycopy(values, arrayOffset, results, 0, positionCount);

            if (mayHaveNull()) {
                for (int position = 0; position < positionCount; position++) {
                    if (isNull[position]) {
                        results[position] = 0;
                    }
                }
            }
        }
    }

    @Override
    public DataType getType() {
        return DataTypes.IntegerType;
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        checkNoDelayMaterialization();
        if (output instanceof IntegerBlock) {
            IntegerBlock outputVectorSlot = output.cast(IntegerBlock.class);
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    outputVectorSlot.values[j] = values[j];
                }
            } else {
                System.arraycopy(values, 0, outputVectorSlot.values, 0, size);
            }
        } else {
            BlockUtils.copySelectedInCommon(selectedInUse, sel, size, this, output);
        }

        super.copySelected(selectedInUse, sel, size, output);
    }

    @Override
    public void shallowCopyTo(RandomAccessBlock another) {
        checkNoDelayMaterialization();
        if (!(another instanceof IntegerBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        IntegerBlock vectorSlot = another.cast(IntegerBlock.class);
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        position = realPositionOf(position);
        return values[position];
    }

    @Override
    public void setElementAt(int position, Object element) {
        checkNoDelayMaterialization();

        if (element != null) {
            isNull[position] = false;
            values[position] = (int) element;
        } else {
            isNull[position] = true;
            hasNull = true;
        }

    }

    public int[] intArray() {
        return values;
    }

    @Override
    public void compact(int[] selection) {
        checkNoDelayMaterialization();
        if (selection == null) {
            return;
        }
        int compactedSize = selection.length;
        for (int i = 0; i < compactedSize; i++) {
            int j = selection[i];
            values[i] = values[j];
            isNull[i] = isNull[j];
        }
        this.positionCount = compactedSize;

        // re-compute the size
        updateSizeInfo();
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(values);
        elementUsedBytes = Byte.BYTES * positionCount + Integer.BYTES * positionCount;
    }

    @Override
    public void collectNulls(int positionOffset, int positionCount, BitSet nullBitmap, int targetOffset) {
        Preconditions.checkArgument(positionOffset + positionCount <= this.positionCount);
        if (isNull == null) {
            return;
        }
        if (selection != null) {
            for (int i = positionOffset; i < positionOffset + positionCount; i++) {
                int j = selection[i];
                if (isNull[j + arrayOffset]) {
                    nullBitmap.set(targetOffset);
                }
                targetOffset++;
            }
        } else {
            for (int i = positionOffset; i < positionOffset + positionCount; i++) {
                if (isNull[i + arrayOffset]) {
                    nullBitmap.set(targetOffset);
                }
                targetOffset++;
            }
        }
    }

    @Override
    public void copyToIntArray(int positionOffset, int positionCount, int[] targetArray, int targetOffset,
                               DictionaryMapping dictionaryMapping) {
        Preconditions.checkArgument(positionOffset + positionCount <= this.positionCount);
        if (selection != null) {
            for (int i = positionOffset; i < positionOffset + positionCount; i++) {
                int j = selection[i];
                targetArray[targetOffset++] = values[j + arrayOffset];
            }
        } else {
            for (int i = positionOffset; i < positionOffset + positionCount; i++) {
                targetArray[targetOffset++] = values[i + arrayOffset];
            }
        }
    }

    @Override
    public void appendTypedHashTable(TypedList typedList, int sourceIndex, int startIndexIncluded,
                                     int endIndexExcluded) {
        Preconditions.checkArgument(endIndexExcluded <= this.positionCount);
        if (selection != null) {
            for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
                int j = selection[i];
                typedList.setInt(sourceIndex++, values[j + arrayOffset]);
            }
        } else {
            typedList.setIntArray(sourceIndex, values, startIndexIncluded, endIndexExcluded);
        }
    }

    @Override
    public void count(int[] groupIds, int[] probePositions, int selSize, NullableLongGroupState state) {
        if (!mayHaveNull()) {
            for (int i = 0; i < selSize; i++) {
                int position = probePositions[i];
                int groupId = groupIds[position];
                state.set(groupId, state.get(groupId) + 1);
            }
            return;
        }

        if (selection == null) {
            for (int i = 0; i < selSize; i++) {
                int position = probePositions[i];
                int groupId = groupIds[position];

                if (!isNull[position]) {
                    state.set(groupId, state.get(groupId) + 1);
                }
            }
        } else {
            for (int i = 0; i < selSize; i++) {
                int position = probePositions[i];
                int groupId = groupIds[position];

                int realPosition = selection[position];

                if (!isNull[realPosition]) {
                    state.set(groupId, state.get(groupId) + 1);
                }
            }
        }
    }

    public int[] getSelection() {
        return selection;
    }

    private int getIntInner(int position) {
        return values[position + arrayOffset];
    }

    private boolean isNullInner(int position) {
        return isNull != null && isNull[position + arrayOffset];
    }

    private void writePositionToInner(int position, BlockBuilder blockBuilder) {
        if (isNullInner(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeInt(getIntInner(position));
        }
    }

    private void addToHasherInner(IStreamingHasher sink, int position) {
        if (isNullInner(position)) {
            sink.putInt(NULL_VALUE);
        } else {
            sink.putInt(getIntInner(position));
        }
    }

    private int hashCodeInner(int position) {
        if (isNullInner(position)) {
            return 0;
        }
        return getIntInner(position);
    }

    private boolean equalsInner(int realPosition, Block other, int otherPosition) {
        boolean n1 = isNullInner(realPosition);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        if (other instanceof IntegerBlock || other instanceof IntegerBlockBuilder) {
            return getIntInner(realPosition) == other.getInt(otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    private void checkNoDelayMaterialization() {
        if (selection != null) {
            throw new AssertionError("un-support delay materialization in this method");
        }
    }
}

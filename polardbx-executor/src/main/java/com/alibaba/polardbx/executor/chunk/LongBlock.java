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
import com.alibaba.polardbx.executor.operator.util.DriverObjectPool;
import com.alibaba.polardbx.executor.operator.util.TypedList;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.BitSet;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Long Block
 */
public class LongBlock extends AbstractBlock {
    public static final long NULL_VALUE = 0L;
    public static final long TRUE_VALUE = 1;
    public static final long FALSE_VALUE = 0;

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(LongBlock.class).instanceSize();

    private long[] values;

    // for zero copy
    private int[] selection;

    public LongBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new long[slotLen];
        this.selection = null;
        updateSizeInfo();
    }

    // for object pool
    public LongBlock(DataType dataType, int slotLen, DriverObjectPool<long[]> objectPool, int chunkLimit) {
        super(dataType, slotLen);
        long[] pooled = objectPool.poll();
        if (pooled != null && pooled.length >= slotLen) {
            this.values = pooled;
        } else {
            this.values = new long[slotLen];
        }

        this.selection = null;
        updateSizeInfo();
        setRecycler(objectPool.getRecycler(chunkLimit));
    }

    public LongBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        this.selection = null;
        updateSizeInfo();
    }

    public LongBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] values, int[] selection) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        this.selection = selection;
        updateSizeInfo();
    }

    public static LongBlock from(LongBlock other, int selSize, int[] selection, boolean useSelection) {
        if (useSelection) {
            return new LongBlock(0, selSize, other.isNull, other.values, selection);
        }
        return new LongBlock(0,
            selSize,
            BlockUtils.copyNullArray(other.isNull, selection, selSize),
            BlockUtils.copyLongArray(other.values, selection, selSize),
            null);
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
            long hashVal = XxhashUtils.finalShuffle(values[pos + arrayOffset]);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);

            // put hash code.
            int hashCode = Long.hashCode(values[pos + arrayOffset]);
            RFBloomFilters[partition].putInt(hashCode);
        }
    }

    @Override
    public void addIntToBloomFilter(RFBloomFilter RFBloomFilter) {
        final int positionCount = getPositionCount();
        for (int pos = 0; pos < positionCount; pos++) {
            // put hash code.
            RFBloomFilter.putInt(Long.hashCode(values[pos + arrayOffset]));
        }
    }

    @Override
    public int mightContainsInt(RFBloomFilter RFBloomFilter, boolean[] bitmap, boolean isConjunctive) {
        int hitCount = 0;
        if (isConjunctive) {
            for (int pos = 0; pos < positionCount; pos++) {
                // Base on the original status in bitmap.
                if (bitmap[pos]) {
                    int hashCode = Long.hashCode(values[pos + arrayOffset]);
                    bitmap[pos] &= RFBloomFilter.mightContainInt(hashCode);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        } else {
            for (int pos = 0; pos < positionCount; pos++) {
                int hashCode = Long.hashCode(values[pos + arrayOffset]);
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
                int hashCode = Long.hashCode(values[pos]);
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

                int hashCode = Long.hashCode(values[pos]);
                ;
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
                        bitmap[pos] &= rfBloomFilter.mightContainInt(Long.hashCode(values[pos]));
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

                        bitmap[pos] &= rfBloomFilter.mightContainInt(Long.hashCode(values[pos]));
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
                    bitmap[pos] = rfBloomFilter.mightContainInt(Long.hashCode(values[pos]));
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {
                    long hashVal = hashCodeUseXxhash(pos);
                    int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                    RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                    bitmap[pos] = rfBloomFilter.mightContainInt(Long.hashCode(values[pos]));
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
    public void recycle() {
        if (recycler != null) {
            recycler.recycle(values);
        }
    }

    @Override
    public void copyToLongArray(int positionOffset, int positionCount, long[] targetArray, int targetOffset) {
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
                typedList.setLong(sourceIndex++, values[j + arrayOffset]);
            }
        } else {
            typedList.setLongArray(sourceIndex, values, startIndexIncluded, endIndexExcluded);
        }
    }

    @Override
    public long getLong(int position) {
        position = realPositionOf(position);
        return getLongInner(position);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getLong(position);
    }

    @Override
    public boolean isNull(int position) {
        position = realPositionOf(position);
        return isNullInner(position);
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        position = realPositionOf(position);
        return equalsInner(position, other, otherPosition);
    }

    @Override
    public void writePositionTo(int[] selection, int offsetInSelection, int positionCount, BlockBuilder blockBuilder) {
        if (this.selection != null || !(blockBuilder instanceof LongBlockBuilder)) {
            // don't support it when selection in use.
            super.writePositionTo(selection, offsetInSelection, positionCount, blockBuilder);
            return;
        }

        if (!mayHaveNull()) {
            ((LongBlockBuilder) blockBuilder).values
                .add(this.values, selection, offsetInSelection, positionCount);

            ((LongBlockBuilder) blockBuilder).valueIsNull
                .add(false, positionCount);

            return;
        }

        for (int i = 0; i < positionCount; i++) {
            int position = selection[i + offsetInSelection];

            if (isNull != null && isNull[position + arrayOffset]) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeLong(values[position + arrayOffset]);
            }
        }
    }

    @VisibleForTesting
    public static LongBlock wrap(long[] values) {
        return new LongBlock(0, values.length, null, values);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        position = realPositionOf(position);
        writePositionToInner(position, blockBuilder);
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
        pos = realPositionOf(pos);
        if (isNullInner(pos)) {
            return NULL_HASH_CODE;
        } else {
            return XxhashUtils.finalShuffle(values[pos + arrayOffset]);
        }
    }

    @Override
    public int[] hashCodeVector() {
        if (mayHaveNull()) {
            return super.hashCodeVector();
        }

        if (selection != null) {
            final int n = getPositionCount();
            int[] hashes = new int[n];
            for (int position = 0; position < n; position++) {
                int realPosition = realPositionOf(position);
                hashes[position] = Long.hashCode(values[realPosition + arrayOffset]);
            }
            return hashes;
        }

        final int n = getPositionCount();
        int[] hashes = new int[n];
        for (int position = 0; position < n; position++) {
            hashes[position] = Long.hashCode(values[position + arrayOffset]);
        }
        return hashes;
    }

    @Override
    public void hashCodeVector(int[] results, int positionCount) {
        if (selection != null) {
            for (int position = 0; position < positionCount; position++) {
                results[position] = Long.hashCode(values[selection[position]]);
            }

            if (mayHaveNull()) {
                for (int position = 0; position < positionCount; position++) {
                    if (isNull[selection[position]]) {
                        results[position] = 0;
                    }
                }
            }

        } else {
            for (int position = 0; position < positionCount; position++) {
                results[position] = Long.hashCode(values[position]);
            }

            if (mayHaveNull()) {
                for (int position = 0; position < positionCount; position++) {
                    if (isNull[position]) {
                        results[position] = 0;
                    }
                }
            }
        }

    }

    /**
     * Designed for test purpose
     */
    public static LongBlock ofInt(Integer... values) {
        final int len = values.length;
        boolean[] valueIsNull = new boolean[len];
        long[] longValues = new long[len];
        for (int i = 0; i < len; i++) {
            if (values[i] != null) {
                longValues[i] = values[i].longValue();
            } else {
                valueIsNull[i] = true;
            }
        }
        return new LongBlock(0, len, valueIsNull, longValues);
    }

    /**
     * Designed for test purpose
     */
    public static LongBlock of(Long... values) {
        final int len = values.length;
        boolean[] valueIsNull = new boolean[len];
        long[] longValues = new long[len];
        for (int i = 0; i < len; i++) {
            if (values[i] != null) {
                longValues[i] = values[i];
            } else {
                valueIsNull[i] = true;
            }
        }
        return new LongBlock(0, len, valueIsNull, longValues);
    }

    @Override
    public DataType getType() {
        return DataTypes.LongType;
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof LongBlock) {
            LongBlock outputVectorSlot = output.cast(LongBlock.class);
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
        if (!(another instanceof LongBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        LongBlock vectorSlot = another.cast(LongBlock.class);
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return values[position];
    }

    @Override
    public void setElementAt(int position, Object element) {
        if (element != null) {
            isNull[position] = false;
            values[position] = (long) element;
        } else {
            isNull[position] = true;
            hasNull = true;
        }
    }

    public long[] longArray() {
        return values;
    }

    @Override
    public void compact(int[] selection) {
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
        elementUsedBytes = INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(isNull))
            + VMSupport.align((int) sizeOf(values))
            + (selection == null ? 0 : VMSupport.align((int) sizeOf(selection)));
        estimatedSize = elementUsedBytes;
    }

    private int realPositionOf(int position) {
        if (selection == null) {
            return position;
        }
        return selection[position];
    }

    private long getLongInner(int position) {
        return values[position + arrayOffset];
    }

    private void writePositionToInner(int position, BlockBuilder blockBuilder) {
        if (isNullInner(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeLong(getLongInner(position));
        }
    }

    private boolean equalsInner(int position, Block other, int otherPosition) {
        boolean n1 = isNullInner(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        if (other instanceof LongBlock || other instanceof LongBlockBuilder) {
            return getLongInner(position) == other.getLong(otherPosition);
        } else if (other instanceof IntegerBlock || other instanceof IntegerBlockBuilder) {
            return getLongInner(position) == other.getInt(otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    private boolean isNullInner(int position) {
        return isNull != null && isNull[position + arrayOffset];
    }

    private void addToHasherInner(IStreamingHasher sink, int position) {
        if (isNullInner(position)) {
            sink.putLong(NULL_VALUE);
        } else {
            sink.putLong(getLongInner(position));
        }
    }

    private int hashCodeInner(int position) {
        if (isNullInner(position)) {
            return 0;
        }
        return Long.hashCode(values[position + arrayOffset]);
    }

    public int[] getSelection() {
        return selection;
    }
}

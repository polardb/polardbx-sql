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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMapping;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;

import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.BitSet;

import static com.alibaba.polardbx.common.CrcAccumulator.NULL_TAG;
import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class SliceBlock extends AbstractCommonBlock {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SliceBlock.class).instanceSize();

    private static final byte[] EMPTY_BYTES = new byte[] {};
    /**
     * If compatible is true, use collation to handle sorting, comparing and hashing.
     */
    private final boolean compatible;
    private SliceType dataType;
    /**
     * In direct mode, storing the bytes' data.
     */
    private Slice data;
    /**
     * In direct mode, storing the offsets of each slot.
     */
    private int[] offsets;
    /**
     * In dictionary mode, storing the dict data.
     */
    private BlockDictionary dictionary;
    /**
     * In dictionary mode, storing the dict id.
     * the id = -1 means null value.
     */
    private int[] dictIds;
    /**
     * Hold the effective position in this block.
     */
    private int[] selection;

    // construct the slice block using dictionary.
    public SliceBlock(SliceType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull,
                      BlockDictionary dictionary, int[] dictIds, boolean compatible) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.dataType = dataType;

        this.offsets = null;
        this.data = null;

        this.dictionary = dictionary;
        this.dictIds = dictIds;

        this.selection = null;
        this.compatible = compatible;
        updateSizeInfo();
    }

    // construct the slice block using dictionary and selection array.
    public SliceBlock(SliceType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull,
                      BlockDictionary dictionary, int[] dictIds, int[] selection, boolean compatible) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.dataType = dataType;

        this.offsets = null;
        this.data = null;

        this.dictionary = dictionary;
        this.dictIds = dictIds;

        this.selection = selection;
        this.compatible = compatible;
        updateSizeInfo();
    }

    public SliceBlock(SliceType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets,
                      Slice data, boolean compatible) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        Preconditions.checkNotNull(dataType);
        this.dataType = dataType;

        this.offsets = offsets;
        this.data = data;

        this.dictionary = null;
        this.dictIds = null;

        this.selection = null;
        this.compatible = compatible;
        updateSizeInfo();
    }

    public SliceBlock(SliceType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets,
                      Slice data, int[] selection, boolean compatible) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        Preconditions.checkNotNull(dataType);
        this.dataType = dataType;

        this.offsets = offsets;
        this.data = data;

        this.dictionary = null;
        this.dictIds = null;

        this.selection = selection;
        this.compatible = compatible;
        updateSizeInfo();
    }

    // random access
    public SliceBlock(SliceType inputType, int positionCount, boolean compatible, boolean useDictionary) {
        super(inputType, positionCount);
        this.dataType = inputType;
        if (useDictionary) {
            this.offsets = null;
            this.data = null;
            this.dictionary = null;
            this.dictIds = new int[positionCount];
        } else {
            this.offsets = new int[positionCount];
            this.data = null;
            this.dictionary = null;
            this.dictIds = null;
        }
        this.selection = null;
        this.compatible = compatible;
        // need to manually call the updateSizeInfo().
    }

    public static SliceBlock from(SliceBlock other, int selSize, int[] selection, boolean compatible,
                                  boolean useSelection) {
        if (useSelection) {
            if (other.dictionary == null) {
                return new SliceBlock(other.dataType, other.arrayOffset, selSize,
                    other.isNull, other.offsets, other.data,
                    selection, compatible
                );
            } else {
                return new SliceBlock(other.dataType, other.arrayOffset, selSize,
                    other.isNull, other.dictionary, other.dictIds,
                    selection, compatible
                );
            }
        }
        if (other.dictionary == null) {

            if (selection == null) {
                // case 1: direct copy slice
                if (other.data == null) {
                    // all values are null
                    return new SliceBlock(other.dataType, other.arrayOffset, selSize,
                        BlockUtils.copyNullArray(other.isNull, null, selSize),
                        BlockUtils.copyIntArray(other.offsets, null, selSize),
                        null, null, compatible);
                }
                return new SliceBlock(other.dataType, other.arrayOffset, selSize,
                    BlockUtils.copyNullArray(other.isNull, null, selSize),
                    BlockUtils.copyIntArray(other.offsets, null, selSize),
                    Slices.copyOf(other.data),
                    null, compatible);
            } else {
                // case 2: refactor offset & slice
                boolean[] targetNulls = BlockUtils.copyNullArray(other.isNull, selection, selSize);
                int[] targetOffsets = new int[selSize];
                if (other.data == null) {
                    // all values are null
                    Arrays.fill(targetOffsets, 0);
                    return new SliceBlock(other.dataType, other.arrayOffset, selSize,
                        targetNulls,
                        targetOffsets,
                        null,
                        null, compatible
                    );
                }
                SliceOutput sliceOutput = new DynamicSliceOutput(selSize);
                for (int position = 0; position < selSize; position++) {
                    int beginOffset = other.beginOffsetInner(selection[position]);
                    int endOffset = other.endOffsetInner(selection[position]);
                    sliceOutput.writeBytes(other.data, beginOffset, endOffset - beginOffset);
                    targetOffsets[position] = sliceOutput.size();
                }

                return new SliceBlock(other.dataType, other.arrayOffset, selSize,
                    targetNulls,
                    targetOffsets,
                    sliceOutput.slice(),
                    null, compatible
                );
            }
        } else {

            if (selection == null) {
                // case 3: direct copy dictionary
                return new SliceBlock(other.dataType, other.arrayOffset, selSize,
                    BlockUtils.copyNullArray(other.isNull, null, selSize),
                    other.dictionary,
                    BlockUtils.copyIntArray(other.dictIds, null, selSize),
                    null, compatible
                );
            } else {
                // case 4: copy arrays with selection.
                return new SliceBlock(other.dataType, other.arrayOffset, selSize,
                    BlockUtils.copyNullArray(other.isNull, selection, selSize),
                    other.dictionary,
                    BlockUtils.copyIntArray(other.dictIds, selection, selSize),
                    null, compatible
                );
            }

        }
    }

    @Override
    public void copyToIntArray(int positionOffset, int positionCount, int[] targetArray, int targetOffset,
                               DictionaryMapping dictionaryMapping) {
        if (dictionary == null) {
            throw new UnsupportedOperationException();
        }
        int[] reMapping = dictionaryMapping.merge(dictionary);
        if (selection != null) {
            for (int i = positionOffset; i < positionOffset + positionCount; i++) {
                int j = selection[i];
                int dictId = dictIds[j + arrayOffset];
                targetArray[targetOffset++] = dictId == -1 ? 0 : reMapping[dictId];
            }
        } else {
            for (int i = positionOffset; i < positionOffset + positionCount; i++) {
                int dictId = dictIds[i + arrayOffset];
                targetArray[targetOffset++] = dictId == -1 ? 0 : reMapping[dictId];
            }
        }
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

    public Slice getData() {
        return data;
    }

    public void setData(Slice data) {
        Preconditions.checkArgument(dictionary == null || data == null);
        this.data = data;
    }

    public int[] getOffsets() {
        return offsets;
    }

    public BlockDictionary getDictionary() {
        return dictionary;
    }

    public void setDictionary(BlockDictionary dictionary) {
        Preconditions.checkArgument(data == null || dictionary == null);
        this.dictionary = dictionary;
    }

    public int[] getDictIds() {
        return dictIds;
    }

    public SliceBlock setDictIds(int[] dictIds) {
        this.dictIds = dictIds;
        return this;
    }

    public int getDictId(int pos) {
        return dictIds[realPositionOf(pos)];
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
    public Object getObjectForCmp(int position) {
        position = realPositionOf(position);
        return getSortKeyInner(position);
    }

    public Comparable getSortKey(int position) {
        position = realPositionOf(position);
        return getSortKeyInner(position);
    }

    @Override
    public Object getObject(int position) {
        position = realPositionOf(position);
        return isNullInner(position) ? null : copySliceInner(position);
    }

    public Slice getRegion(int position) {
        position = realPositionOf(position);
        return getRegionInner(position);
    }

    public Slice getRegion(int position, Slice output) {
        position = realPositionOf(position);
        return getRegionInner(position, output);
    }

    @Override
    public void writePositionTo(int[] selection, int offsetInSelection, int positionCount,
                                BlockBuilder blockBuilder) {
        if (this.selection != null || !(blockBuilder instanceof SliceBlockBuilder)) {
            // don't support it when selection in use.
            super.writePositionTo(selection, offsetInSelection, positionCount, blockBuilder);
            return;
        }

        SliceBlockBuilder sliceBlockBuilder = (SliceBlockBuilder) blockBuilder;

        if (!mayHaveNull()) {
            writeNonNullTo(selection, offsetInSelection, positionCount, sliceBlockBuilder);
            return;
        }

        for (int i = 0; i < positionCount; i++) {
            writePositionToInner(selection[i + offsetInSelection], sliceBlockBuilder);
        }
    }

    private void writeNonNullTo(int[] selection, int offsetInSelection, int positionCount,
                                SliceBlockBuilder blockBuilder) {
        if (dictionary == null && blockBuilder.blockDictionary == null) {
            // case 1: Both the target block builder and this block don't use dictionary.
            for (int i = 0; i < positionCount; i++) {
                int position = selection[i + offsetInSelection];
                int beginOffset = beginOffsetInner(position);
                int endOffset = endOffsetInner(position);

                blockBuilder.sliceOutput.writeBytes(data, beginOffset, endOffset - beginOffset);
                blockBuilder.offsets.add(blockBuilder.sliceOutput.size());
            }

            // write nulls
            blockBuilder.valueIsNull.add(false, positionCount);
            return;
        }

        if (dictionary != null && blockBuilder.blockDictionary == null) {
            // case 2: The target block builder doesn't use dictionary, but this block uses it.
            if (blockBuilder.isEmpty()) {
                // case 2.1: the block builder is empty, just overwrite the dictionary.
                blockBuilder.setDictionary(dictionary);
                for (int i = 0; i < positionCount; i++) {
                    Slice dictValue;
                    int position = selection[i + offsetInSelection];
                    int dictId = dictIds[position];
                    if (dictId == -1) {
                        dictValue = Slices.EMPTY_SLICE;
                    } else {
                        dictValue = dictionary.getValue(dictId);
                    }
                    blockBuilder.valueIsNull.add(false);
                    blockBuilder.values.add(dictId);
                    blockBuilder.sliceOutput.writeBytes(dictValue);
                    blockBuilder.offsets.add(blockBuilder.sliceOutput.size());
                }

            } else {
                // case 2.2: the block builder is not empty, fall back to normal slice.
                for (int i = 0; i < positionCount; i++) {
                    Slice dictValue;
                    int position = selection[i + offsetInSelection];
                    int dictId = dictIds[position];
                    if (dictId == -1) {
                        dictValue = Slices.EMPTY_SLICE;
                    } else {
                        dictValue = dictionary.getValue(dictId);
                    }
                    blockBuilder.valueIsNull.add(false);
                    blockBuilder.sliceOutput.writeBytes(dictValue);
                    blockBuilder.offsets.add(blockBuilder.sliceOutput.size());
                }
            }
            return;
        }

        if (dictionary != null && blockBuilder.blockDictionary != null) {
            // case 3: Both the target block builder and this block use dictionary.
            if (this.dictionary.hashCode() == blockBuilder.blockDictionary.hashCode()
                && this.dictionary.sizeInBytes() == blockBuilder.blockDictionary.sizeInBytes()) {
                // same dictionary
                Slice dictValue;
                for (int i = 0; i < positionCount; i++) {
                    int position = selection[i + offsetInSelection];
                    if (position >= dictIds.length) {
                        throw new ArrayIndexOutOfBoundsException("DictId len: " + dictIds.length +
                            ", position: " + position);
                    }
                    int dictId = dictIds[position];
                    if (dictId == -1) {
                        dictValue = Slices.EMPTY_SLICE;
                    } else {
                        dictValue = dictionary.getValue(dictId);
                    }
                    blockBuilder.valueIsNull.add(false);
                    blockBuilder.values.add(dictId);
                    blockBuilder.sliceOutput.writeBytes(dictValue);
                    blockBuilder.offsets.add(blockBuilder.sliceOutput.size());
                }
                return;
            }
            // different dictionary
            int[] remapping = blockBuilder.mergeDictionary(dictionary);
            for (int i = 0; i < positionCount; i++) {
                int position = selection[i + offsetInSelection];
                int originalDictId = dictIds[position];
                if (originalDictId == -1) {
                    throw new IllegalStateException("Expect non-null value in dictionary");
                }
                int newDictId = remapping[originalDictId];
                Slice dictValue = dictionary.getValue(originalDictId);
                blockBuilder.valueIsNull.add(false);
                blockBuilder.values.add(newDictId);
                blockBuilder.sliceOutput.writeBytes(dictValue);
                blockBuilder.offsets.add(blockBuilder.sliceOutput.size());
            }
            return;
        }

        // case 4: The target block builder uses dictionary, but this block doesn't use.
        // worst performance, considered as a rare case
        Slice[] newValues = new Slice[positionCount];
        for (int i = 0; i < positionCount; i++) {
            int position = selection[i + offsetInSelection];
            newValues[i] = getRegionInner(position);
        }
        int[] remapping = blockBuilder.mergeValues(newValues);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.valueIsNull.add(false);
            blockBuilder.values.add(remapping[i]);
            blockBuilder.sliceOutput.writeBytes(newValues[i]);
            blockBuilder.offsets.add(blockBuilder.sliceOutput.size());
        }
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
        return hashCodeUseXxHashInner(pos);
    }

    @Override
    public long hashCodeUnderPairWise(int pos, boolean enableCompatible) {
        if (!enableCompatible) {
            return hashCodeUseXxhash(pos);
        } else {
            if (isNull(pos)) {
                return NULL_HASH_CODE;
            }
            // should use collation handler to calculate hashcode under partition wise
            int position = realPositionOf(pos);
            Slice subRegion = getRegionInner(position);
            return dataType.hashcode(subRegion);
        }
    }

    @Override
    public int checksum(int position) {
        position = realPositionOf(position);
        return checksumInner(position);
    }

    public int anyMatch(int position, Slice that1, Slice that2) {
        position = realPositionOf(position);

        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);

            return this.data.compareTo(beginOffset, endOffset - beginOffset, that1, 0, that1.length()) == 0
                || this.data.compareTo(beginOffset, endOffset - beginOffset, that2, 0, that2.length()) == 0
                ? 1 : 0;
        } else {
            Slice value = getDictValue(position);

            return value.compareTo(that1) == 0
                || value.compareTo(that2) == 0
                ? 1 : 0;
        }
    }

    public int anyMatch(int position, Slice that1, Slice that2, Slice that3) {
        position = realPositionOf(position);

        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);

            return this.data.compareTo(beginOffset, endOffset - beginOffset, that1, 0, that1.length()) == 0
                || this.data.compareTo(beginOffset, endOffset - beginOffset, that2, 0, that2.length()) == 0
                || this.data.compareTo(beginOffset, endOffset - beginOffset, that3, 0, that3.length()) == 0
                ? 1 : 0;
        } else {
            Slice value = getDictValue(position);

            return value.compareTo(that1) == 0
                || value.compareTo(that2) == 0
                || value.compareTo(that3) == 0
                ? 1 : 0;
        }
    }

    public int anyMatch(int position, Comparable[] those) {
        position = realPositionOf(position);

        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);

            for (int i = 0; i < those.length; i++) {
                if (this.data.compareTo(
                    beginOffset, endOffset - beginOffset, (Slice) those[i], 0, ((Slice) those[i]).length()) == 0) {
                    return 1;
                }
            }
            return 0;
        } else {
            Slice value = getDictValue(position);

            for (int i = 0; i < those.length; i++) {
                if (value.compareTo((Slice) those[i]) == 0) {
                    return 1;
                }
            }
            return 0;
        }
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        position = realPositionOf(position);

        boolean n1 = isNullInner(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        if (compatible) {
            if (other instanceof SliceBlock) {
                return equalsInner(position, other.cast(SliceBlock.class), otherPosition);
            } else if (other instanceof SliceBlockBuilder) {
                return equalsInner(position, (SliceBlockBuilder) other, otherPosition);
            } else {
                throw new AssertionError();
            }
        }

        if (this.dictionary == null) {
            if (other instanceof SliceBlockBuilder) {
                return BlockComparator.SLICE_BLOCK_NO_DICT_SLICE_BLOCK_BUILDER.compareTo(
                    this, position, other, otherPosition
                ) == 0;
            } else if (other instanceof SliceBlock) {
                if (((SliceBlock) other).dictionary == null) {
                    return BlockComparator.SLICE_BLOCK_NO_DICT_SLICE_BLOCK_NO_DICT.compareTo(
                        this, position, other, otherPosition
                    ) == 0;
                } else {
                    return BlockComparator.SLICE_BLOCK_NO_DICT_SLICE_BLOCK_DICT.compareTo(
                        this, position, other, otherPosition
                    ) == 0;
                }
            }
        } else {
            if (other instanceof SliceBlockBuilder) {
                return BlockComparator.SLICE_BLOCK_DICT_SLICE_BLOCK_BUILDER.compareTo(
                    this, position, other, otherPosition
                ) == 0;
            } else if (other instanceof SliceBlock) {
                if (((SliceBlock) other).dictionary == null) {
                    return BlockComparator.SLICE_BLOCK_DICT_SLICE_BLOCK_NO_DICT.compareTo(
                        this, position, other, otherPosition
                    ) == 0;
                } else {
                    return BlockComparator.SLICE_BLOCK_DICT_SLICE_BLOCK_DICT.compareTo(
                        this, position, other, otherPosition
                    ) == 0;
                }
            }
        }

        throw new AssertionError();

    }

    public int equals(int position, Slice that) {
        position = realPositionOf(position);

        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);

            return this.data.compareTo(beginOffset, endOffset - beginOffset, that, 0, that.length()) == 0 ? 1 : 0;
        } else {
            Slice value = getDictValue(position);

            return value.compareTo(that) == 0 ? 1 : 0;
        }
    }

    @Override
    public DataType getType() {
        return dataType;
    }

    /**
     * Reset the collation of this block.
     * This operation is risky cause collations from different charset will lead to error character parsing.
     */
    public void resetCollation(CollationName collationName) {
        // for mix of collation
        Preconditions.checkNotNull(collationName);
        SliceType newDataType = new VarcharType(CollationName.getCharsetOf(collationName), collationName);
        this.dataType = newDataType;
    }

    public void encoding(SliceOutput sliceOutput) {
        sliceOutput.writeBoolean(dictionary != null);
        if (dictionary == null) {
            encodingInner(sliceOutput);
        } else {
            encodingDictionaryInner(sliceOutput);
        }
    }

    private void encodingInner(SliceOutput sliceOutput) {
        if (selection != null) {
            int currentSize = 0;
            int[] realOffsets = new int[positionCount];
            for (int i = 0; i < positionCount; i++) {
                int j = selection[i];
                if (isNull == null || !this.isNull[j]) {
                    int beginOffset = beginOffsetInner(j);
                    int endOffset = endOffsetInner(j);
                    int len = endOffset - beginOffset;
                    currentSize += len;
                }
                realOffsets[i] = currentSize;
            }

            for (int position = 0; position < positionCount; position++) {
                sliceOutput.writeInt(realOffsets[position]);
            }
            int maxOffset = realOffsets[positionCount - 1];
            if (maxOffset > 0) {
                sliceOutput.writeInt(maxOffset);
                for (int position = 0; position < positionCount; position++) {
                    int j = selection[position];
                    if (isNull == null || !this.isNull[j]) {
                        int beginOffset = beginOffsetInner(j);
                        int endOffset = endOffsetInner(j);
                        int len = endOffset - beginOffset;
                        Slice slice = data.slice(beginOffset, len);
                        sliceOutput.writeBytes(slice);
                    }
                }
            }

        } else {
            int[] offset = this.offsets;
            for (int position = 0; position < positionCount; position++) {
                sliceOutput.writeInt(offset[position]);
            }
            int maxOffset = offset[positionCount - 1];
            if (maxOffset > 0) {
                Slice data = this.data;
                sliceOutput.writeInt(maxOffset);
                sliceOutput.writeBytes(data, 0, maxOffset);
            }
        }
    }

    private void encodingDictionaryInner(SliceOutput sliceOutput) {
        dictionary.encoding(sliceOutput);
        if (selection == null) {
            sliceOutput.writeInt(dictIds.length);
            for (int dictId : dictIds) {
                sliceOutput.writeInt(dictId);
            }
        } else {
            sliceOutput.writeInt(positionCount);
            for (int i = 0; i < positionCount; i++) {
                int j = selection[i];
                sliceOutput.writeInt(dictIds[j]);
            }
        }
    }

    // get dict value from dictionary by dictId.
    public Slice getDictValue(int position) {
        Slice value;
        int dictId = dictIds[position];
        if (dictId == -1) {
            value = Slices.EMPTY_SLICE;
        } else {
            value = dictionary.getValue(dictId);
        }
        return value;
    }

    private long hashCodeUseXxHashInner(int pos) {
        if (isNullInner(pos)) {
            return NULL_HASH_CODE;
        }

        if (compatible) {
            Slice subRegion = getRegionInner(pos);
            return dataType.hashcode(subRegion);
        }

        if (dictionary == null) {
            int beginOffset = beginOffsetInner(pos);
            int endOffset = endOffsetInner(pos);
            return XxHash64.hash(data, beginOffset, endOffset - beginOffset);
        } else {
            int dictId = dictIds[pos];
            if (dictId == -1) {
                return NULL_HASH_CODE;
            } else {
                Slice dictValue = dictionary.getValue(dictId);
                return XxHash64.hash(dictValue, 0, dictValue.length());
            }
        }
    }

    public int beginOffsetInner(int position) {
        return position + arrayOffset > 0 ? offsets[position + arrayOffset - 1] : 0;
    }

    public int endOffsetInner(int position) {
        return offsets[position + arrayOffset];
    }

    private boolean isNullInner(int position) {
        return isNull != null && isNull[position + arrayOffset];
    }

    private Comparable getSortKeyInner(int position) {
        if (isNullInner(position)) {
            return null;
        }
        if (compatible) {
            return dataType.getSortKey(getRegionInner(position));
        }
        return getRegionInner(position);
    }

    private boolean equalsInner(int realPosition, SliceBlock other, int otherPosition) {

        // by collation
        Slice region1 = getRegionInner(realPosition);
        Slice region2 = other.getRegion(otherPosition);

        if (compatible) {
            return dataType.compare(region1, region2) == 0;
        } else {
            return region1.equals(region2);
        }
    }

    boolean equalsInner(int realPosition, SliceBlockBuilder other, int otherPosition) {

        // by collation
        Slice region1 = getRegionInner(realPosition);
        Slice region2 = other.getRegion(otherPosition);

        if (compatible) {
            return dataType.compare(region1, region2) == 0;
        } else {
            return region1.equals(region2);
        }
    }

    private int hashCodeInner(int position) {
        if (isNullInner(position)) {
            return 0;
        }

        if (compatible) {
            Slice subRegion = getRegionInner(position);
            return dataType.hashcode(subRegion);
        }

        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);

            return data.hashCode(beginOffset, endOffset - beginOffset);
        } else {
            int dictId = dictIds[position];
            if (dictId == -1) {
                return 0;
            }
            return dictionary.getValue(dictId).hashCode();
        }
    }

    private int checksumInner(int position) {
        if (isNullInner(position)) {
            return NULL_TAG;
        }

        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);
            return ChunkUtil.hashCode(data, beginOffset, endOffset);
        } else {
            int dictId = dictIds[position];
            if (dictId == -1) {
                return NULL_TAG;
            }
            Slice dictValue = dictionary.getValue(dictId);
            return ChunkUtil.hashCode(dictValue, 0, dictValue.length());
        }
    }

    private void writePositionToInner(int position, BlockBuilder blockBuilder) {
        if (!(blockBuilder instanceof SliceBlockBuilder)) {
            throw new AssertionError("Expect writing to a SliceBlockBuilder");
        }

        SliceBlockBuilder b = (SliceBlockBuilder) blockBuilder;
        if (isNullInner(position)) {
            if (dictionary != null && b.blockDictionary == null) {
                if (b.isEmpty()) {
                    b.setDictionary(dictionary);
                }
            }
            b.appendNull();
            return;
        }

        if (dictionary == null && b.blockDictionary == null) {
            // case 1: Both the target block builder and this block don't use dictionary.
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);

            b.valueIsNull.add(false);
            b.sliceOutput.writeBytes(data, beginOffset, endOffset - beginOffset);
            b.offsets.add(b.sliceOutput.size());

            return;
        }

        if (dictionary != null && b.blockDictionary == null) {
            // case 2: The target block builder doesn't use dictionary, but this block uses it.

            if (b.isEmpty()) {
                // case 2.1: the block builder is empty, just overwrite the dictionary.
                b.setDictionary(dictionary);

                Slice dictValue;
                int dictId = dictIds[position];
                if (dictId == -1) {
                    dictValue = Slices.EMPTY_SLICE;
                } else {
                    dictValue = dictionary.getValue(dictId);
                }
                b.valueIsNull.add(false);
                b.values.add(dictId);
                b.sliceOutput.writeBytes(dictValue);
                b.offsets.add(b.sliceOutput.size());
            } else {
                // case 2.2: the block builder is not empty, fall back to normal slice.
                Slice dictValue;
                int dictId = dictIds[position];
                if (dictId == -1) {
                    dictValue = Slices.EMPTY_SLICE;
                } else {
                    dictValue = dictionary.getValue(dictId);
                }
                b.valueIsNull.add(false);
                b.sliceOutput.writeBytes(dictValue);
                b.offsets.add(b.sliceOutput.size());
            }

            return;
        }

        if (dictionary != null && b.blockDictionary != null) {
            // case 3: Both the target block builder and this block use dictionary.
            if (this.dictionary.hashCode() == b.blockDictionary.hashCode()
                && this.dictionary.sizeInBytes() == b.blockDictionary.sizeInBytes()) {
                // same dictionary
                Slice dictValue;
                if (position >= dictIds.length) {
                    throw new ArrayIndexOutOfBoundsException("DictId len: " + dictIds.length +
                        ", position: " + position);
                }
                int dictId = dictIds[position];
                if (dictId == -1) {
                    dictValue = Slices.EMPTY_SLICE;
                } else {
                    dictValue = dictionary.getValue(dictId);
                }
                b.valueIsNull.add(false);
                b.values.add(dictId);
                b.sliceOutput.writeBytes(dictValue);
                b.offsets.add(b.sliceOutput.size());
                return;
            }

            // different dictionary
            int[] remapping = b.mergeDictionary(dictionary);
            int originalDictId = dictIds[position];
            if (originalDictId == -1) {
                throw new IllegalStateException("Expect non-null value in dictionary");
            }
            int newDictId = remapping[originalDictId];
            Slice dictValue = dictionary.getValue(originalDictId);
            b.valueIsNull.add(false);
            b.values.add(newDictId);
            b.sliceOutput.writeBytes(dictValue);
            b.offsets.add(b.sliceOutput.size());
            return;
        }

        // case 4: The target block builder uses dictionary, but this block doesn't use.
        // bad performance, considered as a rare case
        Slice value = getRegionInner(position);
        int[] remapping = b.mergeValue(value);

        b.valueIsNull.add(false);
        b.values.add(remapping[0]);
        b.sliceOutput.writeBytes(value);
        b.offsets.add(b.sliceOutput.size());
    }

    private Slice getRegionInner(int position) {
        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);
            return data.slice(beginOffset, endOffset - beginOffset);
        } else {
            int dictId = dictIds[position];
            if (dictId == -1) {
                return Slices.EMPTY_SLICE;
            }
            return dictionary.getValue(dictId);
        }
    }

    private Slice getRegionInner(int position, Slice output) {
        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);
            return data.slice(beginOffset, endOffset - beginOffset, output);
        } else {
            int dictId = dictIds[position];
            if (dictId == -1) {
                return Slices.EMPTY_SLICE;
            }
            Slice dictValue = dictionary.getValue(dictId);
            return dictValue.slice(0, dictValue.length(), output);
        }
    }

    private Slice copySliceInner(int position) {
        if (dictionary == null) {
            int beginOffset = beginOffsetInner(position);
            int endOffset = endOffsetInner(position);
            return Slices.copyOf(data, beginOffset, endOffset - beginOffset);
        } else {
            int dictId = dictIds[position];
            if (dictId == -1) {
                return Slices.EMPTY_SLICE;
            }
            Slice dictValue = dictionary.getValue(dictId);
            return dictValue == null ? Slices.EMPTY_SLICE : Slices.copyOf(dictValue);
        }
    }

    private void addToHasherInner(IStreamingHasher sink, int position) {
        if (isNullInner(position)) {
            sink.putBytes(EMPTY_BYTES);
        } else {
            Slice encodedSlice = dataType.getCharsetHandler().encodeFromUtf8(getRegionInner(position));
            sink.putBytes(encodedSlice.getBytes());
        }
    }

    @Override
    public void updateSizeInfo() {
        if (dictionary == null) {
            // Slice.length is the memory size in bytes.
            estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + (data == null ? 0 : data.length()) + sizeOf(offsets);
            elementUsedBytes =
                Byte.BYTES * positionCount + (data == null ? 0 : data.length()) + Integer.BYTES * positionCount;
        } else {
            estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + dictionary.sizeInBytes() + sizeOf(dictIds);
            elementUsedBytes = Byte.BYTES * positionCount + dictionary.sizeInBytes() + Integer.BYTES * positionCount;
        }
    }

    public int[] getSelection() {
        return selection;
    }

    public boolean isCompatible() {
        return compatible;
    }
}
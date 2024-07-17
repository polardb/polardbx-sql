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
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Reference Block is only designed for the type which cannot be serialized, such as Blob and Clob
 */
public class ReferenceBlock<T> extends AbstractBlock {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ReferenceBlock.class).instanceSize();

    protected Object[] values;

    public ReferenceBlock(DataType<T> dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new Object[slotLen];
        updateSizeInfo();
    }

    public ReferenceBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, Object[] values,
                          DataType<T> dataType) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.values = values;
        updateSizeInfo();
    }

    public ReferenceBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, Object[] values,
                          DataType<T> dataType,
                          boolean hasNull) {
        super(dataType, positionCount, valueIsNull, hasNull);
        this.values = values;
        updateSizeInfo();
    }

    @SuppressWarnings("unchecked")
    public T getReference(int position) {
        checkReadablePosition(position);
        return (T) values[position + arrayOffset];
    }

    @Override
    public boolean equals(int position, Block otherBlock, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = otherBlock.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        if (otherBlock instanceof ReferenceBlock) {
            return getReference(position).equals((otherBlock.cast(ReferenceBlock.class)).getReference(otherPosition));
        } else if (otherBlock instanceof ReferenceBlockBuilder) {
            return getReference(position).equals(((ReferenceBlockBuilder) otherBlock).getReference(otherPosition));
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public T getObject(int position) {
        if (isNull(position)) {
            return null;
        }
        return getReference(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        blockBuilder.writeObject(getObject(position));
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Objects.hashCode(values[position + arrayOffset]);
    }

    @Override
    public long hashCodeUseXxhash(int position) {
        return hashCode(position);
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof ReferenceBlock) {
            ReferenceBlock outputVectorSlot = output.cast(ReferenceBlock.class);
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
        if (!(another instanceof ReferenceBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        ReferenceBlock vectorSlot = another.cast(ReferenceBlock.class);
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
            values[position] = element;
        } else {
            isNull[position] = true;
            hasNull = true;
        }
    }

    public Object[] objectArray() {
        return values;
    }

    /**
     * Make type-specific block from this block.
     *
     * @param context Evaluation Context
     * @return Type-specific block.
     */
    public Block toTypeSpecificBlock(EvaluationContext context, DataType resultType) {
        DataType targetType = resultType == null ? dataType : resultType;
        boolean shouldConvert = resultType != null && !resultType.equalDeeply(dataType);
        BlockBuilder blockBuilder = BlockBuilders.create(targetType, context.getExecutionContext(), positionCount);

        int batchSize = context.getPreAllocatedChunk().batchSize();
        boolean isSelectionInUse = context.getPreAllocatedChunk().isSelectionInUse();
        if (isSelectionInUse) {
            int[] selection = context.getPreAllocatedChunk().selection();
            for (int i = 0; i < batchSize; i++) {
                int j = selection[i];
                Object value = isNull[j] ? null : values[j];
                if (shouldConvert) {
                    value = targetType.convertFrom(value);
                }
                blockBuilder.writeObject(value);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                Object value = isNull[i] ? null : values[i];
                if (shouldConvert) {
                    value = targetType.convertFrom(value);
                }
                blockBuilder.writeObject(value);
            }
        }

        return blockBuilder.build();
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(values);
        elementUsedBytes = Byte.BYTES * positionCount + sizeOf(values);
    }
}

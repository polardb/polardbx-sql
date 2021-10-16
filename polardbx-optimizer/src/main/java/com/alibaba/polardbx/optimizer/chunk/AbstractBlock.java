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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;

import java.util.function.Consumer;

/**
 * Abstract random accessible data block.
 */
abstract class AbstractBlock implements Block, RandomAccessBlock {

    final int arrayOffset;
    int positionCount;

    long estimatedSize;
    long sizeInBytes;

    protected DataType dataType;
    protected boolean[] isNull;
    protected boolean hasNull;
    protected String digest;

    AbstractBlock(int arrayOffset, int positionCount, boolean[] valueIsNull) {
        Preconditions.checkArgument(positionCount >= 0);
        Preconditions.checkArgument(arrayOffset >= 0);

        this.positionCount = positionCount;
        this.arrayOffset = arrayOffset;
        this.isNull = valueIsNull;
        this.hasNull = isNull != null;
    }

    protected AbstractBlock(DataType dataType, int positionCount) {
        this(dataType, positionCount, new boolean[positionCount], true);
    }

    protected AbstractBlock(DataType dataType, int positionCount, boolean[] isNull, boolean hasNull) {
        this.dataType = dataType;
        this.positionCount = positionCount;
        this.isNull = isNull;
        this.hasNull = hasNull;
        this.arrayOffset = 0;
    }

    @Override
    public long estimateSize() {
        return estimatedSize;
    }

    @Override
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public boolean mayHaveNull() {
        return isNull != null;
    }

    @Override
    public boolean isNull(int position) {
        checkReadablePosition(position);
        return isNull != null && isNull[position + arrayOffset];
    }

    void checkReadablePosition(int position) {
        if (position < 0 || position >= positionCount) {
            throw new IllegalArgumentException("position is not valid:" + position + "," + positionCount);
        }
    }

    @Override
    public void setIsNull(boolean[] isNull) {
        this.isNull = isNull;
        this.hasNull = isNull != null;
    }

    @Override
    public boolean hasNull() {
        return this.hasNull;
    }

    @Override
    public void setHasNull(boolean hasNull) {
        this.hasNull = hasNull;
    }

    @Override
    public DataType getType() {
        return dataType;
    }

    @Override
    public boolean[] nulls() {
        return isNull;
    }

    @Override
    public String getDigest() {
        if (digest == null) {
            digest();
        }
        return digest;
    }

    @Override
    public void resize(int positionCount) {
        this.positionCount = positionCount;
    }

    // ====== methods about copying ======
    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (size > 0 && hasNull) {
            output.setHasNull(true);
            boolean[] valuesIsNullInOutput = output.nulls();
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    valuesIsNullInOutput[j] = isNull[j];
                }
            } else {
                System.arraycopy(isNull, 0, valuesIsNullInOutput, 0, size);
            }
        }
    }

    @Override
    public void shallowCopyTo(RandomAccessBlock another) {
        another.setIsNull(isNull);
        another.setHasNull(hasNull);
    }

    protected void digest() {
        this.digest = new StringBuilder()
            .append("{class = ").append(getClass().getSimpleName())
            .append(", datatype = ").append(dataType.toString())
            .append(", size = ").append(positionCount)
            .append("}")
            .toString();
    }

    protected void updateElementAt(int position, Object element, Consumer<Object> consumer) {
        if (element != null) {
            isNull[position] = false;
            consumer.accept(element);
        } else {
            isNull[position] = true;
            hasNull = true;
        }
    }

    @Override
    public Object elementAt(int position) {
        Preconditions
            .checkArgument(position >= 0 && position < positionCount, "Read position is out of range " + positionCount);
        if (hasNull && isNull[position]) {
            return null;
        }

        return getElementAtUnchecked(position);
    }

    protected Object getElementAtUnchecked(int position) {
        return getObject(position);
    }

    @Override
    public void setElementAt(int position, Object element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return getDigest();
    }
}

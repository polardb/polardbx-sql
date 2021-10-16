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

import com.alibaba.polardbx.optimizer.core.datatype.BlobType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.sql.Blob;

/**
 * Blob Block
 *
 */
public class BlobBlock extends ReferenceBlock<Blob> {

    public BlobBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, Object[] values) {
        super(arrayOffset, positionCount, valueIsNull, values, DataTypes.BlobType);
    }

    @Override
    public Blob getBlob(int position) {
        return getReference(position);
    }

    @Override
    public Blob getObject(int position) {
        return isNull(position) ? null : getBlob(position);
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
        if (otherBlock instanceof BlobBlock || otherBlock instanceof BlobBlockBuilder) {
            return DataTypes.BlobType.compare(getBlob(position), otherBlock.getBlob(otherPosition)) == 0;
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return ((BlobType) DataTypes.BlobType).hashcode(getBlob(position));
    }
}

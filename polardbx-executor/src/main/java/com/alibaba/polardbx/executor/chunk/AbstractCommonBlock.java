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
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public abstract class AbstractCommonBlock extends AbstractBlock {
    protected AbstractCommonBlock(DataType dataType, int positionCount, boolean[] isNull, boolean hasNull) {
        super(dataType, positionCount, isNull, hasNull);
    }

    protected AbstractCommonBlock(DataType dataType, int positionCount) {
        super(dataType, positionCount);
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (!(output instanceof ReferenceBlock)) {
            GeneralUtil.nestedException("cannot copy contents to " + output == null ? null : output.toString());
        }
        ReferenceBlock refBlock = output.cast(ReferenceBlock.class);
        refBlock.setHasNull(this.hasNull);

        Object[] outputArray = refBlock.objectArray();
        if (selectedInUse) {
            for (int j = 0; j < size; j++) {
                int i = sel[j];
                outputArray[i] = getObject(i);
            }
        } else {
            for (int i = 0; i < size; i++) {
                outputArray[i] = getObject(i);
            }
        }

        super.copySelected(selectedInUse, sel, size, refBlock);
    }

    @Override
    public void shallowCopyTo(RandomAccessBlock another) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return getObject(position);
    }

    @Override
    public void setElementAt(int position, Object element) {
        throw new UnsupportedOperationException("Can't update value of wrapped vector slot!");
    }
}
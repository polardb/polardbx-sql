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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;

import java.util.Map;

public abstract class AbstractSequenceManager extends AbstractLifecycle implements ISequenceManager {

    @Override
    public Long nextValue(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long nextValue(String schemaName, String seqName, int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long currValue(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exhaustValue(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateValue(String schemaName, String seqName, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getIncrement(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void invalidate(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int invalidateAll(String schemaName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type checkIfExists(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUsingSequence(String schemaName, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getSequence(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentSeqRange(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMinValueFromCurrentSeqRange(String schemaName, String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getCustomUnitArgsForGroupSeq(String schemaName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean areAllSequencesSameType(String schemaName, Type[] seqTypes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reloadConnProps(String schemaName, Map<String, Object> connProps) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetNewSeqResources(String schemaName) {
        throw new UnsupportedOperationException();
    }
}

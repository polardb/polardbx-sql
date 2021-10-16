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
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;

/**
 * 基于tddl sequence的实现
 *
 * @author jianghang 2014-4-28 下午9:04:00
 * @since 5.1.0
 */
@Activate(order = 1)
public class OptimizerSequenceManager extends AbstractLifecycle implements ISequenceManager {

    @Override
    public Long nextValue(String schemaName, String seqName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().nextValue(schemaName, seqName);
    }

    @Override
    public Long nextValue(String schemaName, String seqName, int batchSize) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().nextValue(schemaName, seqName, batchSize);
    }

    @Override
    public boolean exhaustValue(String schemaName, String seqName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().exhaustValue(schemaName, seqName);
    }

    @Override
    public void updateValue(String schemaName, String seqName, long value) {
        ExecutorContext.getContext(schemaName).getSequenceManager().updateValue(schemaName, seqName, value);
    }

    @Override
    public Integer getIncrement(String schemaName, String seqName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().getIncrement(schemaName, seqName);
    }

    @Override
    public void invalidate(String schemaName, String seqName) {
        ExecutorContext.getContext(schemaName).getSequenceManager().invalidate(schemaName, seqName);
    }

    @Override
    public int invalidateAll(String schemaName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().invalidateAll(schemaName);
    }

    @Override
    public void validateDependence(String schemaName) {
        ExecutorContext.getContext(schemaName).getSequenceManager().validateDependence(schemaName);
    }

    @Override
    public Type checkIfExists(String schemaName, String seqName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().checkIfExists(schemaName, seqName);
    }

    @Override
    public boolean isUsingSequence(String schemaName, String tableName) {
        if (OptimizerContext.getContext(schemaName).isSqlMock()) {
            return false;
        }
        // In planner test mode, there's no need to query database.
        if (ExecutorContext.getContext(schemaName) != null) {
            return ExecutorContext.getContext(schemaName).getSequenceManager().isUsingSequence(schemaName, tableName);
        } else {
            return false;
        }
    }

    @Override
    public Object getSequence(String schemaName, String seqName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().getSequence(schemaName, seqName);
    }

    @Override
    public String getCurrentSeqRange(String schemaName, String seqName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().getCurrentSeqRange(schemaName, seqName);
    }

    @Override
    public long getMinValueFromCurrentSeqRange(String schemaName, String seqName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager()
            .getMinValueFromCurrentSeqRange(schemaName, seqName);
    }

    @Override
    public boolean isCustomUnitGroupSeqSupported(String schemaName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().isCustomUnitGroupSeqSupported(schemaName);
    }

    @Override
    public int[] getCustomUnitArgsForGroupSeq(String schemaName) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().getCustomUnitArgsForGroupSeq(schemaName);
    }

    @Override
    public boolean areAllSequencesSameType(String schemaName, Type seqType) {
        return ExecutorContext.getContext(schemaName).getSequenceManager().areAllSequencesSameType(schemaName, seqType);
    }
}

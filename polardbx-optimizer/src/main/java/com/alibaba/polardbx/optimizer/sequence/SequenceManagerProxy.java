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

package com.alibaba.polardbx.optimizer.sequence;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;

/**
 * sequence proxy实现,基于extenstion查找具体实现
 *
 * @author jianghang 2014-4-28 下午5:09:15
 * @since 5.1.0
 */
public class SequenceManagerProxy extends AbstractLifecycle implements ISequenceManager {

    private static ISequenceManager instance;
    private ISequenceManager delegate;

    public static ISequenceManager getInstance() {
        if (instance == null) {
            synchronized (SequenceManagerProxy.class) {
                if (instance == null) {
                    SequenceManagerProxy seq = new SequenceManagerProxy();
                    seq.init();

                    instance = seq;
                }
            }
        }

        return instance;
    }

    @Override
    protected void doInit() {
        delegate = ExtensionLoader.load(ISequenceManager.class);
        delegate.init();
    }

    @Override
    protected void doDestroy() {
        if (delegate != null) {
            delegate.destroy();
        }
    }

    @Override
    public Long nextValue(String schemaName, String seqName) {
        return delegate.nextValue(schemaName, seqName);
    }

    @Override
    public Long nextValue(String schemaName, String seqName, int batchSize) {
        return delegate.nextValue(schemaName, seqName, batchSize);
    }

    @Override
    public boolean exhaustValue(String schemaName, String seqName) {
        return delegate.exhaustValue(schemaName, seqName);
    }

    @Override
    public void updateValue(String schemaName, String seqName, long value) {
        delegate.updateValue(schemaName, seqName, value);
    }

    @Override
    public Integer getIncrement(String schemaName, String seqName) {
        return delegate.getIncrement(schemaName, seqName);
    }

    @Override
    public void invalidate(String schemaName, String seqName) {
        delegate.invalidate(schemaName, seqName);
    }

    @Override
    public int invalidateAll(String schemaName) {
        return delegate.invalidateAll(schemaName);
    }

    @Override
    public void validateDependence(String schemaName) {
        delegate.validateDependence(schemaName);
    }

    @Override
    public Type checkIfExists(String schemaName, String seqName) {
        return delegate.checkIfExists(schemaName, seqName);
    }

    @Override
    public boolean isUsingSequence(String schemaName, String tableName) {
        return delegate.isUsingSequence(schemaName, tableName);
    }

    @Override
    public Object getSequence(String schemaName, String seqName) {
        return delegate.getSequence(schemaName, seqName);
    }

    @Override
    public String getCurrentSeqRange(String schemaName, String seqName) {
        return delegate.getCurrentSeqRange(schemaName, seqName);
    }

    @Override
    public long getMinValueFromCurrentSeqRange(String schemaName, String seqName) {
        return delegate.getMinValueFromCurrentSeqRange(schemaName, seqName);
    }

    @Override
    public boolean isCustomUnitGroupSeqSupported(String schemaName) {
        return delegate.isCustomUnitGroupSeqSupported(schemaName);
    }

    @Override
    public int[] getCustomUnitArgsForGroupSeq(String schemaName) {
        return delegate.getCustomUnitArgsForGroupSeq(schemaName);
    }

    @Override
    public boolean areAllSequencesSameType(String schemaName, Type seqType) {
        return delegate.areAllSequencesSameType(schemaName, seqType);
    }
}

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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简单的基于内存实现
 *
 * @author jianghang 2014-4-28 下午5:33:24
 * @since 5.1.0
 */
public class MemorySequenceManager extends AbstractLifecycle implements ISequenceManager {

    private LoadingCache<String, AtomicLong> sequence = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, AtomicLong>() {

            @Override
            public AtomicLong load(String key) throws Exception {
                return new AtomicLong(1);
            }
        });

    @Override
    public Long nextValue(String schemaName, String seqName) {
        try {
            return sequence.get(seqName).getAndIncrement();
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Long nextValue(String schemaName, String seqName, int batchSize) {
        try {
            return sequence.get(seqName).getAndAdd(batchSize) + batchSize - 1;
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Long currValue(String schemaName, String seqName) {
        try {
            return sequence.get(seqName).get();
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public boolean exhaustValue(String schemaName, String seqName) {
        return false;
    }

    @Override
    public void updateValue(String schemaName, String seqName, long value) {
        try {
            synchronized (this) {
                long currValue = sequence.get(seqName).get();
                if (currValue < value) {
                    sequence.get(seqName).set(value);
                }
            }
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Integer getIncrement(String schemaName, String seqName) {
        return 1;
    }

    @Override
    public void invalidate(String schemaName, String seqName) {
        sequence.invalidate(seqName);
    }

    @Override
    public int invalidateAll(String schemaName) {
        int size = (int) sequence.size();
        sequence.invalidateAll();
        return size;
    }

    @Override
    public Type checkIfExists(String schemaName, String seqName) {
        return Type.NA;
    }

    @Override
    public boolean isUsingSequence(String schemaName, String tableName) {
        return true;
    }

    @Override
    public Object getSequence(String schemaName, String seqName) {
        return null;
    }

    @Override
    public String getCurrentSeqRange(String schemaName, String seqName) {
        return null;
    }

    @Override
    public long getMinValueFromCurrentSeqRange(String schemaName, String seqName) {
        return 1L;
    }

    @Override
    public int[] getCustomUnitArgsForGroupSeq(String schemaName) {
        return null;
    }

    @Override
    public boolean areAllSequencesSameType(String schemaName, Type seqType) {
        return false;
    }

    @Override
    public void reloadConnProps(String schemaName, Map<String, Object> connProps) {
    }

    @Override
    public void resetNewSeqResources(String schemaName) {
    }
}

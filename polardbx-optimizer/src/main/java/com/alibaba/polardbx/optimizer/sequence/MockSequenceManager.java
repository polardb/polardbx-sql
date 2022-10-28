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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

/**
 * @author chenmo.cm
 */
public class MockSequenceManager extends MemorySequenceManager {

    private static final LoadingCache<String, ISequenceManager> schemaSequenceMap = CacheBuilder.newBuilder().build(
        new CacheLoader<String, ISequenceManager>() {
            @Override
            public ISequenceManager load(String key) {
                return new MemorySequenceManager();
            }
        }
    );

    public final Set<String> usingSequence = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    @Override
    public Long nextValue(String schemaName, String seqName) {
        try {
            return schemaSequenceMap.get(schemaName).nextValue(schemaName, seqName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Long nextValue(String schemaName, String seqName, int batchSize) {
        try {
            return schemaSequenceMap.get(schemaName).nextValue(schemaName, seqName, batchSize);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Long currValue(String schemaName, String seqName) {
        try {
            return schemaSequenceMap.get(schemaName).currValue(schemaName, seqName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public boolean exhaustValue(String schemaName, String seqName) {
        try {
            return schemaSequenceMap.get(schemaName).exhaustValue(schemaName, seqName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void updateValue(String schemaName, String seqName, long value) {
        try {
            schemaSequenceMap.get(schemaName).updateValue(schemaName, seqName, value);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Integer getIncrement(String schemaName, String seqName) {
        try {
            return schemaSequenceMap.get(schemaName).getIncrement(schemaName, seqName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void invalidate(String schemaName, String seqName) {
        try {
            schemaSequenceMap.get(schemaName).invalidate(schemaName, seqName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public int invalidateAll(String schemaName) {
        try {
            return schemaSequenceMap.get(schemaName).invalidateAll(schemaName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public SequenceAttribute.Type checkIfExists(String schemaName, String seqName) {
        try {
            return schemaSequenceMap.get(schemaName).checkIfExists(schemaName, seqName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public boolean isUsingSequence(String schemaName, String tableName) {
        try {
            return schemaSequenceMap.get(schemaName).isUsingSequence(schemaName, tableName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Object getSequence(String schemaName, String seqName) {
        try {
            return schemaSequenceMap.get(schemaName).getSequence(schemaName, seqName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public String getCurrentSeqRange(String schemaName, String seqName) {
        try {
            return schemaSequenceMap.get(schemaName).getCurrentSeqRange(schemaName, seqName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public int[] getCustomUnitArgsForGroupSeq(String schemaName) {
        try {
            return schemaSequenceMap.get(schemaName).getCustomUnitArgsForGroupSeq(schemaName);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

}

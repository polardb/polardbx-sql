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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.AggHashMap;
import com.alibaba.polardbx.executor.operator.util.AggResultIterator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;

import java.util.List;

public abstract class AbstractHashAggExec extends AbstractExecutor {

    protected final List<Aggregator> aggregators;

    protected final List<DataType> outputColumnMeta;

    protected final int[] groups;

    protected AggHashMap hashTable;

    AggResultIterator resultIterator;

    MemoryPool memoryPool;

    OperatorMemoryAllocatorCtx memoryAllocator;

    protected boolean finished = false;

    public AbstractHashAggExec(int[] groups, List<Aggregator> aggregators, List<DataType> outputColumnMeta,
                               ExecutionContext context) {
        super(context);
        this.groups = groups;
        this.aggregators = aggregators;
        this.outputColumnMeta = outputColumnMeta;
    }

    @Override
    Chunk doNextChunk() {
        Chunk ret = resultIterator.nextChunk();
        if (ret == null) {
            finished = true;
        }
        return ret;
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputColumnMeta;
    }

    static DataType[] collectDataTypes(List<DataType> columns, int[] indexes) {
        DataType[] result = new DataType[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            result[i] = columns.get(indexes[i]);
        }
        return result;
    }

    static DataType[] collectDataTypes(List<DataType> columns) {
        return collectDataTypes(columns, 0, columns.size());
    }

    public static DataType[] collectDataTypes(List<DataType> columns, int start, int end) {
        DataType[] result = new DataType[end - start];
        for (int i = start; i < end; i++) {
            result[i - start] = columns.get(i);
        }
        return result;
    }
}

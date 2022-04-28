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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;

public abstract class AbstractOSSTableScanExec extends SourceExec implements Closeable {
    public AbstractOSSTableScanExec(ExecutionContext context) {
        super(context);
    }

    public static AbstractOSSTableScanExec create(OSSTableScan ossTableScan, ExecutionContext context, List<DataType> dataTypeList) {
        final boolean useBufferPool = context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_BUFFER_POOL);
        AbstractOSSTableScanExec exec;
        if (useBufferPool) {
            exec = new OSSTableScanExec(ossTableScan, context, dataTypeList);
        } else {
            exec = new AsyncOSSTableScanExec(ossTableScan, context, dataTypeList);
        }
        return exec;
    }

    abstract public void initWaitFuture(ListenableFuture<List<BloomFilterInfo>> listListenableFuture);

    abstract public void setPreAllocatedChunk(MutableChunk preAllocatedChunk);

    abstract public void setFilterInputTypes(List<DataType<?>> filterInputTypes);

    abstract public void setFilterOutputTypes(List<DataType<?>> filterOutputTypes);

    abstract public void setCondition(VectorizedExpression condition);

    abstract public void setFilterBitmap(int[] filterBitmap);

    abstract public void setOutProject(int[] outProject);
}

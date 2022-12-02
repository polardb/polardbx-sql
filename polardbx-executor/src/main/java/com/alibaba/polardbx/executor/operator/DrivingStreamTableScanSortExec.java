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
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;

import java.util.List;

public class DrivingStreamTableScanSortExec extends ResumeTableScanSortExec {

    public DrivingStreamTableScanSortExec(
        LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
        long maxRowCount, long skipped, long fetched, SpillerFactory spillerFactory,
        long stepSize, List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, maxRowCount, skipped, fetched, spillerFactory, stepSize, dataTypeList);

    }

    @Override
    protected Chunk fetchChunk() {
        Chunk ret = super.fetchChunk();
        if (ret == null && isFinish) {
            scanClient.cancelAllThreads(false);
            resume();
        }
        return ret;
    }

    @Override
    public boolean shouldSuspend() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doSuspend() {
        throw new UnsupportedOperationException();
    }
}

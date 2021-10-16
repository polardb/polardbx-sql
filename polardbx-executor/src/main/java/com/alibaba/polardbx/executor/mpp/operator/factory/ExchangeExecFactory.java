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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClientSupplier;
import com.alibaba.polardbx.executor.mpp.operator.IExchangeClient;
import com.alibaba.polardbx.executor.mpp.planner.RemoteSourceNode;
import com.alibaba.polardbx.executor.operator.ExchangeExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.SortMergeExchangeExec;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.List;

public class ExchangeExecFactory extends ExecutorFactory {

    private static final Logger log = LoggerFactory.getLogger(ExchangeExecFactory.class);

    private RemoteSourceNode sourceNode;
    private ExchangeClientSupplier supplier;
    private IExchangeClient exchangeClient = null;
    private MemoryPool memoryPool = null;
    private boolean mergeSort;
    private PagesSerdeFactory pagesSerdeFactory;
    private List<DataType> types;

    public ExchangeExecFactory(
        PagesSerdeFactory pagesSerdeFactory,
        RemoteSourceNode sourceNode, ExchangeClientSupplier supplier, boolean mergeSort) {
        this.pagesSerdeFactory = pagesSerdeFactory;
        this.sourceNode = sourceNode;
        this.supplier = supplier;
        this.mergeSort = mergeSort;
        this.types = CalciteUtils.getTypes(sourceNode.getRowType());
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        Executor ret;
        RelCollation collation = sourceNode.getRelCollation();

        if (mergeSort) {
            List<RelFieldCollation> sortList = collation.getFieldCollations();

            List<OrderByOption> orderBys = ExecUtils.convertFrom(sortList);
            ret = new SortMergeExchangeExec(context, sourceNode.getRelatedId(), supplier,
                pagesSerdeFactory.createPagesSerde(types), orderBys, types
            );
        } else {
            if (exchangeClient == null) {
                memoryPool = MemoryPoolUtils
                    .createOperatorTmpTablePool("Exchange@" + System.identityHashCode(this), context.getMemoryPool());
                exchangeClient = supplier.get(new RecordMemSystemListener(memoryPool.getMemoryAllocatorCtx()), context);
            }
            ret = new ExchangeExec(context, sourceNode.getRelatedId(), exchangeClient, memoryPool,
                pagesSerdeFactory.createPagesSerde(types),
                types
            );
        }

        ret.setId(sourceNode.getRelatedId());
        if (context.getRuntimeStatistics() != null) {
            RuntimeStatHelper.registerStatForExec(sourceNode, ret, context);
        }
        return ret;
    }
}

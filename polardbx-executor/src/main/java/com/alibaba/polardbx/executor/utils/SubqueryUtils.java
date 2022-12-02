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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.profiler.memory.MemoryStatAttribute;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPoolHolder;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SubqueryUtils {

    public static final AtomicInteger nextSubqueryId = new AtomicInteger();

    public static SubqueryApply createSubqueryApply(String subqueryId,
                                                    Chunk.ChunkRow inputChunk,
                                                    RelNode plan,
                                                    List<RexNode> leftConditions,
                                                    SqlKind opKind,
                                                    ExecutionContext executionContext,
                                                    CorrelationId correlateId,
                                                    RelDataType correlateDataRowType,
                                                    SemiJoinType semiJoinType,
                                                    boolean maxOnerow) {
        return new SubqueryApply(subqueryId, inputChunk, executionContext, correlateId, semiJoinType, plan,
            leftConditions, opKind, correlateDataRowType, maxOnerow);
    }

    public static void buildScalarSubqueryValue(List<RexDynamicParam> scalarList, ExecutionContext executionContext) {
        for (RexNode rexNode : scalarList) {
            RexDynamicParam rexDynamicParam = (RexDynamicParam) rexNode;
            String subqueryId =
                rexDynamicParam.getRel().getId() + "_" + Thread.currentThread().getName() + "_"
                    + nextSubqueryId.getAndIncrement();
            SubqueryApply subqueryApply =
                createSubqueryApply(subqueryId, null, rexDynamicParam.getRel(), null, null, executionContext,
                    null, null,
                    rexDynamicParam.getSemiType(), rexDynamicParam.isMaxOnerow());

            try {
                subqueryApply.prepare();
                subqueryApply.processUntilFinish();
            } finally {
                subqueryApply.close();
            }
            SubQueryDynamicParamUtils.saveScalarSubQueryComputedValue(
                executionContext.getScalarSubqueryCtxMap(),
                rexDynamicParam, subqueryApply.getResultValue());
        }
    }

    public static ExecutionContext prepareSubqueryContext(ExecutionContext executionContext, String subqueryId) {
        ExecutionContext.CopyOption copyOption = new ExecutionContext.CopyOption()
            .setMemoryPoolHolder(new QueryMemoryPoolHolder())
            .setParameters(executionContext.cloneParamsOrNull());
        ExecutionContext subqueryContext = executionContext.copy(copyOption);
        String nameOfSubQueryMemoryPool = MemoryStatAttribute.APPLY_SUBQUERY_POOL + "_" + subqueryId;
        // build memoryPool & runtimeStat for scalar subQuery
        MemoryPool sqlMemoryPool = executionContext.getMemoryPool();
        MemoryPool subQueryMemoryPool = sqlMemoryPool.getOrCreatePool(nameOfSubQueryMemoryPool,
            sqlMemoryPool.getMaxLimit(), MemoryType.SUBQUERY);
        subqueryContext.setMemoryPool(subQueryMemoryPool);
        RuntimeStatistics newRuntimeStat = RuntimeStatHelper.buildRuntimeStat(subqueryContext);
        subqueryContext.setRuntimeStatistics(newRuntimeStat);
        subqueryContext.setApplyingSubquery(true);
        subqueryContext.setSubqueryId(subqueryId);
        return subqueryContext;
    }
}

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

package com.alibaba.polardbx.optimizer.memory;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.SqlTypeUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import org.apache.calcite.rel.RelNode;

import java.util.UUID;

/**
 * @author chenghui.lch
 */
public class MemoryPoolUtils {

    private static final Logger logger = LoggerFactory.getLogger(MemoryPoolUtils.class);

    public static MemoryPool createNewPool(String name, long limit, MemoryType memoryType, MemoryPool parent) {
        MemoryPool mp;
        if (memoryType == MemoryType.QUERY) {
            mp = new QueryMemoryPool(name, limit, parent);
        } else if (memoryType == MemoryType.TASK) {
            mp = new TaskMemoryPool(name, limit, parent);
        } else if (memoryType == MemoryType.OPERATOR) {
            mp = new MemoryPool(name, limit, parent, MemoryType.OPERATOR);
        } else if (memoryType == MemoryType.SUBQUERY) {
            //子查询不要做做SPILL
            mp = new QueryMemoryPool(name, limit, parent);
        } else if (memoryType == MemoryType.GENERAL_TP) {
            mp = new TpMemoryPool(name, limit, limit, parent);
        } else if (memoryType == MemoryType.GENERAL_AP) {
            mp = new ApMemoryPool(name, limit, limit, parent);
        } else {
            mp = new MemoryPool(name, limit, parent, memoryType);
        }
        return mp;
    }

    public static MemoryPool getPlanBuilderPool(RelNode plan, ExecutionContext executionContext) {
        MemoryPool queryMemoryPool;
        if (executionContext.getMemoryPool() != null) {
            queryMemoryPool = executionContext.getMemoryPool();
        } else {
            synchronized (executionContext) {
                if (executionContext.getMemoryPool() == null) {
                    WorkloadType workloadType = WorkloadUtil.getWorkloadType(executionContext, plan);
                    executionContext.setMemoryPool(MemoryManager.getInstance().createQueryMemoryPool(
                        WorkloadUtil.isApWorkload(workloadType),
                        executionContext.getTraceId(), executionContext.getExtraCmds()));
                }
                queryMemoryPool = executionContext.getMemoryPool();
            }
        }
        if (queryMemoryPool instanceof QueryMemoryPool) {
            return ((QueryMemoryPool) queryMemoryPool).getPlanMemPool();
        } else if (queryMemoryPool instanceof TaskMemoryPool) {
            //mpp worker
            return queryMemoryPool.getOrCreatePool("planner", queryMemoryPool.maxLimit, MemoryType.PLANER);
        } else {
            throw new IllegalStateException();
        }
    }

    public static MemoryPool createOperatorTmpTablePool(ExecutionContext ec) {
        final MemoryPool mp = ec.getMemoryPool();
        final String poolName = UUID.randomUUID().toString();
        return MemoryPoolUtils.createOperatorTmpTablePool(poolName, mp);
    }

    public static MemoryPool createOperatorTmpTablePool(String memoryPoolName, MemoryPool rootPool) {
        return rootPool.getOrCreatePool(memoryPoolName, MemoryType.OPERATOR);
    }

    public static void clearMemoryPoolIfNeed(ExecutionContext executionContext) {
        if (executionContext == null) {
            return;
        }
        SqlType sqlType = executionContext.getSqlType();
        if (!executionContext.isOnlyUseTmpTblPool() && sqlType != null && SqlTypeUtils.isDmlSqlType(sqlType)) {
            try {
                // For some mulit-stmt dml( e.g. multi-insert / multi-update),
                // they will be exec in a loop, so the memory of pool should be
                // released before staring executing next new insert/update
                // stmt.
                if (executionContext.getMemoryPool() != null) {
                    executionContext.getMemoryPool().clear();
                }
                executionContext.getRuntimeStatistics().getMemoryEstimation().reset();
            } catch (Throwable ex) {
                logger.warn("failed clear memory pool for dml", ex);
            }
        }
    }
}


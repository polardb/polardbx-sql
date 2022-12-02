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

package com.alibaba.polardbx.repo.mysql.handler.execute;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.repo.mysql.handler.LogicalInsertHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author lijiu.lzw
 */
public class InsertSelectExecuteJob extends ExecuteJob {
    private final LogicalInsert logicalInsert;
    private final Map<Integer, Integer> duplicateKeyParamMapping;

    public InsertSelectExecuteJob(ExecutionContext executionContext, ParallelExecutor parallelExecutor,
                                  LogicalInsert logicalInsert, Map<Integer, Integer> duplicateKeyParamMapping){
        super(executionContext, parallelExecutor);
        this.logicalInsert = logicalInsert;
        this.duplicateKeyParamMapping = duplicateKeyParamMapping;

        this.schemaName = logicalInsert.getSchemaName();
        if (StringUtils.isEmpty(this.schemaName)) {
            this.schemaName = executionContext.getSchemaName();
        }
        PhyTableOperationUtil.enableIntraGroupParallelism(this.schemaName,this.executionContext);
    }

    /**
     * 类似 LogicalInsertHandler.executeInsert,
     */
    @Override
    public void execute(List<List<Object>> values, long memorySize) throws Exception {
        if (executionContext.getParams() != null) {
            executionContext.getParams().getSequenceSize().set(0);
            executionContext.getParams().getSequenceIndex().set(0);
            executionContext.getParams().setSequenceBeginVal(null);
        }
        LogicalInsertHandler.buildParamsForSelect(values, logicalInsert,
            duplicateKeyParamMapping, executionContext.getParams());
        String schemaName = logicalInsert.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();
        final String tableName = logicalInsert.getLogicalTableName();
        final boolean isBroadcast = or.isBroadCast(tableName);
        final ExecutionContext insertEc = executionContext.copy();
        LogicalJob job = new LogicalJob(parallelExecutor, insertEc);
        job.setMemorySize(memorySize);

        if (null != logicalInsert.getPrimaryInsertWriter() && !logicalInsert.hasHint() && executionContext
            .getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE)) {

            RexUtils.updateParam(logicalInsert, executionContext, true, null);

            // Get plan for primary
            final InsertWriter primaryWriter = logicalInsert.getPrimaryInsertWriter();
            List<RelNode> inputs = primaryWriter.getInput(executionContext);
            final List<RelNode> primaryPhyPlan =
                inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList());

            final List<RelNode> allPhyPlan = new ArrayList<>(primaryPhyPlan);
            final List<RelNode> replicatePhyPlan =
                inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList());

            allPhyPlan.addAll(replicatePhyPlan);

            // Get plan for gsi
            final AtomicInteger writableGsiCount = new AtomicInteger(0);
            final List<InsertWriter> gsiWriters = logicalInsert.getGsiInsertWriters();
            gsiWriters.stream()
                .map(gsiWriter -> gsiWriter.getInput(executionContext))
                .filter(w -> !w.isEmpty())
                .forEach(w -> {
                    writableGsiCount.incrementAndGet();
                    allPhyPlan.addAll(w);
                });
            boolean multiWriteWithoutBroadcast =
                (writableGsiCount.get() > 0 || GeneralUtil.isNotEmpty(replicatePhyPlan)) && !isBroadcast;
            boolean multiWriteWithBroadcast =
                (writableGsiCount.get() > 0 || GeneralUtil.isNotEmpty(replicatePhyPlan)) && isBroadcast;
            job.setAllPhyPlan(allPhyPlan);
            if (multiWriteWithoutBroadcast || multiWriteWithBroadcast) {
                List<Object> params = new ArrayList<>();
                params.add(primaryPhyPlan);
                params.add(multiWriteWithoutBroadcast);
                params.add(multiWriteWithBroadcast);
                job.setAffectRowsFunction(ExecuteJob::specialAffectRows);
                job.setAffectRowsFunctionParams(params);
            }

        } else {
            List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>();
            PhyTableInsertSharder insertSharder = new PhyTableInsertSharder(logicalInsert,
                executionContext.getParams(),
                SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
            List<RelNode> inputs = logicalInsert.getInput(insertSharder, shardResults, executionContext);
            job.setAllPhyPlan(inputs);
        }
        if (job.allPhyPlan == null || job.allPhyPlan.isEmpty()) {
            //没有任务，直接结束
            parallelExecutor.getMemoryControl().release(memorySize);
            return;
        }
        scheduleJob(job);
    }
}

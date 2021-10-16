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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPhyTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.DropTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

@Getter
@TaskName(name = "CreateTablePhyDdlTask")
public class CreateTablePhyDdlTask extends BasePhyDdlTask {

    private String logicalTableName;

    @JSONCreator
    public CreateTablePhyDdlTask(String schemaName, String logicalTableName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
        this.logicalTableName = logicalTableName;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected List<RelNode> genRollbackPhysicalPlans(ExecutionContext executionContext) {
        DdlPhyPlanBuilder
            dropPhyTableBuilder = DropPhyTableBuilder
            .createBuilder(schemaName, logicalTableName, true, this.physicalPlanData.getTableTopology(),
                executionContext).build();
        List<PhyDdlTableOperation> physicalPlans = dropPhyTableBuilder.getPhysicalPlans();
        return convertToRelNodes(physicalPlans);
    }

}

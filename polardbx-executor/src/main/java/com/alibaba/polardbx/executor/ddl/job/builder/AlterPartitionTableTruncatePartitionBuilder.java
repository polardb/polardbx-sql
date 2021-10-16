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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterPartitionTableTruncatePartitionBuilder extends AlterTableBuilder {

    public AlterPartitionTableTruncatePartitionBuilder(DDL ddl, AlterTablePreparedData preparedData,
                                                       ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public void buildTableRuleAndTopology() {
        partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
            .getPartitionInfo(preparedData.getTableName());
        PartitionInfo tempPartitionInfo = partitionInfo.copy();
        List<PartitionSpec> targetTruncatePartitionSpecs = tempPartitionInfo.getPartitionBy().getPartitions().stream()
            .filter(o -> preparedData.getTruncatePartitionNames().contains(o.getName())).collect(
                Collectors.toList());
        tempPartitionInfo.getPartitionBy().setPartitions(targetTruncatePartitionSpecs);
        tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(tempPartitionInfo);
    }

    @Override
    protected void buildSqlTemplate() {
        final SqlNode primaryTableNode = new FastsqlParser()
            .parse("truncate table " + preparedData.getTableName(), executionContext)
            .get(0);
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(ddlPreparedData.getSchemaName(), executionContext);
        this.sqlTemplate = primaryTableNode.accept(visitor);
        this.originSqlTemplate = this.sqlTemplate;
    }

}

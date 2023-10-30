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

package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionItemPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterTableGroupTruncatePartitionItemBuilder extends AlterTableGroupItemBuilder {

    protected AlterTableGroupTruncatePartitionItemPreparedData preparedData;

    public AlterTableGroupTruncatePartitionItemBuilder(DDL ddl,
                                                       AlterTableGroupTruncatePartitionItemPreparedData preparedData,
                                                       ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    @Override
    public void buildNewTableTopology(String schemaName, String tableName) {
        partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
            .getPartitionInfo(preparedData.getTableName());

        PartitionInfo tempPartitionInfo = partitionInfo.copy();

        PartitionByDefinition partByDef = tempPartitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        Set<String> truncatePartitionNames = preparedData.getTruncatePartitionNames();
        List<PartitionSpec> targetTruncatePartitionSpecs = new ArrayList<>();

        partByDef.getPartitions().forEach(p -> {
            boolean hasSubParts = false;
            if (subPartByDef != null && GeneralUtil.isNotEmpty(p.getSubPartitions())) {
                List<PartitionSpec> targetTruncateSubPartSpecs =
                    p.getSubPartitions().stream().filter(sp -> truncatePartitionNames.contains(sp.getName()))
                        .collect(Collectors.toList());
                p.setSubPartitions(targetTruncateSubPartSpecs);
                hasSubParts = true;
            }
            if (truncatePartitionNames.contains(p.getName()) || hasSubParts) {
                targetTruncatePartitionSpecs.add(p);
            }
        });

        partByDef.setPartitions(targetTruncatePartitionSpecs);

        partByDef.getPhysicalPartitions().clear();

        tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(tempPartitionInfo);
    }

    @Override
    protected void buildSqlTemplate() {
        final SqlNode truncateTableNode =
            new FastsqlParser().parse("truncate table " + preparedData.getTableName(), executionContext).get(0);
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(ddlPreparedData.getSchemaName(), executionContext);
        this.originSqlTemplate = this.sqlTemplate;
        this.sqlTemplate = truncateTableNode.accept(visitor);
    }

}

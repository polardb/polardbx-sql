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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupBackfill extends AbstractRelNode {
    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    final String schemaName;
    final String logicalTableName;
    final Map<String, Set<String>> sourcePhyTables;
    final Map<String, Set<String>> targetPhyTables;

    public AlterTableGroupBackfill(RelOptCluster cluster,
                                   RelTraitSet traitSet,
                                   String schemaName,
                                   String logicalTableName,
                                   Map<String, Set<String>> sourcePhyTables,
                                   Map<String, Set<String>> targetPhyTables) {
        super(cluster, traitSet);
        this.logicalTableName = logicalTableName;
        this.schemaName = schemaName;
        this.sourcePhyTables = sourcePhyTables;
        this.targetPhyTables = targetPhyTables;
    }

    public static AlterTableGroupBackfill createAlterTableGroupBackfill(String schemaName,
                                                                        String logicalTableName, ExecutionContext ec,
                                                                        Map<String, Set<String>> sourcePhyTables,
                                                                        Map<String, Set<String>> targetPhyTables) {
        final RelOptCluster cluster = SqlConverter.getInstance(schemaName, ec).createRelOptCluster(null);
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        return new AlterTableGroupBackfill(cluster, traitSet, schemaName, logicalTableName, sourcePhyTables,
            targetPhyTables);
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public Map<String, Set<String>> getSourcePhyTables() {
        return sourcePhyTables;
    }

    public Map<String, Set<String>> getTargetPhyTables() {
        return targetPhyTables;
    }
}

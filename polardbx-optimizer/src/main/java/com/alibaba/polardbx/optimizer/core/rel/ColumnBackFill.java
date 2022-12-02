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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Used to backfill data from one column to another column
 */
public class ColumnBackFill extends AbstractRelNode {

    private String schemaName;
    private String tableName;
    private String sourceColumn;
    private String targetColumn;

    public static ColumnBackFill createColumnBackfill(String schemaName, String tableName, String sourceColumn,
                                                      String targetColumn, ExecutionContext ec) {
        final RelOptCluster cluster = SqlConverter.getInstance(schemaName, ec).createRelOptCluster(null);
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        return new ColumnBackFill(cluster, traitSet, schemaName, tableName, sourceColumn, targetColumn);
    }

    public ColumnBackFill(RelOptCluster cluster, RelTraitSet traitSet, String schemaName, String tableName,
                       String sourceColumn, String targetColumn) {
        super(cluster, traitSet);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }
}

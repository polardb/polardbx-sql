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
import org.apache.calcite.sql.SqlCall;

import java.util.List;

/**
 * Used to backfill data from one column to another column
 */
public class ColumnBackFill extends AbstractRelNode {

    private String schemaName;
    private String tableName;
    private List<SqlCall> sourceNodes;
    private List<String> targetColumns;
    private boolean forceCnEval;

    public static ColumnBackFill createColumnBackfill(String schemaName, String tableName, List<SqlCall> sourceNodes,
                                                      List<String> targetColumns, boolean forceCnEval,
                                                      ExecutionContext ec) {
        final RelOptCluster cluster = SqlConverter.getInstance(schemaName, ec).createRelOptCluster(null);
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        return new ColumnBackFill(cluster, traitSet, schemaName, tableName, sourceNodes, targetColumns, forceCnEval);
    }

    public ColumnBackFill(RelOptCluster cluster, RelTraitSet traitSet, String schemaName, String tableName,
                          List<SqlCall> sourceNodes, List<String> targetColumns, boolean forceCnEval) {
        super(cluster, traitSet);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.sourceNodes = sourceNodes;
        this.targetColumns = targetColumns;
        this.forceCnEval = forceCnEval;
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

    public List<SqlCall> getSourceNodes() {
        return sourceNodes;
    }

    public void setSourceNodes(List<SqlCall> sourceNodes) {
        this.sourceNodes = sourceNodes;
    }

    public List<String> getTargetColumns() {
        return targetColumns;
    }

    public void setTargetColumns(List<String> targetColumns) {
        this.targetColumns = targetColumns;
    }

    public boolean isForceCnEval() {
        return forceCnEval;
    }

    public void setForceCnEval(boolean forceCnEval) {
        this.forceCnEval = forceCnEval;
    }
}

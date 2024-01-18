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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dylan
 */
public class TableScanFinder extends RelShuttleImpl {

    private List<Pair<String, TableScan>> result = new ArrayList<>();

    private boolean usingPartitions;

    private boolean usingFlashBack;
    private String schemaName;

    public TableScanFinder() {
        this.usingPartitions = false;
        this.usingFlashBack = false;
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView) {
            this.schemaName = scan.getSchemaName();
            ((LogicalView) scan).getPushedRelNode().accept(this);
        }
        result.add(Pair.of(schemaName, scan));
        usingPartitions |= (scan.getPartitions() != null);
        usingFlashBack |= (scan.getFlashback() != null);
        return scan;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
        filter.getCondition().accept(finder);
        for (RexSubQuery subQuery : finder.getSubQueries()) {
            subQuery.rel.accept(this);
        }
        return visitChild(filter, 0, filter.getInput());
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
        for (RexNode node : project.getProjects()) {
            node.accept(finder);
        }
        for (RexSubQuery subQuery : finder.getSubQueries()) {
            subQuery.rel.accept(this);
        }
        return visitChild(project, 0, project.getInput());
    }

    public List<Pair<String, TableScan>> getResult() {
        return result;
    }

    /**
     * ignore sql with partition or flash back
     *
     * @return true if the sql uses partition or flashBack
     */
    public boolean shouldSkip() {
        return usingPartitions || usingFlashBack;
    }

    public Map<String, Map<String, List<TableScan>>> getMappedResult(String schema) {
        Map<String, Map<String, List<TableScan>>> mappedResult = new HashMap<>();
        for (Pair<String, TableScan> entry : result) {
            String schemaName = entry.getKey();
            if (schemaName == null) {
                schemaName = schema;
            }
            RelOptTable table = entry.getValue().getTable();
            if (!(table instanceof RelOptTableImpl)) {
                continue;
            }
            if (!mappedResult.containsKey(schemaName)) {
                mappedResult.put(schemaName, new HashMap<>());
            }
            Map<String, List<TableScan>> tables = mappedResult.get(schemaName);
            String tableName = CBOUtil.getTableMeta(table).getTableName();
            if (!tables.containsKey(tableName)) {
                tables.put(tableName, new ArrayList<>());
            }
            tables.get(tableName).add(entry.getValue());
        }
        return mappedResult;
    }
}

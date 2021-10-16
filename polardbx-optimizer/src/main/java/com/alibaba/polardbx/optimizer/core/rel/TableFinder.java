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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import java.util.function.Function;

/**
 * Visit all table references
 *
 * @author chenmo.cm
 */
public class TableFinder extends RelShuttleImpl {

    private final Function<TableScan, RelNode> tableHandler;
    private final boolean deep;

    public TableFinder(Function<TableScan, RelNode> tableHandler, boolean deep) {
        this.tableHandler = tableHandler;
        this.deep = deep;
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView && this.deep) {
            return ((LogicalView) scan).getPushedRelNode().accept(this);
        } else {
            return tableHandler.apply(scan);
        }
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        // table scan first, so as int ast
        final RelNode visited = super.visit(filter);

        return visited.accept(new RexTableFinder(this));
    }

    @Override
    public RelNode visit(LogicalProject filter) {
        // project subquery first, so as in ast
        final RelNode visited = filter.accept(new RexTableFinder(this));

        return super.visit(visited);
    }

    @Override
    public RelNode visit(LogicalTableLookup tableLookup) {
        return tableLookup.getProject().accept(this);
    }

    private static class RexTableFinder extends RexShuttle {

        final TableFinder tableFinder;

        public RexTableFinder(TableFinder tableFinder) {
            this.tableFinder = tableFinder;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            final RexNode visited = super.visitSubQuery(subQuery);
            subQuery.rel.accept(this.tableFinder);
            return visited;
        }
    }
}

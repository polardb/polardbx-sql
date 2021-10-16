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

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/**
 * @author dylan
 */
public class IndexableColumnRelFinder extends RelVisitor {

    private RelMetadataQuery mq;

    private IndexableColumnSet indexableColumnSet;

    public IndexableColumnRelFinder(RelMetadataQuery mq) {
        this.mq = mq;
        this.indexableColumnSet = new IndexableColumnSet();
    }

    public IndexableColumnRelFinder(RelMetadataQuery mq,
                                    IndexableColumnSet indexableColumnSet) {
        this.mq = mq;
        this.indexableColumnSet = indexableColumnSet;
    }

    public IndexableColumnSet getIndexableColumnSet() {
        return this.indexableColumnSet;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof LogicalJoin) {
            LogicalJoin logicalJoin = (LogicalJoin) node;
            IndexableColumnRexFinder
                indexableColumnRexFinder = new IndexableColumnRexFinder(mq, logicalJoin, indexableColumnSet);
            logicalJoin.getCondition().accept(indexableColumnRexFinder);
        } else if (node instanceof LogicalSemiJoin) {
            LogicalSemiJoin logicalSemiJoin = (LogicalSemiJoin) node;
            IndexableColumnRexFinder
                indexableColumnRexFinder = new IndexableColumnRexFinder(mq, logicalSemiJoin, indexableColumnSet);
            logicalSemiJoin.getCondition().accept(indexableColumnRexFinder);
        } else if (node instanceof Filter) {
            Filter filter = (Filter) node;
            IndexableColumnRexFinder
                indexableColumnRexFinder = new IndexableColumnRexFinder(mq, filter, indexableColumnSet);
            filter.getCondition().accept(indexableColumnRexFinder);
        } else if (node instanceof Project) {
            Project project = (Project) node;
            IndexableColumnRexFinder
                indexableColumnRexFinder = new IndexableColumnRexFinder(mq, project.getInput(), indexableColumnSet);
            for (RexNode rex : project.getProjects()) {
                if (rex instanceof RexDynamicParam) {
                    rex.accept(indexableColumnRexFinder);
                }
            }
        } else if (node instanceof LogicalSort) {
            LogicalSort logicalSort = (LogicalSort) node;
            for (RelFieldCollation fieldCollation : logicalSort.getCollation().getFieldCollations()) {
                RelColumnOrigin columnOrigin = mq.getColumnOrigin(logicalSort, fieldCollation.getFieldIndex());
                indexableColumnSet.addIndexableColumn(columnOrigin);
            }
        } else if (node instanceof LogicalAggregate) {
            LogicalAggregate logicalAggregate = (LogicalAggregate) node;

            for (Integer idx : logicalAggregate.getGroupSet()) {
                RelColumnOrigin columnOrigin = mq.getColumnOrigin(logicalAggregate.getInput(), idx);
                indexableColumnSet.addIndexableColumn(columnOrigin);
            }

            for (AggregateCall aggregateCall : logicalAggregate.getAggCallList()) {
                if (aggregateCall.getAggregation().getKind().belongsTo(SqlKind.MIN_MAX_AGG)) {
                    RelColumnOrigin columnOrigin = mq.getColumnOrigin(logicalAggregate.getInput(),
                        aggregateCall.getArgList().get(0));
                    indexableColumnSet.addIndexableColumn(columnOrigin);
                }
            }
        }
        node.childrenAccept(this);
    }

}

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

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
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
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

/**
 * @author dylan
 */
public class CoverableColumnRelFinder extends RelVisitor {

    private RelMetadataQuery mq;

    private CoverableColumnSet coverableColumnSet;

    private boolean inLogicalView = false;

    public CoverableColumnRelFinder(RelMetadataQuery mq) {
        this.mq = mq;
        this.coverableColumnSet = new CoverableColumnSet();
    }

    public CoverableColumnRelFinder(RelMetadataQuery mq,
                                    CoverableColumnSet coverableColumnSet) {
        this.mq = mq;
        this.coverableColumnSet = coverableColumnSet;
    }

    public CoverableColumnRelFinder(RelMetadataQuery mq,
                                    CoverableColumnSet coverableColumnSet, boolean inLogicalView) {
        this.mq = mq;
        this.coverableColumnSet = coverableColumnSet;
        this.inLogicalView = inLogicalView;
    }

    public CoverableColumnSet getCoverableColumnSet() {
        return this.coverableColumnSet;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (!inLogicalView) {
            if (node instanceof LogicalView) {
                LogicalView logicalView = (LogicalView) node;
                for (int i = 0; i < logicalView.getRowType().getFieldCount(); i++) {
                    Set<RelColumnOrigin> columnOrigins = mq.getColumnOrigins(logicalView.getPushedRelNode(), i);
                    coverableColumnSet.addCoverableColumn(columnOrigins);
                }
                inLogicalView = true;
                node.childrenAccept(this);
                inLogicalView = false;
                return;
            } else if (node instanceof Filter) {
                Filter filter = (Filter) node;
                CoverableColumnRexFinder
                    coverableColumnRexFinder =
                    new CoverableColumnRexFinder(mq, filter, coverableColumnSet, false);
                filter.getCondition().accept(coverableColumnRexFinder);
            } else if (node instanceof Project) {
                Project project = (Project) node;
                CoverableColumnRexFinder
                    coverableColumnRexFinder =
                    new CoverableColumnRexFinder(mq, project, coverableColumnSet, false);
                for (RexNode rex : project.getProjects()) {
                    rex.accept(coverableColumnRexFinder);
                }
            }
        } else {
            if (node instanceof Filter) {
                Filter filter = (Filter) node;
                CoverableColumnRexFinder
                    coverableColumnRexFinder =
                    new CoverableColumnRexFinder(mq, filter, coverableColumnSet, true);
                filter.getCondition().accept(coverableColumnRexFinder);
            } else if (node instanceof Project) {
                Project project = (Project) node;
                CoverableColumnRexFinder
                    coverableColumnRexFinder =
                    new CoverableColumnRexFinder(mq, project.getInput(), coverableColumnSet, true);
                for (RexNode rex : project.getProjects()) {
                    rex.accept(coverableColumnRexFinder);
                }
            } else if (node instanceof LogicalJoin) {
                LogicalJoin logicalJoin = (LogicalJoin) node;
                CoverableColumnRexFinder
                    coverableColumnRexFinder =
                    new CoverableColumnRexFinder(mq, logicalJoin, coverableColumnSet, true);
                logicalJoin.getCondition().accept(coverableColumnRexFinder);
            } else if (node instanceof LogicalSemiJoin) {
                LogicalSemiJoin logicalSemiJoin = (LogicalSemiJoin) node;
                CoverableColumnRexFinder
                    coverableColumnRexFinder =
                    new CoverableColumnRexFinder(mq, logicalSemiJoin, coverableColumnSet, true);
                logicalSemiJoin.getCondition().accept(coverableColumnRexFinder);
            } else if (node instanceof LogicalSort) {
                LogicalSort logicalSort = (LogicalSort) node;
                for (RelFieldCollation fieldCollation : logicalSort.getCollation().getFieldCollations()) {
                    Set<RelColumnOrigin> columnOrigins =
                        mq.getColumnOrigins(logicalSort, fieldCollation.getFieldIndex());
                    coverableColumnSet.addCoverableColumn(columnOrigins);
                }
            } else if (node instanceof LogicalAggregate) {
                LogicalAggregate logicalAggregate = (LogicalAggregate) node;
                for (Integer idx : logicalAggregate.getGroupSet()) {
                    Set<RelColumnOrigin> columnOrigins = mq.getColumnOrigins(logicalAggregate.getInput(), idx);
                    coverableColumnSet.addCoverableColumn(columnOrigins);
                }
                for (AggregateCall aggregateCall : logicalAggregate.getAggCallList()) {
                    List<Integer> args = aggregateCall.getArgList();
                    for (Integer idx : args) {
                        Set<RelColumnOrigin> columnOrigins = mq.getColumnOrigins(logicalAggregate.getInput(), idx);
                        coverableColumnSet.addCoverableColumn(columnOrigins);
                    }
                }
            }
        }
        node.childrenAccept(this);
    }

}

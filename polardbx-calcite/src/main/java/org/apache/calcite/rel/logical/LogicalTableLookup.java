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

package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.List;

/**
 * @author chenmo.cm
 */
public class LogicalTableLookup extends TableLookup {

    private RelOptCost fixedCost;

    public LogicalTableLookup(RelOptCluster cluster, RelTraitSet traitSet, RelNode index, RelNode primary,
                              RelOptTable indexTable, RelOptTable primaryTable, RelTraitSet joinTraitSet,
                              JoinRelType joinType, RexNode condition,
                              List<RexNode> projects, RelDataType rowType, boolean relPushedToPrimary,
                              SqlNodeList hints) {
        super(cluster,
            traitSet,
            index,
            primary,
            indexTable,
            primaryTable,
            joinTraitSet,
            joinType,
            condition,
            projects,
            rowType,
            relPushedToPrimary,
            hints);
    }

    public LogicalTableLookup copy(JoinRelType joinType, RexNode condition, List<RexNode> projects, RelDataType rowType) {
        LogicalTableLookup logicalTableLookup =
            copy(joinType, condition, projects, rowType, getJoin().getLeft(), getJoin().getRight(),
                isRelPushedToPrimary());
        logicalTableLookup.setFixedCost(this.fixedCost);
        return logicalTableLookup;
    }

    public LogicalTableLookup copy(JoinRelType joinType, RexNode condition, List<RexNode> projects, RelDataType rowType, RelNode index,
                                   RelNode primary, boolean relPushedToPrimary) {
        LogicalTableLookup logicalTableLookup =
            copy(getTraitSet(), joinType, condition, projects, rowType, index, primary, relPushedToPrimary);
        logicalTableLookup.setFixedCost(this.fixedCost);
        return logicalTableLookup;
    }

    public LogicalTableLookup copy(RelTraitSet traits, JoinRelType joinType, RexNode condition, List<RexNode> projects,
                                   RelDataType rowType,
                                   RelNode index, RelNode primary, boolean relPushedToPrimary) {
        LogicalTableLookup logicalTableLookup = new LogicalTableLookup(getCluster(),
            traits,
            index,
            primary,
            getIndexTable(),
            getPrimaryTable(),
            this.getCluster().getPlanner().emptyTraitSet(),
            joinType,
            condition,
            projects,
            rowType,
            relPushedToPrimary,
            getHints());
        logicalTableLookup.setFixedCost(this.fixedCost);
        return logicalTableLookup;
    }

    @Override
    public LogicalTableLookup copy(RelTraitSet traitSet, RelNode index, RelNode primary, RelOptTable indexTable,
                        RelOptTable primaryTable, Project project, Join join,
                        boolean relPushedToPrimary, SqlNodeList hints) {
        LogicalTableLookup logicalTableLookup = new LogicalTableLookup(getCluster(),
            traitSet,
            index,
            primary,
            indexTable,
            primaryTable,
            join.getTraitSet(),
            join.getJoinType(),
            join.getCondition(),
            project.getProjects(),
            project.getRowType(),
            relPushedToPrimary,
            hints);
        logicalTableLookup.setFixedCost(this.fixedCost);
        return logicalTableLookup;
    }

    @Override
    protected String getOpName() {
        return "LogicalTableLookup";
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    public RelOptCost getFixedCost() {
        return fixedCost;
    }

    public void setFixedCost(RelOptCost fixedCost) {
        this.fixedCost = fixedCost;
    }
}

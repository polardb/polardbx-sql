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

package org.apache.calcite.rel.core;

import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlNodeList;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * <code>TableLookup</code> is an abstract base class for implementations of
 * {@link org.apache.calcite.rel.logical.LogicalTableLookup}.
 *
 * @author chenmo.cm
 * @date 2019/6/14 2:00 PM
 */
public abstract class TableLookup extends SingleRel {

    /**
     * For index-primary table relation check
     */
    protected final RelOptTable indexTable;
    protected final RelOptTable primaryTable;

    /**
     * <pre>
     *       TableLookup
     *          /   \
     *  IndexScan   LogicalView
     *
     * is semantically equal to
     *
     *         Project
     *            |
     *       Inner Join(pk = pk and sk = sk)
     *          /   \
     *  IndexScan   LogicalView
     *
     * </pre>
     */
    protected LogicalProject    project;
    protected LogicalJoin       join;

    /**
     * If any operator pushed to right child, table lookup must not be removed
     */
    protected final boolean     relPushedToPrimary;

    public TableLookup(RelOptCluster cluster, RelTraitSet traitSet, RelNode index, RelNode primary,
                       RelOptTable indexTable, RelOptTable primaryTable, RelTraitSet joinTraitSet, JoinRelType joinType,
                       RexNode condition, List<RexNode> projects, RelDataType rowType, boolean relPushedToPrimary,
                       SqlNodeList hints){
        super(cluster, traitSet, index);
        Preconditions.checkNotNull(indexTable);
        Preconditions.checkNotNull(primaryTable);
        this.indexTable = indexTable;
        this.primaryTable = primaryTable;
        this.hints = hints;
        this.relPushedToPrimary = relPushedToPrimary;
        this.join = new LogicalJoin(cluster,
            joinTraitSet,
            index,
            primary,
            condition,
            ImmutableSet.of(),
            joinType,
            false,
            ImmutableList.of(),
            hints);
        this.project = (LogicalProject) new LogicalProject(cluster, traitSet.replace(Convention.NONE),
            this.join, projects, rowType).setHints(hints);
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        super.replaceInput(ordinalInParent, p);
        this.join.replaceInput(ordinalInParent, p);
        recomputeDigest();
    }

    @Override
    protected RelDataType deriveRowType() {
        return project.getRowType();
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        final Project project = (Project)this.project.accept(shuttle);
        if (project == this.project) {
            return this;
        }
        final Join join = (Join) project.getInput(0);
        return copy(getTraitSet(),
            join.getLeft(),
            join.getRight(),
            getIndexTable(),
            getPrimaryTable(),
            project,
            join,
            isRelPushedToPrimary(),
            getHints());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return copy(traitSet,
            inputs.get(0),
            join.getRight(),
            indexTable,
            primaryTable,
            this.project,
            this.join,
            relPushedToPrimary,
            this.hints);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, getOpName());
        for (Ord<RelDataTypeField> field : Ord.zip(this.project.getRowType().getFieldList())) {
            String fieldName = field.e.getName();

            RexExplainVisitor visitor = new RexExplainVisitor(this.project);
            this.project.getChildExps().get(field.i).accept(visitor);
            pw.item(fieldName.replaceAll("[\\t\\n\\r]", " "), visitor.toSqlString().replaceAll("[\\t\\n\\r]", " "));
        }

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        this.join.getCondition().accept(visitor);

        if (isRelPushedToPrimary()) {
            return pw.item("condition", visitor.toSqlString())
                .item("type", this.join.getJoinType().name().toLowerCase())
                .item("removable", "false");
        } else {
            return pw.item("condition", visitor.toSqlString())
                .item("type", this.join.getJoinType().name().toLowerCase());
        }
    }

    public abstract RelNode copy(RelTraitSet traitSet, RelNode index, RelNode primary, RelOptTable indexTable,
                                    RelOptTable primaryTable, Project project, Join join,
                                    boolean relPushedToPrimary, SqlNodeList hints);

    protected abstract String getOpName();

    public RelOptTable getIndexTable() {
        return indexTable;
    }

    public RelOptTable getPrimaryTable() {
        return primaryTable;
    }

    public boolean isRelPushedToPrimary() {
        return relPushedToPrimary;
    }

    public LogicalProject getProject() {
        return project;
    }

    public LogicalJoin getJoin() {
        return join;
    }

    @Override
    public RelNode getInput() {
        return join.getLeft();
    }
}

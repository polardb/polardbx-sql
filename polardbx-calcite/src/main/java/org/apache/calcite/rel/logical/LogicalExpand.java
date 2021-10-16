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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class LogicalExpand extends SingleRel {

    private final List<List<RexNode>> projects;
    private RelDataType outputRowType;
    private int expandIdIdx;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param input Input relational expression
     */
    public LogicalExpand(RelOptCluster cluster, RelTraitSet traits,
                         RelNode input, RelDataType outputRowType, List<List<RexNode>> projects, int expandIdIdx) {
        super(cluster, traits, input);
        this.outputRowType = outputRowType;
        this.projects = projects;
        this.expandIdIdx = expandIdIdx;
    }

    /**
     * Creates a LogicalExpand by parsing serialized output.
     */
    public LogicalExpand(RelInput input) {
        super(input.getCluster(),
            input.getTraitSet(),
            input.getInput());
        this.outputRowType = input.getRowType("outputType");
        this.projects = input.getExpressionListList("projects");
        this.expandIdIdx = input.getInteger("expandIdIdx");
    }

    public List<List<RexNode>> getProjects() {
        return projects;
    }

    public int getExpandIdIdx() {
        return expandIdIdx;
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalExpand(getCluster(), traitSet, sole(inputs), outputRowType, projects, expandIdIdx);
    }

    public RelNode copy(RelTraitSet traitSet, RelNode input, List<List<RexNode>> projects,
                        RelDataType outputRowType) {
        return new LogicalExpand(getCluster(), traitSet, input, outputRowType, projects, expandIdIdx);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a LogicalExpand.
     */
    public static LogicalExpand create(final RelNode input, RelDataType outputRowType,
                                       final List<List<RexNode>> projects, int expandIdIdx) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = input.getCluster().getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
        return new LogicalExpand(cluster, traitSet, input, outputRowType, projects, expandIdIdx);
    }

    @Override
    public RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double rowCnt = mq.getRowCount(getInput()) * projects.size();
        return planner.getCostFactory().makeCost(rowCnt, 1, 0, 0, 0);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getInput()) * projects.size();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("projects", projects);
        pw.item("outputType", outputRowType);
        pw.item("expandIdIdx", expandIdIdx);
        return pw;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "Expand");
        List<String> fieldNames = new ArrayList<>();
        for (Ord<RelDataTypeField> field : Ord.zip(outputRowType.getFieldList())) {
            String fieldName = field.e.getName();
            if (fieldName == null) {
                fieldName = "field#" + field.i;
            }
            fieldNames.add(fieldName.replaceAll("[\\t\\n\\r]", " "));
        }

        List<String> projectList = new ArrayList<String>(projects.size());
        for (List<RexNode> exps : projects) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < exps.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                RexExplainVisitor visitor = new RexExplainVisitor(this);
                exps.get(i).accept(visitor);
                sb.append(fieldNames.get(i)).append("=").append(visitor.toSqlString().replaceAll("[\\t\\n\\r]", " "));
            }
            sb.append("}");
            projectList.add(sb.toString());
        }
        pw.item("projects", StringUtils.join(projectList, ", "));
        return pw;
    }
}

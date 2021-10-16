/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class RuntimeFilterBuilder extends SingleRel {
    //~ Constructors -----------------------------------------------------------

    private final RexNode condition;

    protected RuntimeFilterBuilder(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexNode condition) {
        super(cluster, traitSet, child);
        this.condition = condition;
    }

    /**
     * Creates a RuntimeFilterBuilder by parsing serialized output.
     */
    public RuntimeFilterBuilder(RelInput input) {
        this(input.getCluster(), input.getTraitSet(), input.getInput(),
            input.getExpression("condition"));
    }

    /**
     * Creates a LogicalFilter.
     */
    public static RuntimeFilterBuilder create(final RelNode input, RexNode condition) {
        return new RuntimeFilterBuilder(input.getCluster(), input.getTraitSet(), input, condition);
    }

    @Override
    public final RuntimeFilterBuilder copy(RelTraitSet traitSet,
                                           List<RelNode> inputs) {
        return new RuntimeFilterBuilder(input.getCluster(), traitSet, sole(inputs), condition);
    }

    public final RuntimeFilterBuilder copy(RelTraitSet traitSet,
                                           List<RelNode> inputs,
                                           RexNode condition) {
        return new RuntimeFilterBuilder(input.getCluster(), traitSet, sole(inputs), condition);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("condition", condition);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "RuntimeFilterBuilder");
        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        pw.item("condition", visitor.toSqlString());
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(
        RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeCost(1, 1, 0, 0, 0);
    }

    public RexNode getCondition() {
        return condition;
    }
}


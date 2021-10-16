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

package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;

/**
 * @author dylan
 */
public class ViewPlan extends AbstractRelNode {

    private final RelOptTableImpl view;

    private final RelNode plan;

    protected ViewPlan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTableImpl view, RelNode plan) {
        super(cluster, traitSet);
        this.plan = plan;
        this.view = view;
    }

    public RelNode getPlan() {
        return plan;
    }

    public static ViewPlan create(RelOptCluster cluster, RelOptTableImpl view, RelNode plan) {
        final RelTraitSet traitSet = cluster.traitSetOf(DrdsConvention.INSTANCE);
        return new ViewPlan(cluster, traitSet, view, plan);
    }

    @Override
    protected RelDataType deriveRowType() {
        return plan.getRowType();
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "ViewPlan");
        pw.item("viewName", getViewName());
        pw.item("plan", plan);
        return pw;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "ViewPlan");
        pw.item("viewName", getViewName());
        pw.item("plan", plan);
        return pw;
    }

    @Override
    public RelOptTable getTable() {
        return view;
    }

    public String getViewName() {
        if (view.getImplTable() instanceof DrdsViewTable) {
            return ((DrdsViewTable) view.getImplTable()).getRow().getViewName();
        } else {
            return view.getQualifiedName().get(view.getQualifiedName().size() - 1);
        }
    }
}

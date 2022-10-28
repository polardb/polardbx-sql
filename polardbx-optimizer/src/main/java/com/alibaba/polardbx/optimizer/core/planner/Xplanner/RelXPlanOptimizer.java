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

package com.alibaba.polardbx.optimizer.core.planner.Xplanner;

import com.alibaba.polardbx.optimizer.core.planner.rule.FilterMergeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.Xplan.XPlanGetRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.Xplan.XPlanTableProjectRule;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterWindowTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectSortTransposeRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

/**
 * @version 1.0
 */
public class RelXPlanOptimizer {

    public static RelNode optimize(RelNode relNode) {
        // Replace TableScan to GettableTableScan which support get, multi-get or specialized index scan.
        relNode = relNode.accept(new GettableTableReplacer());

        HepProgramBuilder builder = new HepProgramBuilder();

        // Push filter to TableScan and generate GetPlan.
        builder.addGroupBegin();
        // Just make filter closer to TableScan which helps generating Get in XPlan.
        builder.addRuleInstance(FilterAggregateTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterWindowTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterMergeRule.INSTANCE);
        builder.addGroupEnd();

        // Generate get after all filter pushed and merged.
        builder.addGroupBegin();
        builder.addRuleInstance(XPlanGetRule.INSTANCE);
        builder.addGroupEnd();

        // Push simple project to TableScan and generate TableProject.
        builder.addGroupBegin();
//        builder.addRuleInstance(ProjectJoinTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectSortTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectFilterTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectWindowTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addRuleInstance(XPlanTableProjectRule.INSTANCE);
        builder.addGroupEnd();

        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(relNode);
        return planner.findBestExp();
    }

    private static class GettableTableReplacer extends RelShuttleImpl {
        @Override
        public RelNode visit(TableScan scan) {
            if (scan instanceof LogicalView) {
                final RelNode pushed = ((LogicalView) scan).getPushedRelNode();
                return pushed.accept(this);
            }
            return new XPlanTableScan(scan);
        }
    }

    public static class IndexFinder extends RelVisitor {
        String index;
        String tableName;
        double rowCount;

        boolean usingWhere;

        public IndexFinder() {
            this.index = null;
            this.tableName = null;
            this.rowCount = -1D;
            this.usingWhere = false;
        }

        public String getIndex() {
            return index;
        }

        public String getTableName() {
            return tableName;
        }

        public boolean found() {
            return !StringUtils.isEmpty(index);
        }

        public double getRowCount() {
            return (rowCount < 1) ? 1D : rowCount;
        }

        public boolean isUsingWhere() {
            return usingWhere;
        }

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof XPlanTableScan) {
                XPlanTableScan scan = (XPlanTableScan) node;
                index = scan.getGetIndex();
                if (StringUtils.isEmpty(index)) {
                    index = "PRIMARY";
                }
                tableName = Util.last(scan.getTable().getQualifiedName());
                rowCount =
                    node.getCluster().getMetadataQuery().getRowCount(((XPlanTableScan) node).getNodeForMetaQuery());
                return;
            }
            if (node instanceof LogicalFilter) {
                usingWhere = true;
            }
            node.childrenAccept(this);
        }
    }
}

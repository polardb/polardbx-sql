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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.OneStepTransformer;
import com.alibaba.polardbx.optimizer.core.planner.rule.FilterMergeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.Xplan.XPlanCalcRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.Xplan.XPlanNewGetRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.Xplan.XPlanTableProjectRule;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.alibaba.polardbx.optimizer.statis.XplanStat;
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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.InvocationTargetException;

/**
 * @version 1.0
 */
public class RelXPlanOptimizer {

    private static final Logger logger = LoggerFactory.getLogger(RelXPlanOptimizer.class);

    /**
     * Generate XPlan via raw relnode.
     * TODO: lock not supported now.
     * Always generate the XPlan in case of switching connection pool.
     */
    public static boolean canUseXPlan(SqlNode sqlNode) {
        return ConfigDataMode.isPolarDbX() && sqlNode.getKind() == SqlKind.SELECT
            && ((SqlSelect) sqlNode).getLockMode() == SqlSelect.LockMode.UNDEF;
    }

    public static void setXTemplate(BaseQueryOperation operation, RelNode relNode,
                                    ExecutionContext ec) {
        final RelToXPlanConverter converter = new RelToXPlanConverter();
        try {
            RelNode node = RelXPlanOptimizer.optimize(relNode);
            operation.setXTemplate(converter.convert(node));
            operation.setOriginPlan(relNode);
        } catch (Exception e) {
            Throwable throwable = e;
            while (throwable.getCause() != null
                && throwable.getCause() instanceof InvocationTargetException) {
                throwable = ((InvocationTargetException) throwable.getCause()).getTargetException();
            }
            logger.info("XPlan converter: " + throwable.getMessage());
        }
    }

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
        builder.addRuleInstance(XPlanNewGetRule.INSTANCE);
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

        HepPlanner planner = new HepPlanner(builder.build(), relNode.getCluster().getPlanner().getContext());
        planner.stopOptimizerTrace();
        planner.setRoot(relNode);
        return planner.findBestExp();
    }

    public static RelNode optimizeFilter(RelNode relNode) {
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

        public IndexFinder() {
            this.index = null;
        }

        public String getIndex() {
            return index;
        }

        public boolean found() {
            return !StringUtils.isEmpty(index);
        }

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof XPlanTableScan) {
                XPlanTableScan scan = (XPlanTableScan) node;
                index = scan.getGetIndex();
                if (StringUtils.isEmpty(index)) {
                    index = "PRIMARY";
                }
                return;
            }
            node.childrenAccept(this);
        }
    }

    public static class XplanExplainExecuteVisitor extends RelVisitor {

        private final String xplanIndex;
        private XPlanCalcRule.IndexInfo indexInfo;

        public XplanExplainExecuteVisitor(ExecutionContext executionContext) {
            this.xplanIndex = XplanStat.getXplanIndex(executionContext.getXplanStat());
            this.indexInfo = null;
        }

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof LogicalFilter && node.getInput(0) instanceof XPlanTableScan) {
                XPlanCalcRule rule = new XPlanCalcRule(xplanIndex);
                OneStepTransformer.transform(node, rule);
                this.indexInfo = rule.getIndexInfo();
                return;
            }
            node.childrenAccept(this);
        }

        public XPlanCalcRule.IndexInfo getIndexInfo() {
            return indexInfo;
        }
    }
}

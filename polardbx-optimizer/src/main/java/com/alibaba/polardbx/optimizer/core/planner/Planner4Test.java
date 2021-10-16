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

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.optimizer.core.planner.rule.SQL_REWRITE_RULE_PHASE;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;

import java.util.LinkedList;
import java.util.List;

/**
 * @author fangwu
 */
public class Planner4Test {

    private SQL_REWRITE_RULE_PHASE pre;

    private List<RelOptRule> preRule = new LinkedList<>();

    private RelOptRule extr;

    private HepMatchOrder matchOrder = HepMatchOrder.ARBITRARY;

    public Planner4Test() {
    }

    private HepProgramBuilder getHepProgramBuilder() {
        HepProgramBuilder hepPgmBuilder = new HepProgramBuilder();
        hepPgmBuilder.addMatchOrder(matchOrder);

        for (SQL_REWRITE_RULE_PHASE r : SQL_REWRITE_RULE_PHASE.values()) {
            hepPgmBuilder.addGroupBegin();
            for (ImmutableList<RelOptRule> relOptRuleList : r.getCollectionList()) {
                hepPgmBuilder.addRuleCollection(relOptRuleList);
            }
            for (RelOptRule relOptRule : r.getSingleList()) {
                hepPgmBuilder.addRuleInstance(relOptRule);
            }
            hepPgmBuilder.addGroupEnd();

            if (pre == r) {
                for (RelOptRule relOptRule : preRule) {
                    hepPgmBuilder.addGroupBegin();
                    hepPgmBuilder.addRuleInstance(relOptRule);
                    hepPgmBuilder.addGroupEnd();
                }
                if (extr != null) {
                    hepPgmBuilder.addGroupBegin();
                    hepPgmBuilder.addRuleInstance(extr);
                    hepPgmBuilder.addGroupEnd();
                }
                return hepPgmBuilder;
            }
        }
        return hepPgmBuilder;
    }

    RelNode sqlRewriteAndPlanEnumerate(RelNode input, PlannerContext plannerContext) {
        HepProgramBuilder hepProgramBuilder = getHepProgramBuilder();
        final HepPlanner planner = new HepPlanner(hepProgramBuilder.build(), plannerContext);
        planner.setRoot(input);
        return planner.findBestExp();
    }

    public void setPre(SQL_REWRITE_RULE_PHASE pre) {
        this.pre = pre;
    }

    public void setExtr(RelOptRule extr) {
        this.extr = extr;
    }

    public void clearPreRule() {
        preRule.clear();
    }

    public void addPreRule(RelOptRule preRule) {
        this.preRule.add(preRule);
    }

    public void addPreRule(List<RelOptRule> preRules) {
        this.preRule.addAll(preRules);
    }

    public void setMatchOrder(HepMatchOrder matchOrder) {
        if (null != matchOrder) {
            this.matchOrder = matchOrder;
        }
    }
}

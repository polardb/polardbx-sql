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

package com.alibaba.polardbx.optimizer.core.rel;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

/**
 * Created by yunhan.lyh on 2018/6/28.
 */
public class BushyJoin extends MultiJoin {
    private List<List<RelNode>> finalEquiGroups = null;

    public BushyJoin(
        RelOptCluster cluster,
        List<RelNode> inputs,
        RexNode joinFilter,
        RelDataType rowType,
        boolean isFullOuterJoin,
        List<RexNode> outerJoinConditions,
        List<JoinRelType> joinTypes,
        List<ImmutableBitSet> projFields,
        ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap,
        RexNode postJoinFilter) {
        super(cluster, inputs, joinFilter, rowType, isFullOuterJoin, outerJoinConditions, joinTypes, projFields,
            joinFieldRefCountsMap, postJoinFilter);
    }

    public List<List<RelNode>> getFinalEquiGroups() {
        return this.finalEquiGroups;
    }

    public void setFinalEquiGroups(List<List<RelNode>> finalEquiGroups) {
        this.finalEquiGroups = finalEquiGroups;
    }

    public void setJoinFilter(RexNode joinFilter) {
        this.joinFilter = joinFilter;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new BushyJoin(
            getCluster(),
            inputs,
            joinFilter,
            rowType,
            isFullOuterJoin,
            outerJoinConditions,
            joinTypes,
            projFields,
            joinFieldRefCountsMap,
            postJoinFilter);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode joinFilter = shuttle.apply(this.joinFilter);
        List<RexNode> outerJoinConditions = shuttle.apply(this.outerJoinConditions);
        RexNode postJoinFilter = shuttle.apply(this.postJoinFilter);

        if (joinFilter == this.joinFilter
            && outerJoinConditions == this.outerJoinConditions
            && postJoinFilter == this.postJoinFilter) {
            return this;
        }

        return new BushyJoin(
            getCluster(),
            inputs,
            joinFilter,
            rowType,
            isFullOuterJoin,
            outerJoinConditions,
            joinTypes,
            projFields,
            joinFieldRefCountsMap,
            postJoinFilter);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "BushyJoin");

        final LoptMultiJoin loptMultiJoin = new LoptMultiJoin(this);
        RexExplainVisitor visitor = new RexExplainVisitor(this);
        RexUtil.composeConjunction(
            this.getCluster().getRexBuilder(),
            loptMultiJoin.getJoinFilters(),
            false).accept(visitor);
        return pw.item("condition", visitor.toSqlString())
            .item("type", "inner");
    }
}

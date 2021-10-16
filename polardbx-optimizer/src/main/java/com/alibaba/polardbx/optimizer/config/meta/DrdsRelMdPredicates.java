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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

public class DrdsRelMdPredicates extends RelMdPredicates {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.PREDICATES.method, new DrdsRelMdPredicates());

    public RelOptPredicateList getPredicates(LogicalView rel, RelMetadataQuery mq) {
        return rel.getPredicates(mq);
    }

    @Override
    public RelOptPredicateList getPredicates(SemiJoin join, RelMetadataQuery mq) {
        switch (join.getJoinType()) {
        case INNER:
            // Treat it as left join
            SemiJoin t = join.copy(join.getTraitSet(),
                join.getCondition(),
                join.getLeft(),
                join.getRight(),
                JoinRelType.LEFT,
                join.isSemiJoinDone());
            return super.getPredicates(t, mq);
        case LEFT:
            // Treat it as left join
            return super.getPredicates(join, mq);
        case ANTI:
        case SEMI:
            return super.getPredicates(join, mq);
        }
        throw new AssertionError("unsupported join type " + join.getJoinType());
    }

    public RelOptPredicateList getPredicates(TableLookup rel, RelMetadataQuery mq) {
        if (rel.isRelPushedToPrimary()) {
            return mq.getPulledUpPredicates(rel.getProject());
        } else {
            final RelOptPredicateList pullUps = mq.getPulledUpPredicates(rel.getJoin().getLeft());
            return RelMdPredicates.pullUpPredicates(rel.getProject(), pullUps);
        }
    }

    public RelOptPredicateList getPredicates(ViewPlan rel, RelMetadataQuery mq) {
        return mq.getPulledUpPredicates(rel.getPlan());
    }

    public RelOptPredicateList getPredicates(MysqlTableScan rel, RelMetadataQuery mq) {
        return mq.getPulledUpPredicates(rel.getNodeForMetaQuery());
    }
}

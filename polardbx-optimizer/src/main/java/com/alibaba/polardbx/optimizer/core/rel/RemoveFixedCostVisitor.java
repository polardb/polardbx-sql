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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

/**
 * @author dylan
 */
public class RemoveFixedCostVisitor extends RelShuttleImpl {

    public RemoveFixedCostVisitor() {
    }

    @Override
    public RelNode visit(RelNode other) {
        // other type
        if (other instanceof HashJoin) {
            ((HashJoin) other).setFixedCost(null);
        } else if (other instanceof BKAJoin) {
            ((BKAJoin) other).setFixedCost(null);
        } else if (other instanceof SortMergeJoin) {
            ((SortMergeJoin) other).setFixedCost(null);
        } else if (other instanceof NLJoin) {
            ((NLJoin) other).setFixedCost(null);
        } else if (other instanceof SemiHashJoin) {
            ((SemiHashJoin) other).setFixedCost(null);
        } else if (other instanceof SemiBKAJoin) {
            ((SemiBKAJoin) other).setFixedCost(null);
        } else if (other instanceof SemiSortMergeJoin) {
            ((SemiSortMergeJoin) other).setFixedCost(null);
        } else if (other instanceof SemiNLJoin) {
            ((SemiNLJoin) other).setFixedCost(null);
        } else if (other instanceof MaterializedSemiJoin) {
            ((MaterializedSemiJoin) other).setFixedCost(null);
        } else if (other instanceof HashGroupJoin) {
            ((HashGroupJoin) other).setFixedCost(null);
        } else if (other instanceof SortWindow) {
            ((SortWindow) other).setFixedCost(null);
        }
        return visitChildren(other);
    }
}


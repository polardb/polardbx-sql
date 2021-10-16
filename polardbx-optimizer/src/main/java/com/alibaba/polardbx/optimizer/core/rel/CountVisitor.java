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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;

import java.util.Stack;

public class CountVisitor extends RelShuttleImpl {
    private int joinCount = 0;
    private int maxContinuousInnerJoinCount = 0;
    private Stack<Integer> maxContinuousInnerJoinCountStack = new Stack<>();
    private int outerJoinCount = 0;

    private int semiJoinCount = 0;
    private int sortCount = 0;
    private int limitCount = 0;

    public CountVisitor() {
    }

    @Override
    public RelNode visit(RelNode other) {
        // other type
        if (other instanceof LogicalSemiJoin) {
            return this.visit((LogicalSemiJoin) other);
        } else if (other instanceof MergeSort) {
            if (((MergeSort) other).withLimit()) {
                limitCount++;
            }
            return visitChildren(other);
        } else {
            return visitChildren(other);
        }
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        joinCount++;
        if (join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.RIGHT) {
            outerJoinCount++;
        }
        visitChildren(join);
        assert maxContinuousInnerJoinCountStack.size() >= 0 && maxContinuousInnerJoinCountStack.size() <= 2;
        int joinCount = 0;
        while (maxContinuousInnerJoinCountStack.size() > 0) {
            joinCount += maxContinuousInnerJoinCountStack.pop();
        }
        if (join.getJoinType() == JoinRelType.INNER) {
            joinCount++;
            maxContinuousInnerJoinCountStack.push(joinCount);
            maxContinuousInnerJoinCount = Math.max(joinCount, maxContinuousInnerJoinCount);
        } else {
            maxContinuousInnerJoinCountStack.push(0);
        }
        return join;
    }

    public RelNode visit(LogicalSemiJoin join) {
        joinCount++;
        semiJoinCount++;
        maxContinuousInnerJoinCountStack.clear();
        maxContinuousInnerJoinCountStack.push(0);
        visitChildren(join);
        return join;
    }

    @Override
    public RelNode visit(LogicalSort logicalSort) {
        if (logicalSort.withLimit()) {
            limitCount++;
        }
        sortCount++;
        visitChildren(logicalSort);
        return logicalSort;
    }

    @Override
    public RelNode visit(LogicalTableLookup tableLookup) {
        // we don't consider TableLookup as join here
        super.visit(tableLookup.getInput());
        return tableLookup;
    }

    public int getMaxContinuousInnerJoinCount() {
        return maxContinuousInnerJoinCount;
    }

    public int getJoinCount() {
        return joinCount;
    }

    public int getOuterJoinCount() {
        return outerJoinCount;
    }

    public int getSemiJoinCount() {
        return semiJoinCount;
    }

    public int getSortCount() {
        return sortCount;
    }

    public int getLimitCount() {
        return limitCount;
    }

    public void resetJoinCount() {
        joinCount = 0;
        semiJoinCount = 0;
        outerJoinCount = 0;
        maxContinuousInnerJoinCount = 0;
        sortCount = 0;
        limitCount = 0;
        maxContinuousInnerJoinCountStack.clear();
    }
}

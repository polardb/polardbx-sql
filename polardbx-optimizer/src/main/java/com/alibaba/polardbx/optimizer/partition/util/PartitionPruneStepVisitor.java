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

package com.alibaba.polardbx.optimizer.partition.util;

import com.alibaba.polardbx.optimizer.partition.pruning.PartClauseInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartClauseIntervalInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPredPathInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPredPathItem;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepCombine;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepOp;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;

import java.util.HashSet;
import java.util.Set;

/**
 * @author fangwu
 */
public class PartitionPruneStepVisitor {

    private Set<Integer> dynamicIndexList = new HashSet<>();

    public void visitCombine(PartitionPruneStepCombine combineStep) {
        for (PartitionPruneStep step : combineStep.getSubSteps()) {
            visit(step);
        }
    }

    public void visitOp(PartitionPruneStepOp opStep) {
        if (opStep == null) {
            return;
        }
        PartPredPathInfo partPredPathInfo = opStep.getPartPredPathInfo();

        if (partPredPathInfo != null) {
            PartPredPathItem prefixPathItem = partPredPathInfo.getPrefixPathItem();
            if (prefixPathItem != null) {
                PartClauseIntervalInfo[] partIntervalArr =
                    new PartClauseIntervalInfo[partPredPathInfo.getPartKeyEnd() + 1];
                prefixPathItem.toPartClauseInfoArray(partIntervalArr);
                for (PartClauseIntervalInfo partClauseIntervalInfo : partIntervalArr) {
                    if (partClauseIntervalInfo != null) {
                        PartClauseInfo partClause = partClauseIntervalInfo.getPartClause();
                        if (partClause != null) {
                            RexNode rexNode = partClause.getConstExpr();
                            if (rexNode instanceof RexDynamicParam) {
                                RexDynamicParam param = (RexDynamicParam) rexNode;
                                if (param.getIndex() >= 0) {
                                    dynamicIndexList.add(param.getIndex() + 1);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void visit(PartitionPruneStep step) {
        if (step == null) {
            return;
        }
        if (step instanceof PartitionPruneStepCombine) {
            visitCombine((PartitionPruneStepCombine) step);
        } else if (step instanceof PartitionPruneStepOp) {
            visitOp((PartitionPruneStepOp) step);
        } else {
            throw new UnsupportedOperationException(" partition prune step : " + step.getStepDigest());
        }
    }

    public Set<Integer> getDynamicIndexList() {
        return dynamicIndexList;
    }
}

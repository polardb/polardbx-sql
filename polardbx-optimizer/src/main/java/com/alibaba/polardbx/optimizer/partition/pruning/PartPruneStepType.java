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

package com.alibaba.polardbx.optimizer.partition.pruning;

/**
 * @author chenghui.lch
 */
public enum PartPruneStepType {

    // Type for PartitionPruneStepOp
    PARTPRUNE_OP_MATCHED_PART_KEY("PART_OP"),
    PARTPRUNE_OP_MISMATCHED_PART_KEY("NO_PART_OP"),

    // Type for PartitionPruneStepCombine
    PARTPRUNE_COMBINE_UNION("OR"),
    PARTPRUNE_COMBINE_INTERSECT("AND");

    private String symbol;

    PartPruneStepType(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }
    
    public static boolean isStepCombine( PartPruneStepType stepType ) {
        return stepType == PartPruneStepType.PARTPRUNE_COMBINE_UNION || stepType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT;
    }
}

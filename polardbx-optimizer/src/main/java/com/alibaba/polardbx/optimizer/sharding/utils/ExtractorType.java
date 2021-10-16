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

package com.alibaba.polardbx.optimizer.sharding.utils;

import java.util.EnumSet;

/**
 * @author chenmo.cm
 */
public enum ExtractorType {
    /**
     * collection condition for partition pruning
     */
    PARTITIONING_CONDITION,
    /**
     * collection condition for predicate move-around
     */
    PREDICATE_MOVE_AROUND,
    /**
     * collection condition for join pushdown
     */
    COLUMN_EQUIVALENCE;

    private static EnumSet<ExtractorType> columnEquivalenceOnly = EnumSet.of(ExtractorType.COLUMN_EQUIVALENCE);
    private static EnumSet<ExtractorType> useOuterJoinCondition = EnumSet.of(ExtractorType.COLUMN_EQUIVALENCE,
        ExtractorType.PARTITIONING_CONDITION);

    public boolean columnEquivalenceOnly() {
        return ExtractorType.columnEquivalenceOnly.contains(this);
    }

    public boolean useOuterJoinCondition() {
        return ExtractorType.useOuterJoinCondition.contains(this);
    }
}

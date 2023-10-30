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

package com.alibaba.polardbx.optimizer.core.join;

import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LookupPredicateBuilder {

    private final Join join;

    private final LookupPredicate predicate;

    public LookupPredicateBuilder(Join join, List<String> lvOriginNames) {
        this.join = join;
        boolean notIn = (join.getJoinType() == JoinRelType.ANTI);
        this.predicate = new LookupPredicate(notIn, lvOriginNames);
    }

    public LookupPredicate build(List<LookupEquiJoinKey> joinKeys) {
        Set<Integer> lookupColumnSet = new HashSet<>();
        for (LookupEquiJoinKey key : joinKeys) {
            if (key.isNullSafeEqual()) {
                RelDataType relDataTypeInner =
                    join.getInner().getRowType().getFieldList().get(key.getInnerIndex()).getType();
                RelDataType relDataTypeOuter =
                    join.getOuter().getRowType().getFieldList().get(key.getOuterIndex()).getType();
                if (relDataTypeInner.isNullable() || relDataTypeOuter.isNullable()) {
                    continue; // '<=>' semantics can not be represented as IN expression unless both join key are not nullable
                }
            }
            if (!key.isCanFindOriginalColumn()) {
                continue; // can not be represented as IN expression, because column origin is null
            }
            int targetIndex;
            if (join instanceof MaterializedSemiJoin) {
                // lookup on outer side
                if (!lookupColumnSet.add(key.getOuterIndex())) {
                    continue;
                }
                String column = getJoinKeyColumnName(key);
                targetIndex = key.getInnerIndex();
                predicate.addEqualPredicate(
                    new SqlIdentifier(column, SqlParserPos.ZERO), targetIndex, key.getUnifiedType());
            } else {
                // lookup on inner side
                if (!lookupColumnSet.add(key.getInnerIndex())) {
                    continue;
                }
                String column = getJoinKeyColumnName(key);
                targetIndex = key.getOuterIndex();
                predicate.addEqualPredicate(
                    new SqlIdentifier(column, SqlParserPos.ZERO), targetIndex, key.getUnifiedType());
            }
        }
        return predicate;
    }

    public static String getJoinKeyColumnName(LookupEquiJoinKey joinKey) {
        return joinKey.getLookupColunmnName();
    }
}

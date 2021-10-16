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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LookupPredicateBuilder {

    private final Join join;
    private final RelNode leftNode;
    private final RelNode rightNode;
    private final JoinRelType joinType;

    private final LookupPredicate predicate;

    public LookupPredicateBuilder(Join join) {
        this.join = join;
        this.leftNode = join.getLeft();
        this.rightNode = join.getRight();
        this.joinType = join.getJoinType();

        boolean notIn = (join.getJoinType() == JoinRelType.ANTI);
        this.predicate = new LookupPredicate(notIn);
    }

    public LookupPredicate build(List<EquiJoinKey> joinKeys) {
        Set<Integer> lookupColumnSet = new HashSet<>();
        for (EquiJoinKey key : joinKeys) {
            if (key.isNullSafeEqual()) {
                continue; // '<=>' semantics can not be represented as IN expression
            }
            if (!key.isCanFindOriginalColumn()) {
                continue; // can not be represented as IN expression, because column origin is null
            }
            SqlIdentifier column;
            int targetIndex;
            if (join instanceof MaterializedSemiJoin) {
                // lookup on outer side
                if (!lookupColumnSet.add(key.getOuterIndex())) {
                    continue;
                }
                column = getIdentifierByIndex(leftNode, key.getOuterIndex());
                targetIndex = key.getInnerIndex();
            } else {
                // lookup on inner side
                if (!lookupColumnSet.add(key.getInnerIndex())) {
                    continue;
                }
                column = getIdentifierByIndex(joinType.innerSide(leftNode, rightNode), key.getInnerIndex());
                targetIndex = key.getOuterIndex();
            }
            predicate.addEqualPredicate(column, targetIndex, key.getUnifiedType());
        }
        return predicate;
    }

    public static SqlIdentifier getIdentifierByIndex(RelNode relNode, int index) {
        RelColumnOrigin relColumnOrigin;
        RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
        synchronized (mq) {
            //这里会被多个线程同时调用，容易抛出CyclicMetadataException 风险
            relColumnOrigin = mq.getColumnOrigin(relNode, index);
        }
        if (relColumnOrigin != null) {
            String name = relColumnOrigin.getOriginTable().getRowType().getFieldNames()
                .get(relColumnOrigin.getOriginColumnOrdinal());
            return new SqlIdentifier(name, SqlParserPos.ZERO);
        }
        return new SqlIdentifier(relNode.getRowType().getFieldNames().get(index), SqlParserPos.ZERO);
    }

}

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

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class EquiJoinUtils {

    public static List<EquiJoinKey> buildEquiJoinKeys(Join join, RelNode outer, RelNode inner,
                                                      RexCall condition, JoinRelType joinType) {
        return createEquiJoinKeys(false, join, outer, inner, condition, joinType, false);
    }

    public static List<LookupEquiJoinKey> buildLookupEquiJoinKeys(Join join, RelNode outer, RelNode inner,
                                                                  RexCall condition, JoinRelType joinType,
                                                                  boolean includeNullSafeEqual) {
        List<EquiJoinKey> equiJoinKeys = createEquiJoinKeys(
            true, join, outer, inner, condition, joinType, includeNullSafeEqual);

        return equiJoinKeys.stream().map(x -> (LookupEquiJoinKey) x).collect(Collectors.toList());
    }

    public static List<EquiJoinKey> buildEquiJoinKeys(Join join, RelNode outer, RelNode inner,
                                                      RexCall condition, JoinRelType joinType,
                                                      boolean includeNullSafeEqual) {
        return createEquiJoinKeys(
            false, join, outer, inner, condition, joinType, includeNullSafeEqual);
    }

    public static List<EquiJoinKey> createEquiJoinKeys(boolean isLookupView, Join join, RelNode outer, RelNode inner,
                                                       RexCall condition, JoinRelType joinType,
                                                       boolean includeNullSafeEqual) {
        final SqlOperator operator = condition.getOperator();
        final List<RexNode> operands = condition.getOperands();

        // Only accept equal conditions or their conjunctions
        if (operator == TddlOperatorTable.EQUALS
            || operator == TddlOperatorTable.NULL_SAFE_EQUAL
            || operator == TddlOperatorTable.IS_NOT_DISTINCT_FROM) {
            boolean nullSafeEqual = (operator != TddlOperatorTable.EQUALS);
            if (!includeNullSafeEqual && nullSafeEqual) {
                return Collections.emptyList();
            }
            if (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef) {
                assert operands.get(0) instanceof RexInputRef : "expect RexInputRef for first operand";
                assert operands.get(1) instanceof RexInputRef : "expect RexInputRef for second operand";

                int firstIndex = ((RexInputRef) operands.get(0)).getIndex();
                int secondIndex = ((RexInputRef) operands.get(1)).getIndex();

                int offset = joinType.leftSide(outer, inner).getRowType().getFieldCount();
                final boolean indexOp1IsLeft = firstIndex < offset;
                final boolean indexOp2IsRight = secondIndex < offset;
                if (indexOp1IsLeft == indexOp2IsRight) {
                    return new ArrayList<>();
                }
                int leftIndex = Math.min(firstIndex, secondIndex);
                final int rightIndexWithoutOffset = Math.max(firstIndex, secondIndex);
                int rightIndex = rightIndexWithoutOffset - offset;
                int outerIndex = joinType.outerSide(leftIndex, rightIndex);
                int innerIndex = joinType.innerSide(leftIndex, rightIndex);

                RelDataType innerType = inner.getRowType().getFieldList().get(innerIndex).getType();
                RelDataType outerType = outer.getRowType().getFieldList().get(outerIndex).getType();
                DataType unifiedType = CalciteUtils.getUnifiedDataType(innerType, outerType);

                RelMetadataQuery mq = join.getCluster().getMetadataQuery();

                // check lookupGsi
                // check Materialized SemiJoin
                for (RelNode lookupSide : Arrays.asList(outer, inner)) {
                    if (isLookupGsiSide(lookupSide)) {
                        RelColumnOrigin relColumnOrigin = null;
                        synchronized (mq) {
                            relColumnOrigin =
                                mq.getColumnOrigin(lookupSide, lookupSide == inner ? innerIndex : outerIndex);
                        }
                        if (relColumnOrigin == null) {
                            return new ArrayList<>();
                        }
                        RelOptTable relOptTable = relColumnOrigin.getOriginTable();
                        boolean isGsi = CBOUtil.getTableMeta(relOptTable).isGsi();
                        if (!isGsi) {
                            return new ArrayList<>();
                        }
                    }
                }

                if (isLookupView) {
                    final RelColumnOrigin columnOrigin;
                    String lookupColumnName = null;
                    synchronized (mq) {
                        if (join instanceof MaterializedSemiJoin || joinType == JoinRelType.RIGHT) {
                            columnOrigin = mq.getColumnOrigin(join.getLeft(), leftIndex);
                            if (columnOrigin != null) {
                                lookupColumnName = columnOrigin.getOriginTable().getRowType().getFieldNames()
                                    .get(columnOrigin.getOriginColumnOrdinal());
                            } else {
                                lookupColumnName = join.getLeft().getRowType().getFieldNames().get(leftIndex);
                            }
                        } else {
                            columnOrigin = mq.getColumnOrigin(join.getRight(), rightIndex);
                            if (columnOrigin != null) {
                                lookupColumnName = columnOrigin.getOriginTable().getRowType().getFieldNames()
                                    .get(columnOrigin.getOriginColumnOrdinal());
                            } else {
                                lookupColumnName = join.getRight().getRowType().getFieldNames().get(rightIndex);
                            }
                        }
                    }

                    return Collections.singletonList(
                        new LookupEquiJoinKey(outerIndex, innerIndex, unifiedType, nullSafeEqual,
                            columnOrigin != null, lookupColumnName));
                } else {
                    return Collections.singletonList(
                        new EquiJoinKey(outerIndex, innerIndex, unifiedType, nullSafeEqual));
                }
            } else {
                return new ArrayList<>();
            }
        } else if (operator == TddlOperatorTable.AND) {
            return operands.stream()
                .filter(op -> op instanceof RexCall)
                .flatMap(
                    op -> createEquiJoinKeys(isLookupView,
                        join, outer, inner, (RexCall) op, joinType, includeNullSafeEqual).stream())
                .collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * only support fully before and after expand
     * not work for (RelSubset, HepRelVertex)
     */
    private static boolean isLookupGsiSide(RelNode relNode) {
        if (relNode instanceof LogicalTableLookup) {
            // before expand
            RelNode input = ((LogicalTableLookup) relNode).getInput();
            if (input instanceof Gather) {
                input = ((Gather) input).getInput();
            }
            if (input instanceof LogicalIndexScan) {
                return ((LogicalIndexScan) input).getJoin() != null;
            }
        } else if (relNode instanceof LogicalProject) {
            // after expand
            RelNode input = ((LogicalProject) relNode).getInput();
            if (input instanceof BKAJoin) {
                BKAJoin bkaJoin = (BKAJoin) input;
                if (bkaJoin.getJoinType() == JoinRelType.INNER) {
                    RelNode bkaJoinLeftInput = bkaJoin.getLeft();
                    if (bkaJoinLeftInput instanceof Gather) {
                        bkaJoinLeftInput = ((Gather) bkaJoinLeftInput).getInput();
                    }
                    if (bkaJoinLeftInput instanceof LogicalIndexScan) {
                        return ((LogicalIndexScan) bkaJoinLeftInput).getJoin() != null;
                    }
                }
            }
        }
        return false;
    }

    public static List<EquiJoinKey> buildEquiJoinKeys(RelNode outer, RelNode inner,
                                                      List<Integer> outerIndexes, List<Integer> innerIndexes) {
        final int numKeys = outerIndexes.size();

        List<EquiJoinKey> results = new ArrayList<>(numKeys);
        for (int i = 0; i < numKeys; i++) {
            int outerIndex = outerIndexes.get(i);
            int innerIndex = innerIndexes.get(i);

            RelDataType outerType = outer.getRowType().getFieldList().get(outerIndex).getType();
            RelDataType innerType = inner.getRowType().getFieldList().get(innerIndex).getType();
            DataType unifiedType = CalciteUtils.getUnifiedDataType(outerType, innerType);

            results.add(new EquiJoinKey(
                outerIndex, innerIndex, unifiedType, false));
        }

        return results;
    }
}

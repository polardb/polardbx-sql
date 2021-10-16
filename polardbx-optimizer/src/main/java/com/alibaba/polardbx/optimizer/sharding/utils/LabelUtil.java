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

import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author chenmo.cm
 */
public class LabelUtil {

    public static Mapping identityColumnMapping(int fieldCount) {
        return Mappings.target(Mappings.createIdentity(fieldCount), fieldCount, fieldCount);
    }

    public static Mapping columnMapping(Map<Integer, Integer> mapping, int srcFieldCount, int dstFieldCount) {
        return (Mapping) Mappings.target(mapping, srcFieldCount, dstFieldCount);
    }

    public static RelDataType concatRowTypes(List<Label> inputs, RelDataTypeFactory typeFactory) {
        RelDataType result = inputs.get(0).getFullRowType().fullRowType;
        for (int i = 1; i < inputs.size(); i++) {
            // Do not care nullability and name, just concatenate fields
            result = PlannerUtils.deriveJoinType(typeFactory, result, inputs.get(i).getFullRowType().fullRowType);
        }

        return result;
    }

    /**
     * Multiply top and bottom mapping, skip src which maps to a negative target
     */
    public static void multiplyAndShiftMap(Builder<Integer, Integer> result, ImmutableBitSet.Builder srcWithTarget,
                                           Mapping topMapping, Mapping bottomMapping, int topShift, int bottomShift) {
        for (int src = 0; src < topMapping.getSourceCount(); src++) {
            int target = topMapping.getTargetOpt(src);
            // if target is negative, use target from top mapping
            if (target >= 0) {
                final int bottomTarget = bottomMapping.getTargetOpt(target);
                // shift target
                target = bottomTarget >= 0 ? bottomShift + bottomTarget : bottomTarget;
            }

            // shift src
            final int finalSrc = topShift + src;
            result.put(finalSrc, target);

            if (null != srcWithTarget && target >= 0) {
                srcWithTarget.set(finalSrc);
            }
        }
    }

    /**
     * Permute fields according to fieldMapping, choose bottom field if possible
     */
    public static List<RelDataTypeField> permuteColumns(Mapping fieldMapping, List<RelDataTypeField> topFields,
                                                        List<RelDataTypeField> bottomFields) {
        List<RelDataTypeField> fields = new ArrayList<>(fieldMapping.getSourceCount());

        for (int i = 0; i < fieldMapping.getSourceCount(); i++) {
            final int inputRef = fieldMapping.getTargetOpt(i);
            if (inputRef < 0) {
                // column generate by project or aggregation
                RelDataTypeField origin = topFields.get(i);
                fields.add(new RelDataTypeFieldImpl(origin.getName(), i, origin.getType()));
            } else {
                // get real column name from table
                RelDataTypeField origin = bottomFields.get(inputRef);
                fields.add(new RelDataTypeFieldImpl(origin.getName(), i, origin.getType()));
            }
        }
        return fields;
    }

    /**
     * @return reference index, -1 if rexNode is not a input reference
     */
    public static int refIndexOpt(RexNode rexNode) {
        RexNode tmpRef = rexNode;
        if (tmpRef.getKind() == SqlKind.CAST) {
            tmpRef = ((RexCall) tmpRef).operands.get(0);
        }

        // only care about column ref without additional calculation
        if (tmpRef instanceof RexInputRef) {
            return ((RexInputRef) tmpRef).getIndex();
        }

        return -1;
    }

    /**
     * Return the inverse mapping of {@link Project#getPartialMapping(int, List)}
     * and collection RexNode other than RexInputRef or CAST
     */
    public static Mapping getInversePartialMapping(Project project, List<Pair<Integer, RexNode>> columnRexMap) {
        final List<RexNode> projects = project.getProjects();
        final int targetCount = project.getInput().getRowType().getFieldCount();

        final Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, projects.size(), targetCount);

        for (Ord<? extends RexNode> exp : Ord.zip(projects)) {
            int ref = refIndexOpt(exp.e);

            if (ref >= 0) {
                mapping.set(exp.i, ref);
            } else if (null != columnRexMap) {
                mapping.set(exp.i, -1);
                columnRexMap.add(Pair.of(exp.i, exp.e));
            }
        }

        return mapping;
    }

    /**
     * Return a inverse partial function which mapping columns from the output of
     * {@code window} to the input of {@code window}
     */
    public static Mapping getInversePartialMapping(Window window) {
        final int sourceCount = window.getRowType().getFieldCount();
        final int targetCount = window.getInput().getRowType().getFieldCount();

        final Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, sourceCount, targetCount);

        for (int i = 0; i < sourceCount; i++) {
            if (i < targetCount) {
                mapping.set(i, i);
            } else {
                mapping.set(i, -1);
            }
        }

        return mapping;
    }

    /**
     * Return a inverse partial function which mapping columns from the output of
     * {@code logicalExpand} to the input of {@code LogicalExpand}
     */
    public static Mapping getInversePartialMapping(LogicalExpand logicalExpand) {
        final int sourceCount = logicalExpand.getRowType().getFieldCount();
        final int targetCount = logicalExpand.getInput().getRowType().getFieldCount();

        final Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, sourceCount, targetCount);

        for (int i = 0; i < sourceCount; i++) {
            if (i < targetCount) {
                mapping.set(i, i);
            } else {
                mapping.set(i, -1);
            }
        }

        return mapping;
    }

    /**
     * Return a inverse partial function which mapping columns from the output of
     * {@code agg} to the input of {@code agg}
     */
    public static Mapping getInversePartialMapping(Aggregate agg, List<Pair<Integer, AggregateCall>> newColumnAggCall) {
        final int sourceCount = agg.getRowType().getFieldCount();
        final int targetCount = agg.getInput().getRowType().getFieldCount();
        final List<Integer> groupList = agg.getGroupSet().asList();
        final int groupCount = agg.getGroupCount();
        final int aggCallOffset = agg.getIndicatorCount() + groupCount;
        final List<AggregateCall> aggCallList = agg.getAggCallList();

        final Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, sourceCount, targetCount);
        for (int i = 0; i < sourceCount; i++) {
            if (i < groupCount) {
                // grouping column
                mapping.set(i, groupList.get(i));
            } else if (i >= aggCallOffset) {
                // aggregate call
                mapping.set(i, -1);
                newColumnAggCall.add(Pair.of(i, aggCallList.get(i - aggCallOffset)));
            }

        }

        return mapping;
    }

    /**
     * Return a inverse partial function which mapping columns from the output of
     * {@code agg} to the input of {@code agg}
     */
    public static Mapping getInversePartialMapping(HashGroupJoin agg,
                                                   List<Pair<Integer, AggregateCall>> newColumnAggCall) {
        final int sourceCount = agg.getRowType().getFieldCount();
        final int targetCount = agg.getJoinRowType().getFieldCount();
        final List<Integer> groupList = agg.getGroupSet().asList();
        final int groupCount = agg.getGroupSet().cardinality();
        final int aggCallOffset = agg.getIndicatorCount() + groupCount;
        final List<AggregateCall> aggCallList = agg.getAggCallList();

        final Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, sourceCount, targetCount);
        for (int i = 0; i < sourceCount; i++) {
            if (i < groupCount) {
                // grouping column
                mapping.set(i, groupList.get(i));
            } else if (i >= aggCallOffset) {
                // aggregate call
                mapping.set(i, -1);
                newColumnAggCall.add(Pair.of(i, aggCallList.get(i - aggCallOffset)));
            }

        }

        return mapping;
    }

    /**
     * Build mapping for projects and multiply with current mapping.
     * <p>
     * {@code multiply(A, B)} returns a mapping C such that C is equivalent to A * B
     * (the mapping A followed by the mapping B).
     *
     * @return Mapping mapping3 such that mapping3 = mapping1 * mapping2
     */
    public static Mapping multiply(List<RexNode> projects, Mapping columnMapping) {
        final Mapping mapping = Mappings
            .create(MappingType.PARTIAL_FUNCTION, projects.size(), columnMapping.getTargetCount());

        for (Ord<? extends RexNode> exp : Ord.zip(projects)) {
            int ref = refIndexOpt(exp.e);

            if (ref >= 0) {
                final int target = columnMapping.getTargetOpt(ref);
                if (target >= 0) {
                    mapping.set(exp.i, target);
                }
            }
        }
        return mapping;
    }

    /**
     * Build mapping for projects and multiply with current mapping.
     * <p>
     * {@code multiply(A, B)} returns a mapping C such that C is equivalent to A * B
     * (the mapping A followed by the mapping B).
     *
     * @return Mapping mapping3 such that mapping3 = mapping1 * mapping2
     */
    public static Mapping multiply(Mapping mapping1, Mapping mapping2) {
        final Mapping mapping = Mappings
            .create(MappingType.PARTIAL_FUNCTION, mapping1.getSourceCount(), mapping2.getTargetCount());

        for (int source = 0; source < mapping1.getSourceCount(); source++) {
            int x = mapping1.getTargetOpt(source);

            if (x >= 0) {
                final int target = mapping2.getTargetOpt(x);
                if (target >= 0) {
                    mapping.set(source, target);
                }
            }
        }
        return mapping;
    }

    /**
     * Returns the source array by inverse partial function and target array.
     *
     * @param mapping Mapping with type {@link MappingType#INVERSE_PARTIAL_FUNCTION}
     * @param targetArray Target array
     * @param <T> Element type
     * @return Array with elements permuted according to mapping
     */
    public static <T> T[] inverseApply(Mapping mapping, T[] targetArray, Class<T> clazz) {
        final T[] array2 = (T[]) Array.newInstance(clazz, mapping.getSourceCount());
        for (int source = 0; source < mapping.getSourceCount(); source++) {
            int ref = mapping.getTargetOpt(source);
            if (ref >= 0) {
                array2[source] = targetArray[ref];
            }
        }
        return array2;
    }

    /**
     * Converts a predicate on a particular set of columns into a predicate on
     * a subset of those columns, weakening if necessary.
     *
     * @param builder Rex builder
     * @param columnMapping Columns which the final predicate can reference
     * @param node Predicate expression
     * @return Predicate expression narrowed to reference only certain columns
     */
    public static RexNode apply(RexBuilder builder, Mapping columnMapping, RexNode node) {
        final RexRebaseShuttle rebaseShuttle = new RexRebaseShuttle(builder, columnMapping);
        return node.accept(rebaseShuttle);
    }

    public static boolean isPermutation(Project project) {
        return !isNotPermutation(project);
    }

    public static boolean isNotPermutation(Project project) {
        return project.getProjects().stream().anyMatch(rex -> refIndexOpt(rex) < 0);
    }

    public static boolean withAggCall(Aggregate agg) {
        return !withoutAggCall(agg);
    }

    public static boolean withoutAggCall(Aggregate agg) {
        return GeneralUtil.isEmpty(agg.getAggCallList());
    }

    public static String joinNullableString(String delimiter, String... operands) {
        return Stream.of(operands).filter(s -> null != s && !s.isEmpty()).collect(Collectors.joining(delimiter));
    }

    public static RelDataType concatApplyRowTypes(List<Label> inputs, RelDataTypeFactory typeFactory,
                                                  RelDataTypeField relDataTypeField) {
        RelDataType result = inputs.get(0).getFullRowType().fullRowType;
        for (int i = 1; i < inputs.size(); i++) {
            if (i == 1) {
                result = PlannerUtils.deriveJoinType(typeFactory, result, typeFactory.createStructType(
                    Lists.newArrayList(relDataTypeField)));
            }
            // Do not care nullability and name, just concatenate fields
            result = PlannerUtils.deriveJoinType(typeFactory, result, inputs.get(i).getFullRowType().fullRowType);
        }

        return result;
    }

    private static class RexRebaseShuttle extends RexPermuteInputsShuttle {

        private final RexBuilder rexBuilder;

        private RexRebaseShuttle(RexBuilder builder, Mapping mapping, RelNode... inputs) {
            super(mapping, inputs);
            this.rexBuilder = builder;
        }

        private RexRebaseShuttle(RexBuilder builder, Mappings.TargetMapping mapping,
                                 ImmutableList<RelDataTypeField> fields) {
            super(mapping, fields);
            this.rexBuilder = builder;
        }

        /**
         * Creates a shuttle with an empty field list. It cannot handle GET calls but
         * otherwise works OK.
         */
        public static RexRebaseShuttle of(RexBuilder builder, Mappings.TargetMapping mapping) {
            return new RexRebaseShuttle(builder, mapping, ImmutableList.of());
        }

        @Override
        public RexNode visitInputRef(RexInputRef local) {
            final int index = local.getIndex();
            int target = mapping.getTargetOpt(index);

            if (target < 0) {
                return null;
            }

            return new RexInputRef(target, local.getType());
        }

        @Override
        public RexNode visitCall(RexCall call) {
            switch (call.getKind()) {
            case OR:
                return visitAndOr(call, true);
            case AND:
                return visitAndOr(call, false);
            default:
                break;
            }

            if (call.getOperator() == RexBuilder.GET_OPERATOR) {
                throw new AssertionError("Do not support GET_OPERATOR");
            }

            final boolean[] update = {false};
            final List<RexNode> clonedOperands = visitList(call.operands, update);
            if (update[0]) {
                return Optional.ofNullable(clonedOperands)
                    .filter(o -> !o.isEmpty())
                    .map(ops -> rexBuilder.makeCall(call.getType(), call.getOperator(), ops))
                    .orElse(null);
            } else {
                return call;
            }
        }

        protected RexNode visitAndOr(RexCall call, boolean operandOfOr) {
            final boolean[] update = {false};

            List<RexNode> clonedOperands = visitList(call.operands, update, operandOfOr);
            if (update[0]) {
                return Optional.ofNullable(clonedOperands)
                    .filter(o -> !o.isEmpty())
                    .filter(o -> !(!operandOfOr && o.stream().anyMatch(RexNode::isAlwaysFalse)))
                    .filter(o -> !(operandOfOr && o.stream().anyMatch(RexNode::isAlwaysTrue)))
                    .map(ops -> ops.size() == 1 ? ops.get(0) :
                        rexBuilder.makeCall(call.getType(), call.getOperator(), ops))
                    .orElse(null);
            } else {
                return call;
            }
        }

        protected List<RexNode> visitList(List<? extends RexNode> exprs, boolean[] update, boolean isOr) {
            ImmutableList.Builder<RexNode> clonedOperands = ImmutableList.builder();
            for (RexNode operand : exprs) {
                RexNode clonedOperand = operand.accept(this);
                if ((clonedOperand != operand) && (update != null)) {
                    update[0] = true;
                }
                // ignore null operand of AND
                if (null != clonedOperand) {
                    clonedOperands.add(clonedOperand);
                } else if (isOr) {
                    // weaken null operand of OR
                    clonedOperands.add(rexBuilder.makeLiteral(true));
                }
            }

            return clonedOperands.build();
        }

        @Override
        protected List<RexNode> visitList(List<? extends RexNode> exprs, boolean[] update) {
            ImmutableList.Builder<RexNode> clonedOperands = ImmutableList.builder();
            for (RexNode operand : exprs) {
                RexNode clonedOperand = operand.accept(this);
                if ((clonedOperand != operand) && (update != null)) {
                    update[0] = true;
                }
                if (null == clonedOperand) {
                    return null;
                }
                clonedOperands.add(clonedOperand);
            }

            return clonedOperands.build();
        }

        @Override
        public RexNode visitOver(RexOver over) {
            throw new AssertionError("Do not support RexOver");
        }

        @Override
        public RexWindow visitWindow(RexWindow window) {
            throw new AssertionError("Do not support RexWindow");
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            if (subQuery.getOperator().getKind() == SqlKind.SCALAR_QUERY) {
                return null;
            }
            throw new AssertionError("Do not support RexSubQuery");
        }

    }
}

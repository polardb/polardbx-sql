/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.rel.dml.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.DynamicImplicitDefaultHandlerCall;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.ReplaceRexWithParamHandlerCall;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.ReplaceWithScaleHandlerCall;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public final class RexHandlerCallFactory {

    public static DynamicImplicitDefaultHandlerCall createDynamicImplicitDefaultHandlerCall(
        RexNode rex,
        boolean mustBeLiteral,
        boolean mustBeDeterministic,
        boolean withImplicitDefault,
        boolean withDynamicImplicitDefault,
        boolean computeAllDynamicImplicitDefault,
        RexNode defaultRex,
        RexBuilder rexBuilder,
        int rexIndex,
        RelNode baseRel) {
        return createDynamicImplicitDefaultHandlerCall(new BaseRexHandlerCall(rex), mustBeLiteral, mustBeDeterministic,
            withImplicitDefault, withDynamicImplicitDefault, computeAllDynamicImplicitDefault, defaultRex, rexBuilder,
            rexIndex, baseRel);
    }

    public static DynamicImplicitDefaultHandlerCall createDynamicImplicitDefaultHandlerCall(
        RexHandlerCall baseHandlerCall,
        boolean mustBeLiteral,
        boolean mustBeDeterministic,
        boolean withImplicitDefault,
        boolean withDynamicImplicitDefault,
        boolean computeAllDynamicImplicitDefault,
        RexNode defaultRex,
        RexBuilder rexBuilder,
        int rexIndex,
        RelNode baseRel) {
        // Check null
        final RelUtils.NullRefFinder nullRefFinder = new RelUtils.NullRefFinder(rexIndex);
        nullRefFinder.go(baseRel);

        return new DynamicImplicitDefaultHandlerCall(baseHandlerCall,
            mustBeLiteral,
            mustBeDeterministic,
            withImplicitDefault,
            withDynamicImplicitDefault,
            computeAllDynamicImplicitDefault,
            defaultRex,
            rexBuilder,
            nullRefFinder.getNullableState());
    }

    public static DynamicImplicitDefaultHandlerCall createDynamicImplicitDefaultHandlerCall(
        RexNode rex,
        boolean mustBeLiteral,
        boolean mustBeDeterministic,
        boolean withImplicitDefault,
        boolean withDynamicImplicitDefault,
        boolean computeAllDynamicImplicitDefault,
        RexNode defaultRex,
        RexBuilder rexBuilder,
        RexUtils.NullableState nullableState) {
        return new DynamicImplicitDefaultHandlerCall(new BaseRexHandlerCall(rex),
            mustBeLiteral,
            mustBeDeterministic,
            withImplicitDefault,
            withDynamicImplicitDefault,
            computeAllDynamicImplicitDefault,
            defaultRex,
            rexBuilder,
            nullableState);
    }

    public static RexHandlerCall createReplaceWithScaleHandlerCall(RexHandlerCall baseHandlerCall, RexNode defaultRex,
                                                                   boolean computeAllDynamicImplicitDefault,
                                                                   boolean replaceExplicitRexCallWithComputedDynamicImplicitDefault,
                                                                   RexBuilder rexBuilder) {
        if (replaceExplicitRexCallWithComputedDynamicImplicitDefault && computeAllDynamicImplicitDefault) {
            final RexNode rex = baseHandlerCall.getRex();

            if (defaultRex instanceof RexCallParam && rex instanceof RexCall) {
                final RexNode maxScaleRex = ((RexCallParam) defaultRex).getRexCall();

                if (maxScaleRex instanceof RexCall && ((RexCall) maxScaleRex).getOperator().getName()
                    .equalsIgnoreCase(((RexCall) rex).getOperator().getName())) {

                    // For now, handle CURRENT_TIMESTAMP() ONLY
                    final Long maxScale = RexUtils.currentTimestampScaleFinder(maxScaleRex);

                    if (maxScale >= 0) {
                        baseHandlerCall = new ReplaceWithScaleHandlerCall(baseHandlerCall,
                            defaultRex,
                            maxScale,
                            RexUtils::currentTimestampScaleFinder,
                            rexBuilder,
                            // For now, handle CURRENT_TIMESTAMP() ONLY
                            (r, c) -> c.scaleFinder.apply(r) >= 0);
                    }
                }
            }
        }
        return baseHandlerCall;
    }

    private static @NotNull DynamicImplicitDefaultHandlerCall createDynamicImplicitDefaultHandlerCall(RexNode rex,
                                                                                                      boolean mustBeLiteral,
                                                                                                      boolean mustBeDeterministic,
                                                                                                      boolean withImplicitDefault,
                                                                                                      boolean withDynamicImplicitDefault,
                                                                                                      boolean computeAllDynamicImplicitDefault,
                                                                                                      RexNode defaultRex,
                                                                                                      RexBuilder rexBuilder,
                                                                                                      boolean replaceExplicitRexCallWithComputedDynamicImplicitDefault) {
        final RexHandlerCall baseHandlerCall =
            createReplaceWithScaleHandlerCall(new BaseRexHandlerCall(rex), defaultRex, computeAllDynamicImplicitDefault,
                replaceExplicitRexCallWithComputedDynamicImplicitDefault, rexBuilder);

        return new DynamicImplicitDefaultHandlerCall(baseHandlerCall,
            mustBeLiteral,
            mustBeDeterministic,
            withImplicitDefault,
            withDynamicImplicitDefault,
            computeAllDynamicImplicitDefault,
            defaultRex,
            rexBuilder,
            // determine nullable state at runtime
            null);
    }

    @Getter
    public static class DynamicImplicitDefaultHandlerCallBuilder {
        protected final LogicalInsert insert;
        protected final ExecutionContext ec;

        protected final Set<Integer> literalColumnIndex;
        protected final Set<Integer> deterministicColumnIndex;

        protected final Map<Integer, RexNode> implicitDefaultColumnIndex;
        protected final Map<Integer, RexNode> dynamicImplicitDefaultColumnIndex;

        // If any of dynamic implicit default column must be computed in CN, make it all
        protected final boolean computeAllDynamicImplicitDefault;

        protected final boolean handleOnDuplicateKeyUpdate;

        public DynamicImplicitDefaultHandlerCallBuilder(LogicalInsert insert, ExecutionContext ec) {
            this(insert, ec, false);
        }

        public DynamicImplicitDefaultHandlerCallBuilder(LogicalInsert insert, ExecutionContext ec,
                                                        boolean handleOnDuplicateKeyUpdate) {
            this.insert = Preconditions.checkNotNull(insert);
            this.ec = Preconditions.checkNotNull(ec);

            this.literalColumnIndex = new HashSet<>(insert.getLiteralColumnIndex());
            this.deterministicColumnIndex = new HashSet<>(insert.getDeterministicColumnIndex(ec));

            final boolean replaceImplicitDefault =
                ec.getParamManager().getBoolean(ConnectionParams.DML_REPLACE_IMPLICIT_DEFAULT);
            final boolean replaceDynamicImplicitDefault =
                ec.getParamManager().getBoolean(ConnectionParams.DML_REPLACE_DYNAMIC_IMPLICIT_DEFAULT);

            this.implicitDefaultColumnIndex = new HashMap<>();
            this.dynamicImplicitDefaultColumnIndex = new HashMap<>();
            if (replaceDynamicImplicitDefault) {
                this.dynamicImplicitDefaultColumnIndex.putAll(insert.getDynamicImplicitDefaultColumnIndexMap());
                this.implicitDefaultColumnIndex.putAll(dynamicImplicitDefaultColumnIndex);
            }
            if (replaceImplicitDefault) {
                // implicit default column map contains all dynamic implicit default columns
                this.implicitDefaultColumnIndex.putAll(insert.getImplicitDefaultColumnIndexMap());
            }

            this.handleOnDuplicateKeyUpdate = handleOnDuplicateKeyUpdate;

            // If any of dynamic implicit default column must be computed in CN, make it all
            this.computeAllDynamicImplicitDefault = computeAllDynamicImplicitDefault();
        }

        protected boolean computeAllDynamicImplicitDefault() {
            return dynamicImplicitDefaultColumnIndex.keySet().stream().anyMatch(
                columnIndex -> literalColumnIndex.contains(columnIndex) || deterministicColumnIndex.contains(
                    columnIndex));
        }

        public boolean withImplicitDefault(int columnIndex) {
            return implicitDefaultColumnIndex.containsKey(columnIndex);
        }

        public DynamicImplicitDefaultHandlerCall buildSelect(int columnIndex, RexNode rex, RelNode sourceRel) {
            final RexBuilder rexBuilder = insert.getCluster().getRexBuilder();

            final boolean mustBeDeterministic = deterministicColumnIndex.contains(columnIndex);
            final boolean mustBeLiteral = literalColumnIndex.contains(columnIndex);
            final boolean withImplicitDefault = implicitDefaultColumnIndex.containsKey(columnIndex);
            final boolean withDynamicImplicitDefault = dynamicImplicitDefaultColumnIndex.containsKey(columnIndex);

            final RexNode defaultRex = implicitDefaultColumnIndex.get(columnIndex);

            return RexHandlerCallFactory.createDynamicImplicitDefaultHandlerCall(rex,
                mustBeLiteral,
                mustBeDeterministic,
                withImplicitDefault,
                withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault,
                defaultRex,
                rexBuilder,
                columnIndex,
                sourceRel);
        }
    }

    @Getter
    public static class ReplaceRexWithParamHandlerCallBuilder extends DynamicImplicitDefaultHandlerCallBuilder {
        protected final AtomicInteger maxParamIndex;
        protected final boolean replaceDynamicImplicitDefaultWrapperWithParam;
        protected final boolean computeAllDynamicImplicitDefaultRefInOneGo;
        protected final boolean replaceExplicitRexCallWithComputedDynamicImplicitDefault;

        protected final Map<String, RexCallParam> dynamicImplicitDefaultParamMap;

        public ReplaceRexWithParamHandlerCallBuilder(LogicalInsert insert, AtomicInteger maxParamIndex,
                                                     ExecutionContext ec) {
            this(insert, maxParamIndex, ec, false);
        }

        public ReplaceRexWithParamHandlerCallBuilder(LogicalInsert insert, AtomicInteger maxParamIndex,
                                                     ExecutionContext ec, boolean handleOnDuplicateKeyUpdate) {
            super(insert, ec, handleOnDuplicateKeyUpdate);
            this.maxParamIndex = Preconditions.checkNotNull(maxParamIndex);

            this.replaceDynamicImplicitDefaultWrapperWithParam =
                ec.getParamManager().getBoolean(ConnectionParams.DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM);
            this.computeAllDynamicImplicitDefaultRefInOneGo = ec.getParamManager()
                .getBoolean(ConnectionParams.DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO);
            this.replaceExplicitRexCallWithComputedDynamicImplicitDefault = ec.getParamManager()
                .getBoolean(ConnectionParams.DML_REPLACE_EXPLICIT_REX_CALL_WITH_COMPUTED_DYNAMIC_IMPLICIT_DEFAULT);

            // load dynamic implicit default param map and update insert plan
            this.dynamicImplicitDefaultParamMap =
                insert.loadDynamicImplicitDefaultParamMap(maxParamIndex, computeAllDynamicImplicitDefaultRefInOneGo);
        }

        @Override
        protected boolean computeAllDynamicImplicitDefault() {
            if (this.handleOnDuplicateKeyUpdate) {
                // If any of dynamic implicit default column must be computed in CN, make it all
                return this.insert.getDuplicateKeyUpdateList().stream().anyMatch(
                    item -> dynamicImplicitDefaultColumnIndex.containsKey(
                        ((RexInputRef) ((RexCall) item).getOperands().get(0)).getIndex()));
            } else {
                // If any of dynamic implicit default column must be computed in CN, make it all
                return dynamicImplicitDefaultColumnIndex.keySet().stream().anyMatch(
                    columnIndex -> literalColumnIndex.contains(columnIndex) || deterministicColumnIndex.contains(
                        columnIndex));
            }
        }

        public ReplaceRexWithParamHandlerCall buildLogicalDynamicValues(int columnIndex, RexNode rex) {
            final boolean mustBeLiteral = literalColumnIndex.contains(columnIndex);
            final boolean mustBeDeterministic = deterministicColumnIndex.contains(columnIndex);
            return buildLogicalDynamicValues(columnIndex, rex, mustBeLiteral, mustBeDeterministic);
        }

        public ReplaceRexWithParamHandlerCall buildLogicalDynamicValues(int columnIndex, RexNode rex,
                                                                        final boolean mustBeLiteral,
                                                                        final boolean mustBeDeterministic) {
            final boolean withImplicitDefault = implicitDefaultColumnIndex.containsKey(columnIndex);
            final boolean withDynamicImplicitDefault = dynamicImplicitDefaultColumnIndex.containsKey(columnIndex);

            final RexBuilder rexBuilder = insert.getCluster().getRexBuilder();
            final RexNode originDefault = implicitDefaultColumnIndex.get(columnIndex);
            final RexNode defaultRex = getCommonDefaultRex(originDefault, dynamicImplicitDefaultParamMap,
                computeAllDynamicImplicitDefaultRefInOneGo);

            /**
             * <pre>
             * Check type and compute functions.
             * If the function is a sharding key, compute it.
             * If the function can not be pushed down ( like LAST_INSERT_ID() ), compute it.
             * If the function can be pushed down, check its operands, which may be functions can't be pushed down.
             * If the function is not deterministic, compute it.
             * If a parent node need to be computed, its child node must also be computed.
             * If a child node is cloned, its parent node must also be cloned.
             * If the target column is column with implicit default on null, wrap function with IF_NULL(rexNode, implicitDefault).
             * </pre>
             */

            return RexHandlerCallFactory.createReplaceLogicalDynamicValueCall(rex,
                mustBeLiteral,
                mustBeDeterministic,
                withImplicitDefault,
                withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault,
                defaultRex,
                rexBuilder,
                maxParamIndex,
                replaceDynamicImplicitDefaultWrapperWithParam,
                replaceExplicitRexCallWithComputedDynamicImplicitDefault);
        }

        public ReplaceRexWithParamHandlerCall buildOnDuplicateKeyUpdate(int columnIndex, RexNode rex,
                                                                        RexBuilder rexBuilder,
                                                                        final boolean mustBeLiteral,
                                                                        final boolean mustBeDeterministic,
                                                                        RexUtils.ColumnRefFinder columnRefFinder) {
            final boolean withImplicitDefault = implicitDefaultColumnIndex.containsKey(columnIndex);
            final boolean withDynamicImplicitDefault = dynamicImplicitDefaultColumnIndex.containsKey(columnIndex);
            final RexNode originDefault = implicitDefaultColumnIndex.get(columnIndex);
            final RexNode defaultRex = getCommonDefaultRex(originDefault, dynamicImplicitDefaultParamMap,
                computeAllDynamicImplicitDefaultRefInOneGo);

            return RexHandlerCallFactory.createReplaceOnDuplicateKeyUpdateCall(rex,
                mustBeLiteral,
                mustBeDeterministic,
                withImplicitDefault,
                withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault,
                defaultRex,
                rexBuilder,
                maxParamIndex,
                // For execution mode DETERMINISTIC_PUSHDOWN, we do not have before-value of column,
                // so that column ref expression can not be computed on CN.
                r -> !columnRefFinder.analyze(r),
                replaceDynamicImplicitDefaultWrapperWithParam,
                replaceExplicitRexCallWithComputedDynamicImplicitDefault);
        }

        private static @Nullable RexNode getCommonDefaultRex(RexNode originDefault,
                                                             Map<String, RexCallParam> dynamicImplicitDefaultParamMap,
                                                             boolean computeAllDynamicImplicitDefaultRefInOneGo) {
            if (!computeAllDynamicImplicitDefaultRefInOneGo) {
                return originDefault;
            }

            final RexNode defaultRex = Optional.ofNullable(originDefault).map(r -> {
                if (r instanceof RexCall) {
                    final RexCall rexCall = (RexCall) r;
                    if (rexCall.getOperator().getName().equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                        final RexCallParam rexCallParam =
                            dynamicImplicitDefaultParamMap.get(rexCall.getOperator().getName());
                        return null == rexCallParam ? null : new RexCallParam(rexCallParam.getType(),
                            rexCallParam.getIndex(),
                            rexCallParam.getRexCall(),
                            rexCallParam.isWrapRexDynamicParam(),
                            true);
                    }
                }
                return r;
            }).orElse(null);
            return defaultRex;
        }
    }

    public static ReplaceRexWithParamHandlerCall createReplaceLogicalDynamicValueCall(RexNode rex,
                                                                                      boolean mustBeLiteral,
                                                                                      boolean mustBeDeterministic,
                                                                                      boolean withImplicitDefault,
                                                                                      boolean withDynamicImplicitDefault,
                                                                                      boolean computeAllDynamicImplicitDefault,
                                                                                      RexNode defaultRex,
                                                                                      RexBuilder rexBuilder,
                                                                                      AtomicInteger currentMaxParamIndex,
                                                                                      boolean replaceImplicitDefaultWrapperWithParam,
                                                                                      boolean replaceExplicitRexCallWithComputedDynamicImplicitDefault) {
        final DynamicImplicitDefaultHandlerCall dynamicImplicitDefaultHandlerCall =
            createDynamicImplicitDefaultHandlerCall(rex, mustBeLiteral, mustBeDeterministic, withImplicitDefault,
                withDynamicImplicitDefault, computeAllDynamicImplicitDefault, defaultRex, rexBuilder,
                replaceExplicitRexCallWithComputedDynamicImplicitDefault);

        Predicate<RexNode> doReplace = LogicalWriteUtil.FALSE_PREDICATE;
        if (replaceImplicitDefaultWrapperWithParam) {
            final RexFilter replaceWithParamFilter = dynamicImplicitDefaultHandlerCall.buildRexFilterTree();
            doReplace = replaceWithParamFilter::test;
        }

        return new ReplaceRexWithParamHandlerCall(dynamicImplicitDefaultHandlerCall, mustBeLiteral,
            mustBeDeterministic, currentMaxParamIndex, LogicalWriteUtil.TRUE_PREDICATE, doReplace);
    }

    public static ReplaceRexWithParamHandlerCall createReplaceOnDuplicateKeyUpdateCall(RexNode rex,
                                                                                       boolean mustBeLiteral,
                                                                                       boolean mustBeDeterministic,
                                                                                       boolean withImplicitDefault,
                                                                                       boolean withDynamicImplicitDefault,
                                                                                       boolean computeAllDynamicImplicitDefault,
                                                                                       RexNode defaultRex,
                                                                                       RexBuilder rexBuilder,
                                                                                       AtomicInteger currentMaxParamIndex,
                                                                                       Predicate<RexNode> computableRex,
                                                                                       boolean replaceImplicitDefaultWrapperWithParam,
                                                                                       boolean replaceExplicitRexCallWithComputedDynamicImplicitDefault) {
        if (!withImplicitDefault && !withDynamicImplicitDefault) {
            return new ReplaceRexWithParamHandlerCall(new BaseRexHandlerCall(rex),
                mustBeLiteral,
                mustBeDeterministic,
                currentMaxParamIndex,
                computableRex,
                LogicalWriteUtil.FALSE_PREDICATE);
        } else {
            final DynamicImplicitDefaultHandlerCall dynamicImplicitDefaultHandlerCall =
                createDynamicImplicitDefaultHandlerCall(rex, mustBeLiteral, mustBeDeterministic, withImplicitDefault,
                    withDynamicImplicitDefault, computeAllDynamicImplicitDefault, defaultRex, rexBuilder,
                    replaceExplicitRexCallWithComputedDynamicImplicitDefault);

            Predicate<RexNode> doReplace = LogicalWriteUtil.FALSE_PREDICATE;
            if (replaceImplicitDefaultWrapperWithParam) {
                final RexFilter replaceWithParamFilter = dynamicImplicitDefaultHandlerCall.buildRexFilterTree();
                doReplace = replaceWithParamFilter::test;
            }

            return new ReplaceRexWithParamHandlerCall(dynamicImplicitDefaultHandlerCall, mustBeLiteral,
                mustBeDeterministic, currentMaxParamIndex, computableRex, doReplace);
        }
    }
}

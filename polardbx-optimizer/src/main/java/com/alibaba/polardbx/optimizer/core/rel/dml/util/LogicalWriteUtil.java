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

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RexHandlerCallFactory.DynamicImplicitDefaultHandlerCallBuilder;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils.NullableState;
import com.alibaba.polardbx.optimizer.utils.RexUtils.RexNodeTransformer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

public class LogicalWriteUtil {

    /**
     * Find those columns which must be literal. If it's a call, we need to
     * convert it to literal. Stored column indexes are corresponding to the
     * full row type, not insertRowType.
     * <p>
     * Including:
     * 1. Partition columns of primary and index table
     * 2. Local unique keys of primary table, if gsi exists or INSERT IGNORE/REPLACE/INSERT ON DUPLICATE KEY UPDATE on partitioned table
     * 4. All columns with auto increment property must be literal
     */
    public static @NotNull Set<String> literalColumnNames(String schemaName, String tableName,
                                                          @NotNull ExecutionContext ec) {
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        assert null != oc;

        final Set<String> literalColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        // Columns with auto increment property
        TableMeta primaryTableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        literalColumnNames.addAll(primaryTableMeta.getAutoIncrementColumns());

        // Sharding keys of base table
        TddlRuleManager or = oc.getRuleManager();
        List<String> shardColumns = or.getSharedColumns(tableName);
        shardColumns.forEach(columnName -> literalColumnNames.add(columnName.toUpperCase()));

        List<TableMeta> indexTableMetas = GlobalIndexMeta.getIndex(tableName, schemaName, ec);
        if (!indexTableMetas.isEmpty()) {
            // sharding keys of index tables
            for (TableMeta indexMeta : indexTableMetas) {
                shardColumns = or.getSharedColumns(indexMeta.getTableName());
                shardColumns.forEach(columnName -> literalColumnNames.add(columnName.toUpperCase()));
            }

            // Even if it's normal INSERT, unique keys must be literals, so that we can judge if it's null
            primaryTableMeta.getUniqueIndexes(true).forEach(indexMeta -> indexMeta.getKeyColumns()
                .forEach(columnMeta -> literalColumnNames.add(columnMeta.getName().toUpperCase())));
        }

        // Generated columns and all their referencing columns must be literal
        literalColumnNames.addAll(primaryTableMeta.getLogicalGeneratedColumnNames());
        GeneratedColumnUtil.getAllLogicalReferencedColumnsByGen(primaryTableMeta).values()
            .forEach(literalColumnNames::addAll);
        return literalColumnNames;
    }

    /**
     * Columns has to be replaced with literal if multi write happens
     * <p>
     * Including:
     * 1. All columns of broadcast table
     * 2. All columns of gsi table
     * 3. All column of ScaleOut writable table
     */
    public static @NotNull Set<String> deterministicColumnNames(String schemaName, String tableName,
                                                                @NotNull ExecutionContext ec) {
        final Set<String> resultColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        final TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        assert null != oc;
        final TddlRuleManager rule = oc.getRuleManager();

        // Columns in broadcast table
        if (rule.isBroadCast(tableName)) {
            tableMeta.getAllColumns().forEach(cm -> resultColumnNames.add(cm.getName()));
        }

        // Columns in GSI
        final List<TableMeta> indexTableMetas = GlobalIndexMeta.getIndex(tableName, schemaName, ec);
        indexTableMetas.forEach(gsiTm -> gsiTm.getAllColumns().forEach(cm -> resultColumnNames.add(cm.getName())));

        // Columns in ScaleOut writable table
        if (ComplexTaskPlanUtils.canWrite(tableMeta)) {
            tableMeta.getAllColumns().forEach(cm -> resultColumnNames.add(cm.getName()));
        }

        return resultColumnNames;
    }

    /**
     * Columns has to be replaced with literal if multi write happens and statement explicitly set column to null
     * <p>
     * Including:
     * 1. All columns with type of timestamp not null
     * 2. All columns with type of not null and the statement is insert ignore or update ignore
     */
    public static void implicitDefaultColumnNames(RelOptTable targetTable,
                                                  boolean withIgnoreKeyword,
                                                  RexBuilder rexBuilder,
                                                  Map<String, RexNode> outAllImplicitDefaultColumnNames,
                                                  Map<String, RexNode> outDynamicImplicitDefaultColumnNames) {
        final TableMeta tableMeta = CBOUtil.getTableMeta(targetTable);

        final InitializerContext initializerContext =
            new InitializerContext() {
                public RexBuilder getRexBuilder() {
                    return rexBuilder;
                }

                public RexNode convertExpression(SqlNode e) {
                    throw new UnsupportedOperationException();
                }
            };

        for (Ord<RelDataTypeField> ord : Ord.zip(targetTable.getRowType().getFieldList())) {
            final RelDataTypeField relDataTypeField = ord.getValue();
            final String columnName = relDataTypeField.getName();
            final ColumnMeta cm = tableMeta.getColumn(columnName);

            // Skip auto_increment column
            if (cm.isAutoIncrement()) {
                continue;
            }

            if (cm.withImplicitDefault(withIgnoreKeyword)) {
                final InitializerExpressionFactory f = tableMeta.unwrap(InitializerExpressionFactory.class);
                final RexNode implicitDefault = f.newImplicitDefaultValue(targetTable, ord.i, initializerContext);

                outAllImplicitDefaultColumnNames.put(cm.getName(), implicitDefault);
                if (cm.withDynamicImplicitDefault()) {
                    outDynamicImplicitDefaultColumnNames.put(cm.getName(), implicitDefault);
                }
            }
        }
    }

    /**
     * Columns has to be replaced with literal if multi write happens and statement explicitly set column to null
     * <p>
     * Including:
     * 1. All columns with type of timestamp not null
     * 2. All columns with type of not null and the statement is insert ignore or update ignore
     */
    public static void implicitDefaultColumnNames(List<String> targetColumns,
                                                  List<Integer> targetTableIndexes,
                                                  List<RelOptTable> targetTables,
                                                  boolean withIgnoreKeyword,
                                                  boolean replaceImplicitDefaultColumn,
                                                  boolean replaceDynamicImplicitDefaultColumn,
                                                  RexBuilder rexBuilder,
                                                  Map<Integer, Map<String, RexNode>> allImplicitDefaultColumnNames,
                                                  Map<Integer, Map<String, RexNode>> dynamicImplicitDefaultColumnNames) {
        final Map<Integer, Map<String, Integer>> tableIColumnMap = new HashMap<>();
        for (Ord<RelOptTable> relOptTableOrd : Ord.zip(targetTables)) {
            if (null == relOptTableOrd.getValue()) {
                continue;
            }
            final Map<String, Integer> iColumnMap =
                tableIColumnMap.computeIfAbsent(relOptTableOrd.i, k -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER));

            for (Ord<RelDataTypeField> o1 : Ord.zip(relOptTableOrd.getValue().getRowType().getFieldList())) {
                iColumnMap.put(o1.getValue().getName(), o1.i);
            }
        }

        final InitializerContext initializerContext =
            new InitializerContext() {
                public RexBuilder getRexBuilder() {
                    return rexBuilder;
                }

                public RexNode convertExpression(SqlNode e) {
                    throw new UnsupportedOperationException();
                }
            };

        for (Ord<String> stringOrd : Ord.zip(targetColumns)) {
            final Integer targetTableIndex = targetTableIndexes.get(stringOrd.i);
            final RelOptTable targetTable = targetTables.get(targetTableIndex);
            final TableMeta tableMeta = CBOUtil.getTableMeta(targetTable);
            assert null != tableMeta;
            final ColumnMeta cm = tableMeta.getColumn(stringOrd.getValue());
            final Integer iColumn = tableIColumnMap.get(targetTableIndex).get(stringOrd.getValue());

            if (cm.withImplicitDefault(withIgnoreKeyword)) {
                final InitializerExpressionFactory f = tableMeta.unwrap(InitializerExpressionFactory.class);
                final RexNode implicitDefault = f.newImplicitDefaultValue(targetTable, iColumn, initializerContext);

                if (replaceImplicitDefaultColumn) {
                    allImplicitDefaultColumnNames
                        .computeIfAbsent(targetTableIndex, k -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                        .put(cm.getName(), implicitDefault);
                }
                if (cm.withDynamicImplicitDefault() && replaceDynamicImplicitDefaultColumn) {
                    dynamicImplicitDefaultColumnNames
                        .computeIfAbsent(targetTableIndex, k -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                        .put(cm.getName(), implicitDefault);
                }
            }
        }

        allImplicitDefaultColumnNames.putAll(dynamicImplicitDefaultColumnNames);
    }

    public static LogicalInsert handleDynamicImplicitDefault(LogicalInsert insert, ExecutionContext ec) {
        final DynamicImplicitDefaultHandlerCallBuilder handlerCallBuilder =
            new DynamicImplicitDefaultHandlerCallBuilder(insert, ec);

        final RelNode sourceRel = RelUtils.getRelInput(insert);

        final RexBuilder rexBuilder = insert.getCluster().getRexBuilder();

        final List<RexNode> newPros = new ArrayList<>();

        RelNode newSourceRel = sourceRel;
        if (sourceRel instanceof Sort) {
            // Remove sort
            newSourceRel = RelUtils.getRelInput(sourceRel);
        }

        final List<RexNode> currentPros = new ArrayList<>();
        if (newSourceRel instanceof Project) {
            currentPros.addAll(((Project) newSourceRel).getProjects());
        }

        boolean hasImplicitDefaultReplaced = false;
        for (int columnIndex = 0; columnIndex < sourceRel.getRowType().getFieldCount(); columnIndex++) {
            RexNode newPro = currentPros.isEmpty() ?
                // Add project on top
                rexBuilder.makeInputRef(newSourceRel, columnIndex)
                // Merge with current project
                : currentPros.get(columnIndex);

            if (handlerCallBuilder.withImplicitDefault(columnIndex)) {
                // Compute implicit default
                final DynamicImplicitDefaultHandlerCall handlerCall = handlerCallBuilder.buildSelect(columnIndex,
                    newPro,
                    newSourceRel);

                final RexHandlerChain<DynamicImplicitDefaultHandlerCall> chain = RexHandlerChain.create(
                    RexHandlerFactory.DYNAMIC_IMPLICIT_DEFAULT_ALL_HANDLERS);

                final RexNode result = chain.process(handlerCall).getResult();

                if (newPro != result) {
                    hasImplicitDefaultReplaced = true;
                    newPro = result;
                }
            }

            newPros.add(newPro);
        }

        if (hasImplicitDefaultReplaced) {
            final List<String> originalNames = new ArrayList<>(sourceRel.getRowType().getFieldNames());

            if (!currentPros.isEmpty()) {
                newSourceRel = RelUtils.getRelInput(newSourceRel);
            }

            newSourceRel = LogicalProject.create(newSourceRel,
                newPros,
                RexUtil.createOriginalStructType(newSourceRel.getCluster().getTypeFactory(), newPros, originalNames));

            if (sourceRel instanceof Sort) {
                // Add sort back
                newSourceRel = ((Sort) sourceRel).copy(sourceRel.getTraitSet(), ImmutableList.of(newSourceRel));
            }

            return (LogicalInsert) insert.copy(insert.getTraitSet(), ImmutableList.of(newSourceRel));
        }

        return insert;
    }

    /**
     * <pre>
     * If RexNode contains RexInputRef operands, return true
     * </pre>
     */
    public static class InputRefFilter extends TypedRexFilter<RexNode, RexHandlerCall> {
        public InputRefFilter(RexHandlerCall handlerCall) {
            super(RexNode.class, handlerCall);
        }

        @Override
        boolean testWithTypeChecked(RexNode rex) {
            final int inputRefCount = RexUtil.getInputRefCount(rex);

            return inputRefCount > 0;
        }

        public static RexFilter of(LogicalWriteRexHandlerCall call) {
            return new InputRefFilter(call);
        }
    }

    /**
     * <pre>
     * If function cannot push down, return true
     * If function is last_insert_id with no operands, return true
     * </pre>
     */
    public static class UnpushableFunctionFilter extends TypedRexFilter<RexCall, RexHandlerCall> {
        public UnpushableFunctionFilter(RexHandlerCall handlerCall) {
            super(RexCall.class, handlerCall);
        }

        @Override
        boolean testWithTypeChecked(RexCall rexCall) {
            return new RexUtils.RexCallChecker(RexUtils::isUnpushableFunction).analyze(rexCall);
        }

        public static RexFilter of(LogicalWriteRexHandlerCall call) {
            return new UnpushableFunctionFilter(call);
        }
    }

    /**
     * For logical write, If function is dynamic, return true
     */
    public static class DynamicFunctionFilter extends TypedRexFilter<RexCall, LogicalWriteRexHandlerCall> {
        public DynamicFunctionFilter(LogicalWriteRexHandlerCall handlerCall) {
            super(RexCall.class, handlerCall);
        }

        @Override
        public boolean testWithTypeChecked(RexCall rexCall) {
            return handlerCall.mustBeDeterministic
                && new RexUtils.RexCallChecker((r) -> r.getOperator().isDynamicFunction()).analyze(rexCall);
        }

        public static RexFilter of(LogicalWriteRexHandlerCall call) {
            return new DynamicFunctionFilter(call);
        }
    }

    /**
     * For must be literal column, If target column has implicit default, return true
     */
    public static class ImplicitDefaultFilter extends TypedRexFilter<RexNode, DynamicImplicitDefaultHandlerCall> {
        public ImplicitDefaultFilter(DynamicImplicitDefaultHandlerCall handlerCall) {
            super(RexNode.class, handlerCall);
        }

        @Override
        public boolean testWithTypeChecked(RexNode rex) {
            return handlerCall.withImplicitDefault
                && handlerCall.mustBeLiteral;
        }

        public static ImplicitDefaultFilter of(DynamicImplicitDefaultHandlerCall handlerCall) {
            return new ImplicitDefaultFilter(handlerCall);
        }
    }

    /**
     * For logical write, If target column has dynamic implicit default, return true
     */
    public static class DynamicImplicitDefaultFilter
        extends TypedRexFilter<RexNode, DynamicImplicitDefaultHandlerCall> {
        public DynamicImplicitDefaultFilter(DynamicImplicitDefaultHandlerCall handlerCall) {
            super(RexNode.class, handlerCall);
        }

        @Override
        public boolean testWithTypeChecked(RexNode rex) {
            return handlerCall.withDynamicImplicitDefault
                && (handlerCall.computeAllDynamicImplicitDefault || handlerCall.mustBeDeterministic);
        }

        public static DynamicImplicitDefaultFilter of(DynamicImplicitDefaultHandlerCall handlerCall) {
            return new DynamicImplicitDefaultFilter(handlerCall);
        }
    }

    public static abstract class LogicalWriteRexHandlerCall extends RexHandlerCallDecorator {
        public final boolean mustBeLiteral;
        public final boolean mustBeDeterministic;

        public LogicalWriteRexHandlerCall(RexHandlerCall rexHandlerCall, boolean mustBeLiteral,
                                          boolean mustBeDeterministic) {
            super(rexHandlerCall);
            this.mustBeLiteral = mustBeLiteral;
            this.mustBeDeterministic = mustBeDeterministic;
        }
    }

    public static final class ReplaceWithScaleHandlerCall extends RexHandlerCallDecorator {
        public final RexNode maxScaleRex;
        public final Long maxScale;
        public final Function<RexNode, Long> scaleFinder;
        public final RexFilter replaceFilter;
        public final RexBuilder rexBuilder;

        public ReplaceWithScaleHandlerCall(RexHandlerCall base, RexNode maxScaleRex, Long maxScale,
                                           Function<RexNode, Long> scaleFinder, RexBuilder rexBuilder,
                                           BiPredicate<RexNode, ReplaceWithScaleHandlerCall> forceReplacePredicate) {
            super(base);
            this.maxScaleRex = maxScaleRex;
            this.maxScale = maxScale;
            this.scaleFinder = scaleFinder;
            this.replaceFilter = TypedRexFilter.of(this, forceReplacePredicate);
            this.rexBuilder = rexBuilder;
        }

        ReplaceWithScaleHandlerCall(RexHandlerCall base, RexNode maxScaleRex, Long maxScale,
                                    Function<RexNode, Long> scaleFinder, RexBuilder rexBuilder,
                                    RexFilter replaceFilter) {
            super(base);
            this.maxScaleRex = maxScaleRex;
            this.maxScale = maxScale;
            this.scaleFinder = scaleFinder;
            this.replaceFilter = replaceFilter;
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexFilter buildRexFilter() {
            return replaceFilter;
        }

        @Override
        public RexNodeTransformer buildTransformer() {
            RexNodeTransformer transformer = rexHandlerCall.buildTransformer();

            transformer =
                new RexUtils.ReplaceWithScaleDecorator(transformer, rexBuilder, replaceFilter, maxScaleRex, maxScale,
                    scaleFinder);

            return transformer;
        }

        @Override
        public RexHandlerCall copy(RexNode newRex, RexNode result) {
            return new ReplaceWithScaleHandlerCall(rexHandlerCall.copy(newRex, result), maxScaleRex, maxScale,
                scaleFinder, rexBuilder, replaceFilter);
        }

        @Override
        public RexHandlerCall copy() {
            return new ReplaceWithScaleHandlerCall(rexHandlerCall.copy(), maxScaleRex, maxScale, scaleFinder,
                rexBuilder, replaceFilter);
        }
    }

    public static final class DynamicImplicitDefaultHandlerCall extends LogicalWriteRexHandlerCall {
        public final boolean withImplicitDefault;
        public final boolean withDynamicImplicitDefault;
        public final boolean computeAllDynamicImplicitDefault;

        public final RexNode defaultRex;
        public final RexBuilder rexBuilder;

        /**
         * Whether RexNode is a reference of null
         */
        @Getter
        private final NullableState nullableState;

        DynamicImplicitDefaultHandlerCall(RexHandlerCall base, boolean mustBeLiteral,
                                          boolean mustBeDeterministic, boolean withImplicitDefault,
                                          boolean withDynamicImplicitDefault,
                                          boolean computeAllDynamicImplicitDefault, RexNode defaultRex,
                                          RexBuilder rexBuilder, NullableState nullableState) {
            super(base, mustBeLiteral, mustBeDeterministic);
            this.withImplicitDefault = withImplicitDefault;
            this.withDynamicImplicitDefault = withDynamicImplicitDefault;
            this.computeAllDynamicImplicitDefault = computeAllDynamicImplicitDefault;
            this.defaultRex = defaultRex;
            this.rexBuilder = rexBuilder;
            this.nullableState = nullableState;
        }

        @Override
        public RexFilter buildRexFilter() {
            return CombineRexFilter.combine(RexFilter.RexFilterOperator.OR,
                ImplicitDefaultFilter.of(this),
                DynamicImplicitDefaultFilter.of(this));
        }

        @Override
        public RexNodeTransformer buildTransformer() {
            RexNodeTransformer transformer = rexHandlerCall.buildTransformer();

            transformer = new RexUtils.IfNullDefaultDecorator(transformer,
                buildRexFilter(),
                defaultRex,
                rexBuilder,
                nullableState);

            return transformer;
        }

        @Override
        public RexHandlerCall copy(RexNode newRex, RexNode result) {
            return new DynamicImplicitDefaultHandlerCall(rexHandlerCall.copy(newRex, result),
                mustBeLiteral,
                mustBeDeterministic,
                withImplicitDefault,
                withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault,
                defaultRex,
                rexBuilder,
                nullableState);
        }

        @Override
        public RexHandlerCall copy() {
            return new DynamicImplicitDefaultHandlerCall(rexHandlerCall.copy(),
                mustBeLiteral,
                mustBeDeterministic,
                withImplicitDefault,
                withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault,
                defaultRex,
                rexBuilder,
                nullableState);
        }
    }

    public static final class ReplaceRexWithParamHandlerCall extends LogicalWriteRexHandlerCall {
        public final AtomicInteger currentMaxParamIndex;
        public final RexFilter computableRexFilter;
        /**
         * Support more scenario to replace RexNode with Parameter
         */
        public final RexFilter doReplaceRexFilter;

        ReplaceRexWithParamHandlerCall(RexHandlerCall base, boolean mustBeLiteral, boolean mustBeDeterministic,
                                       AtomicInteger currentMaxParamIndex, Predicate<RexNode> computableRex,
                                       Predicate<RexNode> doReplace) {
            super(base, mustBeLiteral, mustBeDeterministic);
            this.currentMaxParamIndex = currentMaxParamIndex;
            this.computableRexFilter = TypedRexFilter.of(this, (r, c) -> computableRex.test(r));
            this.doReplaceRexFilter = TypedRexFilter.of(this, (r, c) -> doReplace.test(r));
        }

        ReplaceRexWithParamHandlerCall(RexHandlerCall base, boolean mustBeLiteral, boolean mustBeDeterministic,
                                       AtomicInteger currentMaxParamIndex, RexFilter computableRexFilter,
                                       RexFilter doReplaceRexFilter) {
            super(base, mustBeLiteral, mustBeDeterministic);
            this.currentMaxParamIndex = currentMaxParamIndex;
            this.computableRexFilter = Preconditions.checkNotNull(computableRexFilter);
            this.doReplaceRexFilter = Preconditions.checkNotNull(doReplaceRexFilter);
        }

        /**
         * If column must be literal, compute
         * If function cannot push down, compute
         * If function is last_insert_id with no operands, compute
         * For logical write, If function is dynamic, compute
         */
        @Override
        public RexFilter buildRexFilter() {
            final CombineRexFilter shouldReplaceRexFilter = CombineRexFilter.create(RexFilter.RexFilterOperator.OR,
                TypedRexFilter.of(this, (r, c) -> c.mustBeLiteral),
                InputRefFilter.of(this),
                UnpushableFunctionFilter.of(this),
                DynamicFunctionFilter.of(this));
            return computableRexFilter.and(doReplaceRexFilter.or(shouldReplaceRexFilter));
        }

        @Override
        public RexNodeTransformer buildTransformer() {
            RexNodeTransformer transformer = rexHandlerCall.buildTransformer();

            transformer = new RexUtils.RexCallParamDecorator(transformer,
                buildRexFilter(),
                currentMaxParamIndex,
                getRex() instanceof RexDynamicParam);

            return transformer;
        }

        public ReplaceRexWithParamHandlerCall copy(RexNode rex, boolean mustBeLiteral, RexNode result) {
            return new ReplaceRexWithParamHandlerCall(rexHandlerCall.copy(rex, result), mustBeLiteral,
                mustBeDeterministic, currentMaxParamIndex, computableRexFilter, doReplaceRexFilter);
        }

        public ReplaceRexWithParamHandlerCall copy(RexNode rex, boolean mustBeLiteral) {
            return copy(rex, mustBeLiteral, rexHandlerCall.getResult());
        }

        @Override
        public ReplaceRexWithParamHandlerCall copy(RexNode rex, RexNode result) {
            return copy(rex, mustBeLiteral, result);
        }

        @Override
        public ReplaceRexWithParamHandlerCall copy() {
            return new ReplaceRexWithParamHandlerCall(rexHandlerCall.copy(), mustBeLiteral,
                mustBeDeterministic, currentMaxParamIndex, computableRexFilter, doReplaceRexFilter);
        }
    }

    /**
     * Wrap RexNode with IF_NULL(rex, defaultRex) if target column has dynamic implicit value
     */
    public static class DynamicImplicitDefaultHandler<T extends RexNode>
        extends TypedRexHandler<T, DynamicImplicitDefaultHandlerCall> {
        protected DynamicImplicitDefaultHandler(Class<T> type) {
            super(type);
        }
    }

    /**
     * Replace specified type RexNode with RexCallParam if target column must be written with literal
     */
    public static class ReplaceRexWithParamHandler<T extends RexNode>
        extends TypedRexHandler<T, ReplaceRexWithParamHandlerCall> {
        protected ReplaceRexWithParamHandler(Class<T> type) {
            super(type);
        }
    }

    public static Predicate<RexNode> FALSE_PREDICATE = o -> false;
    public static Predicate<RexNode> TRUE_PREDICATE = o -> true;
}

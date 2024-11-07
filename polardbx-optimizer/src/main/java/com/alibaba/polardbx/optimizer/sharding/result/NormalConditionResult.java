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

package com.alibaba.polardbx.optimizer.sharding.result;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.TableScanLabel;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class NormalConditionResult extends ColumnEqualityConditionResult {

    protected List<RexNode> predicates;
    protected boolean withRexCallParam;

    static NormalConditionResult create(ExtractorContext context, Label label, Collection<RexNode> predicates) {
        return new NormalConditionResult(context, label, predicates);
    }

    protected NormalConditionResult(ExtractorContext context, Label label, Collection<RexNode> predicates) {
        super(context, label);
        this.predicates = ImmutableList.copyOf(predicates);
    }

    @Override
    public NormalConditionResult simplify() {
        return this;
    }

    @Override
    public List<RexNode> toRexNodes() {
        return predicates;
    }

    public PartitionPruneStep toPartPruneStep(ExecutionContext ec) {
        return toPartPruneStep(ec, null);
    }

    public PartitionPruneStep toPartPruneStep(ExecutionContext ec, PartitionInfo cciPartInfo) {

        PartitionPruneStep pruneStepInfo = null;
        if (null == label || GeneralUtil.isEmpty(predicates)) {
            return null;
        }

        Preconditions.checkArgument(label instanceof TableScanLabel);
        final List<String> qualifiedName = ((TableScanLabel) label).getTable().getQualifiedName();
        final String tableName = Util.last(qualifiedName);
        final String schema = qualifiedName.size() > 1 ? qualifiedName.get(qualifiedName.size() - 2) : null;
        final RelNode rel = label.getRel();
        final RexBuilder builder = this.label.getRel().getCluster().getRexBuilder();
        final List<RexNode> sorted = PredicateUtil.sortPredicates(predicates);
        final RexNode comparison = RexUtil.composeConjunction(builder, sorted, true);
        if (null != comparison) {
            PartitionInfo partInfo = cciPartInfo == null ?
                ec.getSchemaManager(schema).getTable(tableName).getPartitionInfo() : cciPartInfo;
            if (partInfo == null) {
                return null;
            }
            try {
                //RexNode comparisonAfterDnf = RexUtil.toDnf(builder, comparison);
                RexNode partPredcate = comparison;
                pruneStepInfo = PartitionPruner.generatePartitionPrueStepInfo(partInfo, rel, partPredcate, ec);
            } catch (TddlRuntimeException e) {
                if (e.getErrorCode() == ErrorCode.ERR_TODNF_LIMIT_EXCEED.getCode()) {
                    // do nothing, just skip this condition, and do full scan
                    pruneStepInfo = PartitionPruner.generatePartitionPrueStepInfo(partInfo, rel, null, ec);
                } else {
                    throw e;
                }
            }
        }
        return pruneStepInfo;
    }

    @Override
    public Map<String, Comparative> toPartitionCondition(ExecutionContext executionContext) {
        final Map<String, Comparative> result = new HashMap<>();
        if (null == label || GeneralUtil.isEmpty(predicates)) {
            return result;
        }

        Preconditions.checkArgument(label instanceof TableScanLabel);

        final List<String> qualifiedName = ((TableScanLabel) label).getTable().getQualifiedName();
        final String tableName = Util.last(qualifiedName);
        final String schema = qualifiedName.size() > 1 ? qualifiedName.get(qualifiedName.size() - 2) : null;
        final RelDataType tableRowType = label.getRel().getRowType();

        final RexBuilder builder = this.label.getRel().getCluster().getRexBuilder();

        final List<RexNode> sorted = PredicateUtil.sortPredicates(predicates);

        final RexNode comparison = RexUtil.composeConjunction(builder, sorted, true);
        if (null != comparison) {
            try {
                RexNode dnf = RexUtil.toDnf(builder, comparison);
                PlannerUtils.buildComparative(result, dnf, tableRowType, tableName, schema, executionContext);
            } catch (TddlRuntimeException e) {
                if (e.getErrorCode() == ErrorCode.ERR_TODNF_LIMIT_EXCEED.getCode()) {
                    // do nothing, just skip this condition
                } else {
                    throw e;
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, Comparative> toColumnCondition(List<String> columns) {
        final Map<String, Comparative> result = new HashMap<>();
        if (null == label || GeneralUtil.isEmpty(predicates) || columns == null) {
            return result;
        }

        Preconditions.checkArgument(label instanceof TableScanLabel);

        final RelDataType tableRowType = label.getRel().getRowType();

        final RexBuilder builder = this.label.getRel().getCluster().getRexBuilder();

        final List<RexNode> sorted = PredicateUtil.sortPredicates(predicates);

        final RexNode comparison = RexUtil.composeConjunction(builder, sorted, true);
        if (null != comparison) {
            try {
                RexNode dnf = RexUtil.toDnf(builder, comparison);

                final List<String> qualifiedName = ((TableScanLabel) label).getTable().getQualifiedName();
                final String schema = qualifiedName.size() > 1 ? qualifiedName.get(qualifiedName.size() - 2) : null;
                Partitioner partitioner = OptimizerContext.getContext(schema).getPartitioner();

                PlannerUtils.buildColumnsComparative(result, dnf, tableRowType, columns, partitioner);
            } catch (TddlRuntimeException e) {
                if (e.getErrorCode() == ErrorCode.ERR_TODNF_LIMIT_EXCEED.getCode()) {
                    // do nothing, just skip this condition
                } else {
                    throw e;
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, Comparative> toFullPartitionCondition(ExecutionContext executionContext) {
        Map<String, Comparative> result = new HashMap<>();
        if (null == label || GeneralUtil.isEmpty(predicates)) {
            return result;
        }

        Preconditions.checkArgument(label instanceof TableScanLabel);

        final List<String> qualifiedName = ((TableScanLabel) label).getTable().getQualifiedName();
        final String tableName = Util.last(qualifiedName);
        final String schema = qualifiedName.size() > 1 ? qualifiedName.get(qualifiedName.size() - 2) : null;
        final RelDataType tableRowType = label.getRel().getRowType();

        final RexBuilder builder = this.label.getRel().getCluster().getRexBuilder();

        final List<RexNode> sorted = PredicateUtil.sortPredicates(predicates);

        final RexNode comparison = RexUtil.composeConjunction(builder, sorted, true);
        if (null != comparison) {
            try {
                RexNode dnf = RexUtil.toDnf(builder, comparison);
                result = PlannerUtils
                    .buildComparativeWithAllColumn(result, dnf, tableRowType, tableName, schema, executionContext);
            } catch (TddlRuntimeException e) {
                if (e.getErrorCode() == ErrorCode.ERR_TODNF_LIMIT_EXCEED.getCode()) {
                    // do nothing, just skip this condition
                } else {
                    throw e;
                }
            }
        }
        return result;
    }

    @Override
    public ConditionResult convertScalarFunction2RexCallParam(AtomicInteger maxParamIndex,
                                                              ExecutionContext executionContext) {

        final RexUtils.ColumnRefFinder columnRefFinder = new RexUtils.ColumnRefFinder();

        final AtomicBoolean replaced = new AtomicBoolean(false);
        final List<RexNode> retPredicates = predicates.stream().map(
            rex -> {
                final RexUtils.ReplaceScalarFunctionWithRexCallParamVisitor replacer =
                    new RexUtils.ReplaceScalarFunctionWithRexCallParamVisitor(
                        maxParamIndex,
                        (r) -> !columnRefFinder.analyze(r) && !r.isA(SqlKind.ROW),
                        (r) -> replaced.set(true));
                return rex.accept(replacer);
            }
        ).collect(Collectors.toList());

        return NormalConditionResult.create(context, label, retPredicates)
            .setWithRexCallParam(replaced.get());
    }

    public NormalConditionResult setWithRexCallParam(boolean withRexCallParam) {
        this.withRexCallParam = withRexCallParam;
        return this;
    }

}

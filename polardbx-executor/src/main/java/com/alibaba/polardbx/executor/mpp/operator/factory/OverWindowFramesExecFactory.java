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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.druid.util.StringUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.NonFrameOverWindowExec;
import com.alibaba.polardbx.executor.operator.OverWindowFramesExec;
import com.alibaba.polardbx.executor.operator.frame.OverWindowFrame;
import com.alibaba.polardbx.executor.operator.frame.RangeSlidingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RangeUnboundedFollowingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RangeUnboundedPrecedingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RowSlidingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RowUnboundedFollowingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RowUnboundedPrecedingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.UnboundedOverFrame;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hongxi.chx
 */
public class OverWindowFramesExecFactory extends ExecutorFactory {

    private final SortWindow overWindow;
    private final List<DataType> columnMetas;

    public OverWindowFramesExecFactory(SortWindow overWindow, ExecutorFactory executorFactory) {
        this.overWindow = overWindow;
        this.columnMetas = CalciteUtils.getTypes(overWindow.getRowType());
        addInput(executorFactory);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        return createFrameWindowExec(context, index);
    }

    private Executor createFrameWindowExec(ExecutionContext context, int index) {
        ImmutableList<Window.RexWinAggCall> aggCalls = overWindow.groups.get(0).aggCalls;
        SqlOperator operator = aggCalls.get(0).getOperator();
        if (isNoFrameWindowFunction(operator)) {
            Executor nonFrameWindowExec = createNonFrameWindowExec(overWindow, context, index);
            nonFrameWindowExec.setId(overWindow.getRelatedId());
            if (context.getRuntimeStatistics() != null) {
                RuntimeStatHelper.registerStatForExec(overWindow, nonFrameWindowExec, context);
            }
            return nonFrameWindowExec;
        }
        Executor input = getInputs().get(0).createExecutor(context, index);
        ImmutableBitSet gp = overWindow.groups.get(0).keys;
        List<Integer> list = gp.asList();
        int[] groups = new int[list.size()];
        for (int i = 0, n = list.size(); i < n; i++) {
            groups[i] = list.get(i);
        }
        MemoryAllocatorCtx memoryAllocator = context.getMemoryPool().getMemoryAllocatorCtx();

        OverWindowFrame[] overWindowFrames = new OverWindowFrame[overWindow.groups.size()];
        List<DataType> outputDataTypes = CalciteUtils.getTypes(overWindow.getRowType());
        int aggColumnIndex = input.getDataTypes().size();
        for (int i = 0; i < overWindow.groups.size(); i++) {
            Window.Group group = overWindow.groups.get(i);
            List<AggregateCall> aggregateCalls = group.getAggregateCalls(overWindow);
            List<Aggregator> aggregators = AggregateUtils
                .convertAggregators(input.getDataTypes(),
                    outputDataTypes.subList(aggColumnIndex, aggColumnIndex + aggregateCalls.size()), aggregateCalls,
                    context, memoryAllocator);
            if (group.aggCalls.stream().anyMatch(t -> isUnboundedFrameWindowFunction(t.getOperator()))) {
                overWindowFrames[i] = new UnboundedOverFrame(aggregators.toArray(new Aggregator[0]));
            } else if (group.lowerBound.isUnbounded() && group.upperBound.isUnbounded()) {
                overWindowFrames[i] = new UnboundedOverFrame(aggregators.toArray(new Aggregator[0]));
            } else if (group.isRows) {
                overWindowFrames[i] = createRowFrame(overWindow, group, aggregators);
            } else {
                overWindowFrames[i] = createRangeFrame(overWindow, group, aggregators);
            }
            aggColumnIndex += aggregateCalls.size();
        }
        OverWindowFramesExec overWindowFramesExec =
            new OverWindowFramesExec(input, context, overWindowFrames, list, columnMetas);
        overWindowFramesExec.setId(overWindow.getRelatedId());
        if (context.getRuntimeStatistics() != null) {
            RuntimeStatHelper.registerStatForExec(overWindow, overWindowFramesExec, context);
        }
        return overWindowFramesExec;
    }

    private boolean isNoFrameWindowFunction(SqlOperator method) {
        return method == SqlStdOperatorTable.RANK || method == SqlStdOperatorTable.DENSE_RANK
            || method == SqlStdOperatorTable.ROW_NUMBER;
    }

    private boolean isUnboundedFrameWindowFunction(SqlOperator method) {
        return method == SqlStdOperatorTable.CUME_DIST || method == SqlStdOperatorTable.PERCENT_RANK;
    }

    private com.alibaba.polardbx.executor.operator.Executor createNonFrameWindowExec(SortWindow overWindow,
                                                                                     ExecutionContext context, int index) {
        Executor input = getInputs().get(0).createExecutor(context, index);
        MemoryAllocatorCtx memoryAllocator = context.getMemoryPool().getMemoryAllocatorCtx();

        List<DataType> dataTypes = CalciteUtils.getTypes(overWindow.getRowType());
        List<AggregateCall> aggregateCalls = overWindow.groups.get(0).getAggregateCalls(overWindow);
        ImmutableBitSet gp = overWindow.groups.get(0).keys;
        List<Integer> list = gp.asList();
        int[] groups = list.stream().mapToInt(Integer::intValue).toArray();

        List<Aggregator> aggregators = AggregateUtils
            .convertAggregators(input.getDataTypes(),
                dataTypes.subList(groups.length, groups.length + aggregateCalls.size()), aggregateCalls, context,
                memoryAllocator);

        boolean[] peer = new boolean[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            Window.Group group = overWindow.groups.get(0);
            if (group.lowerBound.isCurrentRow() && group.upperBound.isCurrentRow()) {
                peer[i] = true;
            } else {
                peer[i] = false;
            }
        }

        return new NonFrameOverWindowExec(input, context, aggregators,
            Arrays.stream(groups).boxed().collect(Collectors.toList()), peer, dataTypes);
    }

    private OverWindowFrame createRowFrame(SortWindow overWindow, SortWindow.Group group,
                                           List<Aggregator> aggregators) {
        if (group.lowerBound.isUnbounded()) {
            RexWindowBound upperBound = group.upperBound;
            int offset =
                getConstant(upperBound, overWindow.constants, overWindow.getInput().getRowType().getFieldCount());
            return new RowUnboundedPrecedingOverFrame(aggregators, offset);
        } else if (overWindow.groups.get(0).upperBound.isUnbounded()) {
            RexWindowBound lowerBound = overWindow.groups.get(0).lowerBound;
            int offset =
                getConstant(lowerBound, overWindow.constants, overWindow.getInput().getRowType().getFieldCount());
            return new RowUnboundedFollowingOverFrame(aggregators, offset);
        } else {
            RexWindowBound lowerBound = overWindow.groups.get(0).lowerBound;
            RexWindowBound upperBound = overWindow.groups.get(0).upperBound;
            int lowerOffset =
                getConstant(lowerBound, overWindow.constants, overWindow.getInput().getRowType().getFieldCount());
            int upperOffset =
                getConstant(upperBound, overWindow.constants, overWindow.getInput().getRowType().getFieldCount());
            return new RowSlidingOverFrame(aggregators, lowerOffset, upperOffset);
        }
    }

    private int getConstant(RexWindowBound bound, ImmutableList<RexLiteral> constants, int offset) {
        if (bound.isCurrentRow()) {
            return 0;
        }
        RexNode boundOffset = bound.getOffset();
        assert boundOffset instanceof RexInputRef;
        if (!(boundOffset instanceof RexInputRef)) {
            throw new IllegalArgumentException(
                "bound offset must be a instance of RexInputRef, but was " + boundOffset.getClass().getName() + ".");
        }
        RexLiteral rexLiteral = constants
            .get(((RexInputRef) boundOffset).getIndex() - offset);
        Object value2 = rexLiteral.getValue2();
        if (!StringUtils.isNumber(String.valueOf(value2))) {
            throw new IllegalArgumentException("bound index must be a digit, but was " + value2.toString() + ".");
        }
        return StringUtils.stringToInteger(
            String.valueOf(value2));
    }

    private OverWindowFrame createRangeFrame(SortWindow overWindow, SortWindow.Group group,
                                             List<Aggregator> aggregators) {
        List<RelFieldCollation> sortList = group.orderKeys.getFieldCollations();
        List<OrderByOption> orderBys = new ArrayList<>(sortList.size());
        if (sortList != null) {
            for (int i = 0, n = sortList.size(); i < n; i++) {
                RelFieldCollation field = sortList.get(i);
                orderBys.add(new OrderByOption(field.getFieldIndex(), field.direction, field.nullDirection));
            }
        }
        if (orderBys.size() != 1) {
            throw new IllegalArgumentException(
                "RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression");
        }
        OrderByOption orderByOption = orderBys.get(0);
        int index = orderByOption.getIndex();
        DataType dataType =
            DataTypeUtil.calciteToDrdsType(overWindow.getInput().getRowType().getFieldList().get(index).getType());
        if (group.lowerBound.isUnbounded()) {
            RexWindowBound upperBound = group.upperBound;
            int offset =
                getConstant(upperBound, overWindow.constants, overWindow.getInput().getRowType().getFieldCount());
            return new RangeUnboundedPrecedingOverFrame(aggregators, offset,
                index, orderByOption.isAsc(),
                dataType);
        } else if (overWindow.groups.get(0).upperBound.isUnbounded()) {
            RexWindowBound lowerBound = overWindow.groups.get(0).lowerBound;
            int offset =
                getConstant(lowerBound, overWindow.constants, overWindow.getInput().getRowType().getFieldCount());
            return new RangeUnboundedFollowingOverFrame(aggregators, offset, index, orderByOption.isAsc(), dataType);
        } else {
            RexWindowBound lowerBound = overWindow.groups.get(0).lowerBound;
            RexWindowBound upperBound = overWindow.groups.get(0).upperBound;
            int lowerOffset =
                getConstant(lowerBound, overWindow.constants, overWindow.getInput().getRowType().getFieldCount());
            int upperOffset =
                getConstant(upperBound, overWindow.constants, overWindow.getInput().getRowType().getFieldCount());
            return new RangeSlidingOverFrame(aggregators, lowerOffset, upperOffset, index, orderByOption.isAsc(),
                dataType);
        }
    }

}

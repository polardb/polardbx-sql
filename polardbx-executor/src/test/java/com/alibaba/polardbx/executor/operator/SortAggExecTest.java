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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SortAggExecTest extends BaseExecTest {
    @Test
    public void testInputIsNull() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(null)
            .withChunk(null)
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {1};
        aggregators.add(new CountV2(targetIndex, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        SortAggExec exec =
            new SortAggExec(inputExec, groups, aggregators, outputColumn,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of()
        )), false);
    }

    @Test
    public void testSimpleCount() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 0, 1, 1),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(2, 2, 3, 3),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {1};
        aggregators.add(new CountV2(targetIndex, false, context.getMemoryPool().getMemoryAllocatorCtx() ,-1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.LongType);
        SortAggExec exec =
            new SortAggExec(inputExec, groups, aggregators, outputColumn,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l),
            LongBlock.of(2l, 2l, 2l, 2l)
        )), false);
    }

    @Test
    public void testSimpleSum() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 0, 1, 1),
                IntegerBlock.of(3, 5, 4, 3)))
            .withChunk(new Chunk(
                IntegerBlock.of(2, 2, 3, 3),
                IntegerBlock.of(9, 8, 1, 7)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        SortAggExec exec =
            new SortAggExec(inputExec, groups, aggregators, outputColumn,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            IntegerBlock.of(8, 7, 17, 8)
        )), false);
    }

    @Test
    public void testLargeNumerberBigDecimalSum() {
        DecimalBlockBuilder decimalBlockBuilder1 = new DecimalBlockBuilder(256);
        DecimalBlockBuilder decimalBlockBuilder2 = new DecimalBlockBuilder(256);
        BigDecimal[] bigDecimals1 = {
            new BigDecimal("100000000000000000000000000000000000000000000000000.001"),
            new BigDecimal("100000000000000000000000000000000000000000000000000.001"),
            new BigDecimal("200000000000000000000000000000000000000000000000000.002"),
            new BigDecimal("200000000000000000000000000000000000000000000000000.002")};
        BigDecimal[] bigDecimals2 = {
            new BigDecimal("300000000000000000000000000000000000000000000000000.003"),
            new BigDecimal("300000000000000000000000000000000000000000000000000.003"),
            new BigDecimal("300000000000000000000000000000000000000000000000000.003"),
            new BigDecimal("400000000000000000000000000000000000000000000000000.004")};
        Arrays.stream(bigDecimals1).map(Decimal::fromBigDecimal).forEach(d -> {
            decimalBlockBuilder1.writeDecimal(d);
        });
        Arrays.stream(bigDecimals2).map(Decimal::fromBigDecimal).forEach(d -> {
            decimalBlockBuilder2.writeDecimal(d);
        });

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 0, 1, 1),
                decimalBlockBuilder1.build()
            ))
            .withChunk(new Chunk(
                IntegerBlock.of(2, 2, 2, 3),
                decimalBlockBuilder2.build()))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        SortAggExec exec =
            new SortAggExec(inputExec, groups, aggregators, outputColumn, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(256);
        Arrays.stream(new BigDecimal[] {
            new BigDecimal("200000000000000000000000000000000000000000000000000.002"),
            new BigDecimal("400000000000000000000000000000000000000000000000000.004"),
            new BigDecimal("900000000000000000000000000000000000000000000000000.009"),
            new BigDecimal("400000000000000000000000000000000000000000000000000.004")
        }).map(Decimal::fromBigDecimal).forEach(decimalBlockBuilder::writeDecimal);

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            decimalBlockBuilder
        )), false);
    }

    @Test
    public void testBigDecimalNullAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 0, 1, 1),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(2, 2, 3, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new AvgV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        SortAggExec exec =
            new SortAggExec(inputExec, groups, aggregators, outputColumn, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(256);
        decimalBlockBuilder.appendNull();
        decimalBlockBuilder.appendNull();
        decimalBlockBuilder.appendNull();
        decimalBlockBuilder.appendNull();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            decimalBlockBuilder.build()
        )), false);
    }

    @Test
    public void testDoubleNullAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 0, 1, 1),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(2, 2, 3, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new AvgV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DoubleType);
        SortAggExec exec =
            new SortAggExec(inputExec, groups, aggregators, outputColumn, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            DoubleBlock.of(null, null, null, null)
        )), false);
    }

    @Test
    public void testNullSum() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 0, 1, 1),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(2, 2, 3, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        SortAggExec exec =
            new SortAggExec(inputExec, groups, aggregators, outputColumn, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            IntegerBlock.of(null, null, null, null)
        )), false);
    }

    @Test
    public void testNullCount() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 0, 1, 1),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(2, 2, 3, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {1};
        aggregators.add(new CountV2(targetIndex, false, context.getMemoryPool().getMemoryAllocatorCtx(),-1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.LongType);
        SortAggExec exec =
            new SortAggExec(inputExec, groups, aggregators, outputColumn, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l),
            LongBlock.of(0l, 0l, 0l, 0l)
        )), false);
    }
}

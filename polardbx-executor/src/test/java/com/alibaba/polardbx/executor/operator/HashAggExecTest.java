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
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.executor.calc.aggfunctions.Avg;
import com.alibaba.polardbx.executor.calc.aggfunctions.Count;
import com.alibaba.polardbx.executor.calc.aggfunctions.Sum;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HashAggExecTest extends BaseExecTest {

    private static String TABLE_NAME = "MOCK_HASH_AGG_TABLE";

    private static String COLUMN_PREFIX = "MOCK_HASH_AGG_COLUMN_";

    private static int DEFAULT_AGG_HASH_TABLE_SIZE = 1024;

    // TODO  mysql客户端返回结果一致，但是单元测试结果不一致
    //非顺序情况下返回结果不一致
    //missing (4)   : 0:0 1:4.0000 , 0:1 1:3.5000 , 0:2 1:8.5000 , 0:3 1:4.0000
    //unexpected (4): 0:0 1:4.000000000 , 0:1 1:3.500000000 , 0:2 1:8.500000000 , 0:3 1:4.000000000
    @Ignore
    @Test
    public void testHashAggSimpleAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Avg(1, false, DataTypes.DecimalType, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(256);
        BigDecimal[] bigDecimals = {
            new BigDecimal("4.0000"), new BigDecimal("3.5000"), new BigDecimal("8.5000"),
            new BigDecimal("4.0000")};
        Arrays.stream(bigDecimals).map(Decimal::fromBigDecimal).forEach(decimalBlockBuilder::writeDecimal);

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            decimalBlockBuilder.build()
        )), false);

    }

    @Test
    public void testHashAggSimpleCount() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {1};
        aggregators.add(new Count(targetIndex, false, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l),
            LongBlock.of(2l, 2l, 2l, 2l)
        )), false);
    }

    @Test
    public void testHashAggSimpleSum() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.DecimalType, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            IntegerBlock.of(8, 7, 17, 8)
        )), false);
    }

    @Test
    public void testHashAggLargeNumerberBigDecimalSum() {
        DecimalBlockBuilder decimalBlockBuilder1 = new DecimalBlockBuilder(256);
        DecimalBlockBuilder decimalBlockBuilder2 = new DecimalBlockBuilder(256);
        BigDecimal[] bigDecimals = {
            new BigDecimal("100000000000000000000000000000000000000000000000000.001"),
            new BigDecimal("200000000000000000000000000000000000000000000000000.002"),
            new BigDecimal("300000000000000000000000000000000000000000000000000.003"),
            new BigDecimal("400000000000000000000000000000000000000000000000000.004")};
        Arrays.stream(bigDecimals).map(Decimal::fromBigDecimal).forEach(d -> {
            decimalBlockBuilder1.writeDecimal(d);
            decimalBlockBuilder2.writeDecimal(d);
        });

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                decimalBlockBuilder1.build()
            ))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                decimalBlockBuilder1.build()))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.DecimalType, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(256);
        Arrays.stream(new BigDecimal[] {
            new BigDecimal("200000000000000000000000000000000000000000000000000.002"),
            new BigDecimal("400000000000000000000000000000000000000000000000000.004"),
            new BigDecimal("600000000000000000000000000000000000000000000000000.006"),
            new BigDecimal("800000000000000000000000000000000000000000000000000.008")
        }).map(Decimal::fromBigDecimal).forEach(decimalBlockBuilder::writeDecimal);

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            decimalBlockBuilder
        )), false);
    }

    @Test
    public void testHashAggBigDecimalNullAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Avg(1, false, DataTypes.DecimalType, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
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
    public void testHashAggDoubleNullAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Avg(1, false, DataTypes.DoubleType, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DoubleType);
        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            DoubleBlock.of(null, null, null, null)
        )), false);
    }

    @Test
    public void testHashAggNullSum() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.DecimalType, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3),
            IntegerBlock.of(null, null, null, null)
        )), false);
    }

    @Test
    public void testHashAggNullCount() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {1};
        aggregators.add(new Count(targetIndex, false, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l),
            LongBlock.of(0l, 0l, 0l, 0l)
        )), false);
    }
}


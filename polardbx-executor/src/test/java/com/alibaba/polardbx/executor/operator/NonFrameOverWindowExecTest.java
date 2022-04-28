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
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.executor.calc.aggfunctions.Avg;
import com.alibaba.polardbx.executor.calc.aggfunctions.BitAnd;
import com.alibaba.polardbx.executor.calc.aggfunctions.BitOr;
import com.alibaba.polardbx.executor.calc.aggfunctions.BitXor;
import com.alibaba.polardbx.executor.calc.aggfunctions.Count;
import com.alibaba.polardbx.executor.calc.aggfunctions.Max;
import com.alibaba.polardbx.executor.calc.aggfunctions.Min;
import com.alibaba.polardbx.executor.calc.aggfunctions.Rank;
import com.alibaba.polardbx.executor.calc.aggfunctions.RowNumber;
import com.alibaba.polardbx.executor.calc.aggfunctions.Sum0;
import com.alibaba.polardbx.executor.calc.aggfunctions.Sum;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 单元测试包含的测试内容：
 * * 1. 测试正常情况下的函数是否能正确执行
 * -> sum and sum_v2
 * -> max and min
 * -> avg_v2 and count_v2
 * -> bitOr and bitAnd and bitXor
 * -> rank and row_number
 * 2. 测试current row and current row的情况
 * 3. 测试partition by列表
 * 4. 测试partition by为string等其他类型的情况
 */
public class NonFrameOverWindowExecTest extends BaseExecTest {

    private MockExec input;
    private List<Aggregator> aggregators;

    // 注意null值
    // 测试数据格式
    // null, 1, null
    // null, 1, 2
    //  0, 1, 1
    // --chunk break
    //  0, 1, 2
    // --chunk break
    //  0, 1, 1
    //  0, 2, null
    // --chunk break
    //  0, 2, 2
    // --chunk break
    //  1, 1, 1
    public void prepareNormalData() throws Exception {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType).
                row(null, 1, null).
                row(null, 1, 2).
                row(0, 1, 1).
                chunkBreak().
                row(0, 1, 2).
                row(0, 2, null).
                chunkBreak().
                row(0, 2, 2).
                chunkBreak().
                row(1, 1, 1);
        input = rowChunksBuilder.buildExec();
    }

    @Test
    @Ignore
    public void testSumAndSumV2() throws Exception {
        prepareNormalData();
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum0(2, false, DataTypes.LongType, -1));

        // 那几列是partition by 的key
        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        NonFrameOverWindowExec overWindowExec =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes,
                new boolean[] {false, false}, input.getDataTypes());

        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, 1, null, new Decimal(1, 0), new Decimal(0, 0))
            .row(null, 1, 2, new Decimal(2, 0), new Decimal(2, 0))
            .row(0, 1, 1, new Decimal(1, 0), new Decimal(1, 0))
            .row(0, 1, 2, new Decimal(2, 0), new Decimal(3, 0))
            .row(0, 2, null, new Decimal(4, 0), new Decimal(3, 0))
            .row(0, 2, 2, new Decimal(6, 0), new Decimal(5, 0))
            .row(1, 1, 1, new Decimal(1, 0), new Decimal(1, 0)).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testMaxAndMin() throws Exception {
        prepareNormalData();
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Max(1, DataTypes.IntegerType, DataTypes.IntegerType, -1));
        aggregators.add(new Min(1, DataTypes.IntegerType, DataTypes.IntegerType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        NonFrameOverWindowExec project =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes,
                new boolean[] {false, false}, input.getDataTypes());
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.IntegerType,
                DataTypes.IntegerType)
            .row(null, 1, null, 1, 1)
            .row(null, 1, 2, 1, 1)
            .row(0, 1, 1, 1, 1)
            .row(0, 1, 2, 1, 1)
            .row(0, 2, null, 2, 1)
            .row(0, 2, 2, 2, 1)
            .row(1, 1, 1, 1, 1).build();
        execForSmpMode(project, expects, false);
    }

    @Test
    @Ignore
    public void testAvgV2AndCountV2() throws Exception {
        prepareNormalData();
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Avg(1, false, DataTypes.DecimalType, -1));
        aggregators.add(new Count(new int[] {2}, false, -1));

        // 那几列是partition by 的key
        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        NonFrameOverWindowExec overWindowExec =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes,
                new boolean[] {false, false}, input.getDataTypes());

        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, 1, null, new Decimal(1, 0), new Decimal(0, 0))
            .row(null, 1, 2, new Decimal(1, 0), new Decimal(1, 0))
            .row(0, 1, 1, new Decimal(1, 0), new Decimal(1, 0))
            .row(0, 1, 2, new Decimal(1, 0), new Decimal(2, 0))
            .row(0, 2, null, DataTypes.DecimalType.getCalculator().divide(4, 3), new Decimal(2, 0))
            .row(0, 2, 2, new Decimal(15, 1), new Decimal(3, 0))
            .row(1, 1, 1, new Decimal(1, 0), new Decimal(1, 0)).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testBitRelated() throws Exception {
        prepareNormalData();
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new BitAnd(2, DataTypes.IntegerType, DataTypes.ULongType, -1));
        aggregators.add(new BitOr(2, DataTypes.IntegerType, DataTypes.ULongType, -1));
        aggregators.add(new BitXor(2, DataTypes.IntegerType, DataTypes.ULongType, -1));

        // 那几列是partition by 的key
        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        NonFrameOverWindowExec overWindowExec =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes,
                new boolean[] {false, false, false}, input.getDataTypes());

        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.ULongType,
                DataTypes.ULongType, DataTypes.ULongType)
            .row(null, 1, null, null, new BigInteger("0"), new BigInteger("0"))
            .row(null, 1, 2, new BigInteger("2"), new BigInteger("2"), new BigInteger("2"))
            .row(0, 1, 1, new BigInteger("1"), new BigInteger("1"), new BigInteger("1"))
            .row(0, 1, 2, new BigInteger("0"), new BigInteger("3"), new BigInteger("3"))
            .row(0, 2, null, new BigInteger("0"), new BigInteger("3"), new BigInteger("3"))
            .row(0, 2, 2, new BigInteger("0"), new BigInteger("3"), new BigInteger("1"))
            .row(1, 1, 1, new BigInteger("1"), new BigInteger("1"), new BigInteger("1")).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testRank() throws Exception {
        prepareNormalData();
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Rank(new int[] {1}, -1));

        // 那几列是partition by 的key
        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        NonFrameOverWindowExec overWindowExec =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes,
                new boolean[] {false}, input.getDataTypes());

        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.IntegerType)
            .row(null, 1, null, 1)
            .row(null, 1, 2, 1)
            .row(0, 1, 1, 1)
            .row(0, 1, 2, 1)
            .row(0, 2, null, 3)
            .row(0, 2, 2, 3)
            .row(1, 1, 1, 1).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testRowNumber() throws Exception {
        prepareNormalData();
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new RowNumber(-1));

        // 那几列是partition by 的key
        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        NonFrameOverWindowExec overWindowExec =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes,
                new boolean[] {false}, input.getDataTypes());

        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.IntegerType)
            .row(null, 1, null, 1)
            .row(null, 1, 2, 2)
            .row(0, 1, 1, 1)
            .row(0, 1, 2, 2)
            .row(0, 2, null, 3)
            .row(0, 2, 2, 4)
            .row(1, 1, 1, 1).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testResetAccumulators() throws Exception {
        prepareNormalData();
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum0(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        NonFrameOverWindowExec project =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes, new boolean[] {false, true},
                input.getDataTypes());
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, 1, null, new Decimal(1, 0), new Decimal(0, 0))
            .row(null, 1, 2, new Decimal(2, 0), new Decimal(2, 0))
            .row(0, 1, 1, new Decimal(1, 0), new Decimal(1, 0))
            .row(0, 1, 2, new Decimal(2, 0), new Decimal(2, 0))
            .row(0, 2, null, new Decimal(4, 0), new Decimal(0, 0))
            .row(0, 2, 2, new Decimal(6, 0), new Decimal(2, 0))
            .row(1, 1, 1, new Decimal(1, 0), new Decimal(1, 0)).build();
        execForSmpMode(project, expects, false);
    }

    @Test
    @Ignore
    public void testPartitionIndexList() throws Exception {
        prepareNormalData();
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum0(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);
        partitionIndexes.add(1);

        NonFrameOverWindowExec project =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes,
                new boolean[] {false}, input.getDataTypes());
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType)
            .row(null, 1, null, new Decimal(0, 0))
            .row(null, 1, 2, new Decimal(2, 0))
            .row(0, 1, 1, new Decimal(1, 0))
            .row(0, 1, 2, new Decimal(3, 0))
            .row(0, 2, null, new Decimal(0, 0))
            .row(0, 2, 2, new Decimal(2, 0))
            .row(1, 1, 1, new Decimal(1, 0)).build();
        execForSmpMode(project, expects, false);
    }

    @Test
    @Ignore
    public void testPartitionType() throws Exception {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.StringType, DataTypes.DateType, DataTypes.DecimalType
                , DataTypes.ULongType, DataTypes.DoubleType, DataTypes.FloatType, DataTypes.IntegerType)
                .row(new String("test"), new Date(1000000000), new Decimal(11, 1), new BigInteger("0"), (double) 1.111,
                    (float) 1.111, 1)
                .row(new String("test"), new Date(1000000000), new Decimal(11, 1), new BigInteger("0"), (double) 1.111,
                    (float) 1.111, 2)
                .row(new String("test"), new Date(2000000000), new Decimal(11, 1), new BigInteger("0"), (double) 1.111,
                    (float) 1.111, 1)
                .row(new String("test"), new Date(2000000000), new Decimal(11, 1), new BigInteger("0"), (double) 1.111,
                    (float) 1.111, 2)
                .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("0"), (double) 1.111,
                    (float) 1.111, 1)
                .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("0"), (double) 1.111,
                    (float) 1.111, 2)
                .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.111,
                    (float) 1.111, 1)
                .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.111,
                    (float) 1.111, 2)
                .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.112,
                    (float) 1.111, 1)
                .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.112,
                    (float) 1.111, 2)
                .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.112,
                    (float) 1.112, 1)
                .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.112,
                    (float) 1.112, 2);
        input = rowChunksBuilder.buildExec();

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum0(6, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>(Arrays.asList(0, 1, 2, 3, 4, 5));

        NonFrameOverWindowExec project =
            new NonFrameOverWindowExec(input, context, aggregators, partitionIndexes,
                new boolean[] {false}, input.getDataTypes());
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.StringType, DataTypes.DateType, DataTypes.DecimalType
                , DataTypes.ULongType, DataTypes.DoubleType, DataTypes.FloatType, DataTypes.IntegerType,
                DataTypes.DecimalType)
            .row(new String("test"), new Date(1000000000), new Decimal(11, 1), new BigInteger("0"), (double) 1.111,
                (float) 1.111, 1, new Decimal(1, 0))
            .row(new String("test"), new Date(1000000000), new Decimal(11, 1), new BigInteger("0"), (double) 1.111,
                (float) 1.111, 2, new Decimal(3, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(11, 1), new BigInteger("0"), (double) 1.111,
                (float) 1.111, 1, new Decimal(1, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(11, 1), new BigInteger("0"), (double) 1.111,
                (float) 1.111, 2, new Decimal(3, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("0"), (double) 1.111,
                (float) 1.111, 1, new Decimal(1, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("0"), (double) 1.111,
                (float) 1.111, 2, new Decimal(3, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.111,
                (float) 1.111, 1, new Decimal(1, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.111,
                (float) 1.111, 2, new Decimal(3, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.112,
                (float) 1.111, 1, new Decimal(1, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.112,
                (float) 1.111, 2, new Decimal(3, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.112,
                (float) 1.112, 1, new Decimal(1, 0))
            .row(new String("test"), new Date(2000000000), new Decimal(12, 1), new BigInteger("1"), (double) 1.112,
                (float) 1.112, 2, new Decimal(3, 0)).build();
        execForSmpMode(project, expects, false);
    }
}

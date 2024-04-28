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


import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Sum0;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AggregateBenchmark {

    public static int totalNumber = 10000000;

    final ExecutionContext context;

    {
        // mock context
        context = new ExecutionContext();
        context.setMemoryPool(
            MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "test", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));
    }

    protected static List<ColumnMeta> getColumnMetas(DataType... dataTypes) {
        List<ColumnMeta> columns = new ArrayList<>(dataTypes.length);
        for (int i = 0; i < dataTypes.length; i++) {
            columns.add(new ColumnMeta("MOCK_TABLE", "COLUMN_" + i, null,
                new Field("MOCK_TABLE", "COLUMN_" + i, dataTypes[i])));
        }
        return columns;
    }

    private void runAggrWithoutGroupby() {
        // sum(id)
        runBenchmark("without groupby", 10, () -> {
            Executor input = new Benchmark.MockIntegerExec(1, totalNumber);
            List<Aggregator> aggregators = new ArrayList<>();
            aggregators.add(new SumV2(0, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
            Integer expectedOutputRowCount = 1;
            HashAggExec hashAggExec =
                new HashAggExec(
                    input.getDataTypes(), new int[] {}, aggregators,
                    ImmutableList.of(DataTypes.LongType),
                    expectedOutputRowCount, context);
            List<Chunk> actual = BaseExecTest.execForMppMode(hashAggExec, input, -1, false);
        });
    }

    private void runAggrWithMultiKey(int columnNum) {
        // group by k sum() for (id & 65535) as k
        runBenchmark("with multi keys " + columnNum, 10, () -> {
            Executor input = new Benchmark.MockMultiKeysExec(1, totalNumber, columnNum);
            List<Aggregator> aggregators = new ArrayList<>();
            aggregators.add(new Sum0(0, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
            DataType[] dataTypes = new DataType[columnNum + 1];
            int[] groups = new int[columnNum];
            for (int i = 0; i < columnNum; i++) {
                dataTypes[i] = DataTypes.IntegerType;
                groups[i] = i;
            }
            dataTypes[columnNum] = DataTypes.LongType;
            Integer expectedOutputRowCount = 65535;

            HashAggExec hashAggExec =
                new HashAggExec(
                    input.getDataTypes(), groups, aggregators, Arrays.asList(dataTypes),
                    expectedOutputRowCount, context);
            List<Chunk> actual = BaseExecTest.execForMppMode(hashAggExec, input, -1, false);
        });
    }

    private void runBenchmark(String name, int iter, Benchmark benchmark) {
        int start = 0;
        List<Benchmark.Timer> timers = new ArrayList<>();
        while (start < iter) {
            Benchmark.Timer timer = new Benchmark.Timer(start);
            timer.startTiming();
            benchmark.run();
            timer.stopTiming();
            timers.add(timer);
            System.out.println(name + " iter " + start + " time= " + timer.totalTime());
            start++;
        }
    }

    public void runBenchmarkSuite() {
        runAggrWithoutGroupby();
        runAggrWithMultiKey(1);
        runAggrWithMultiKey(2);
        runAggrWithMultiKey(4);
//        benchSum();
//        benchSum();
    }

//    private void benchSum() {
//        benchLong2DecimalSumUsingMapWithOverFlow();
//        benchLong2DecimalSumUsingMapWithoutOverFlow();
//    }
//
//    private void benchLong2DecimalSumUsingMapWithOverFlow() {
//        runBenchmark("sum(long) -> decimal using map with over flow" + 1, 10, () -> {
//            Executor input = new Benchmark.MockOverFlowExec(true);
//            List<Aggregator> aggregators = new ArrayList<>();
//            aggregators.add(new Long2DecimalSum(1, false, DataTypes.LongType, DataTypes.DecimalType, -1));
//            DataType[] dataTypes = new DataType[] {DataTypes.IntegerType, DataTypes.DecimalType};
//
//            Integer expectedOutputRowCount = 1024;
//
//            ExecutionContext context = new ExecutionContext();
//            context.setMemoryPool(
//                MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
//                    "test", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));
//
//            HashAggExec hashAggExec =
//                new HashAggExec(
//                    input.getDataTypes(), new int[] {0}, aggregators, Arrays.asList(dataTypes),
//                    expectedOutputRowCount, context);
//            BaseExecTest.execForMppMode(hashAggExec, input, -1, false);
//        });
//    }
//
//    private void benchLong2DecimalSumUsingMapWithoutOverFlow() {
//        runBenchmark("sum(long) -> decimal using map without over flow" + 1, 10, () -> {
//            Executor input = new Benchmark.MockOverFlowExec(false);
//            List<Aggregator> aggregators = new ArrayList<>();
//            aggregators.add(new Long2DecimalSum(1, false, DataTypes.LongType, DataTypes.DecimalType, -1));
//            DataType[] dataTypes = new DataType[] {DataTypes.IntegerType, DataTypes.DecimalType};
//
//            Integer expectedOutputRowCount = 1024;
//
//            ExecutionContext context = new ExecutionContext();
//            context.setMemoryPool(
//                MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
//                    "test", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));
//
//            HashAggExec hashAggExec =
//                new HashAggExec(
//                    input.getDataTypes(), new int[] {0}, aggregators, Arrays.asList(dataTypes),
//                    expectedOutputRowCount, context);
//            BaseExecTest.execForMppMode(hashAggExec, input, -1, false);
//        });
//    }

    public static void main(String[] args) {
        totalNumber = 10000000;
        new AggregateBenchmark().runBenchmarkSuite();
    }
}

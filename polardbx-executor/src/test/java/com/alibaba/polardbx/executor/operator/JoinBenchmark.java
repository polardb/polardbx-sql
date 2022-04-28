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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.List;

public class JoinBenchmark {

    public static int buildNumber = 100000;
    public static int totalNumber = 100000;
    public static int iter = 100;

    final ExecutionContext context;

    {
        // mock context
        context = new ExecutionContext();
        context.setMemoryPool(
            MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "test", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));
    }

    public static void execForMppBenchmark(Executor output, int revokeChunkNum, boolean revokeAfterBuild) {
        Executor input = output.getInputs().get(0);
        ConsumerExecutor consumer = (ConsumerExecutor) output;
        int chunkCnt = 0;
        input.open();
        consumer.openConsume();
        while (true) {
            if (consumer.needsInput()) {
                Chunk ret = input.nextChunk();
                chunkCnt++;
                if (ret == null) {
                    break;
                }
                consumer.consumeChunk(ret);
            }
        }
        long timeStart = System.currentTimeMillis();
        consumer.buildConsume();
        System.out.println("build end, time=" + (System.currentTimeMillis() - timeStart));
        output.open();
        try {
            Chunk chunk;
            while ((chunk = output.nextChunk()) != null) {
            }
        } finally {
            output.close();
        }
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

    private void runJoinWithMultiKey(int columnNum) {
        // group by k sum() for (id & 65535) as k
        runBenchmark("with multi keys " + columnNum, iter, () -> {
            Executor innerInput = new Benchmark.MockMultiKeysExec(1, buildNumber, columnNum, buildNumber);
            Executor outerInput = new Benchmark.MockMultiKeysExec(1, totalNumber, columnNum, buildNumber);

            JoinRelType joinType = JoinRelType.INNER;
            boolean maxOneRow = false;
            List<EquiJoinKey> joinKeys = new ArrayList<>();
            for (int i = 0; i < columnNum; i++) {
                joinKeys.add(new EquiJoinKey(i, i, DataTypes.LongType, false, false));
            }
            IExpression otherCondition = null;
            ParallelHashJoinExec.Synchronizer synchronizer =
                new ParallelHashJoinExec.Synchronizer(1, false);
            ParallelHashJoinExec hashJoinExec =
                new ParallelHashJoinExec(synchronizer, outerInput, innerInput, joinType, maxOneRow, joinKeys,
                    otherCondition,
                    null, false, context, 0);
            execForMppBenchmark(hashJoinExec, -1, false);
        });
    }

    public void runBenchmarkSuite() {
        runJoinWithMultiKey(1);
//        runJoinWithMultiKey(2);
//        runJoinWithMultiKey(4);
    }

    public static void main(String[] args) {
        new JoinBenchmark().runBenchmarkSuite();
    }
}

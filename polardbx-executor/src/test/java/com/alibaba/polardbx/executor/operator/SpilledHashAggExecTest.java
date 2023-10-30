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

import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.GenericSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SyncFileCleaner;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.executor.calc.aggfunctions.Count;
import com.alibaba.polardbx.executor.calc.aggfunctions.Sum;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;

public class SpilledHashAggExecTest extends BaseExecTest {

    private static SpillerFactory spillerFactory;
    private static Path tempPath = Paths.get("./tmp/" + UUID.randomUUID());

    @BeforeClass
    public static void beforeClass() {
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().add(tempPath);
        AsyncFileSingleStreamSpillerFactory singleStreamSpillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), ImmutableList.of(tempPath), 2);
        spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        MoreFiles.deleteRecursively(tempPath, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void testMemoryHashAggr() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
                .row(null, 1).row(1, 2).row(2, 3).row(3, 4);
        MockExec input = rowChunksBuilder.buildExec();

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Count(new int[] {1}, false, -1));
        List<DataType> columns = ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType);
        List<Chunk> expects = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(null, 1).row(1, 1).row(2, 1).row(3, 1).build();
        HashAggExec hashAggExec =
            new HashAggExec(input.getDataTypes(), new int[] {0}, aggregators, columns, 100, context);
        List<Chunk> actual = execForMppMode(hashAggExec, input, -1, false);
        assertExecResultByRow(actual, expects, false);
    }

    @Test
    public void testSpillHashAggr() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType);
        long chunkCnt = 20;
        int cnt = 0;
        while (cnt < chunkCnt) {
            rowChunksBuilder.row(0, 0);
            rowChunksBuilder.row(1, 1);
            rowChunksBuilder.chunkBreak();
            cnt++;
        }
        MockExec input = rowChunksBuilder.buildExec();
        List<Chunk> expects = rowChunksBuilder(DataTypes.IntegerType, DataTypes.LongType)
            .row(0, chunkCnt)
            .row(1, chunkCnt)
            .build();

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Count(new int[] {1}, false, -1));
        List<DataType> columns = ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType);
        HashAggExec hashAggExec = new HashAggExec(input.getDataTypes(), new int[] {0}, aggregators, columns,
            AggregateUtils.collectDataTypes(columns, 1, 2), 100, spillerFactory, context);

        List<Chunk> actual = execForMppMode(hashAggExec, input, 10, true);
        assertExecResultByRow(actual, expects, false);
    }

    @Test
    public void testSpillHashAggr2() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType);
        long chunkCnt = 20;
        int cnt = 0;
        while (cnt < chunkCnt) {
            for (int j = 0; j < 1024; j++) {
                rowChunksBuilder.row(j, j);
            }
            rowChunksBuilder.chunkBreak();
            cnt++;
        }
        MockExec input = rowChunksBuilder.buildExec();

        RowChunksBuilder resultChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.LongType);
        for (int j = 0; j < 1024; j++) {
            resultChunksBuilder.row(j, chunkCnt);
        }
        List<Chunk> expects = resultChunksBuilder.build();

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Count(new int[] {1}, false, -1));
        List<DataType> columns = ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType);
        HashAggExec hashAggExec = new HashAggExec(input.getDataTypes(), new int[] {0}, aggregators, columns,
            AggregateUtils.collectDataTypes(columns, 1, 2), 100, spillerFactory, context);
        List<Chunk> actual = execForMppMode(hashAggExec, input, 10, true);
        assertExecResultByRow(actual, expects, false);
    }

    @Test
    public void testSpillHashAggr3() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.StringType, DataTypes.IntegerType);
        long chunkCnt = 20;
        int cnt = 0;
        while (cnt < chunkCnt) {
            for (int j = 0; j < 1024; j++) {
                rowChunksBuilder.row("a" + j, j);
            }
            rowChunksBuilder.chunkBreak();
            cnt++;
        }
        MockExec input = rowChunksBuilder.buildExec();

        RowChunksBuilder resultChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.StringType, DataTypes.LongType);
        for (int j = 0; j < 1024; j++) {
            resultChunksBuilder.row("a" + j, chunkCnt);
        }
        List<Chunk> expects = resultChunksBuilder.build();

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Count(new int[] {1}, false, -1));
        List<DataType> columns = ImmutableList.of(DataTypes.StringType, DataTypes.LongType);
        HashAggExec hashAggExec = new HashAggExec(input.getDataTypes(), new int[] {0}, aggregators, columns,
            AggregateUtils.collectDataTypes(columns, 1, 2), 100, spillerFactory, context);
        List<Chunk> actual = execForMppMode(hashAggExec, input, 5, false);
        assertExecResultByRow(actual, expects, false);
    }

    @Test
    public void testSpillHashAggWithCountV2() {
        RowChunksBuilder inputBuilder = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .addSequenceChunk(1024, 10000, 10000)
            .addSequenceChunk(1024, 22000, 13000)
            .addSequenceChunk(1024, 30000, 13000)
            .addSequenceChunk(1024, 40000, 15000)
            .addSequenceChunk(200, 70, 70)
            .addSequenceChunk(1024, 50000, 16000)
            .addSequenceChunk(1024, 60000, 17000)
            .addSequenceChunk(1024, 80000, 19000)
            .addSequenceChunk(10240, 80000, 19000);

        List<Aggregator> aggregators = new ArrayList<>();
        List<Aggregator> copiedAggregators = new ArrayList<>();

        List<DataType> columns = ImmutableList.of(DataTypes.StringType, DataTypes.LongType);
        DataType[] aggValueType = AggregateUtils.collectDataTypes(columns, 1, 2);
        //count
        aggregators.add(new Count(new int[] {1}, false, -1));
        copiedAggregators.add(new Count(new int[] {1}, false, -1));
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 3, false);
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 3, true);
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 10, true);
        //sum
        aggregators.clear();
        copiedAggregators.clear();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        copiedAggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 3, false);
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 3, true);
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 10, true);
        //sum + count
        aggregators.clear();
        copiedAggregators.clear();
        aggValueType = new DataType[] {
            DataTypes.LongType, DataTypes.LongType
        };
        columns = ImmutableList.of(DataTypes.StringType, DataTypes.LongType, DataTypes.LongType);
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Count(new int[] {1}, false, -1));
        copiedAggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        copiedAggregators.add(new Count(new int[] {1}, false, -1));
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 3, false);
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 3, true);
        baseTest(aggregators, copiedAggregators, columns, aggValueType, inputBuilder, 10, true);
    }

    public void baseTest(List<Aggregator> aggregators, List<Aggregator> copiedAggregators, List<DataType> columns,
                         DataType[] aggValueType,
                         RowChunksBuilder inputBuilder,
                         int revokeChunkNum, boolean revokeAfterBuild) {
        MockExec input1 = inputBuilder.buildExec();
        MockExec input2 = inputBuilder.buildExec();

        HashAggExec hashAggExec1 = new HashAggExec(input1.getDataTypes(), new int[] {0}, aggregators, columns,
            aggValueType, 100, spillerFactory, context);

        HashAggExec hashAggExec2 = new HashAggExec(input2.getDataTypes(), new int[] {0}, copiedAggregators, columns,
            aggValueType, 100, null, context);

        List<Chunk> expects = execForMppMode(hashAggExec2, input2, 0, false);
        List<Chunk> actuals = execForMppMode(hashAggExec1, input1, revokeChunkNum, revokeAfterBuild);

        assertExecResultByRow(actuals, expects, false);
    }
}

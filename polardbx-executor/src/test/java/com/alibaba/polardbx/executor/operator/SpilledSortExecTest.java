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

import com.google.common.collect.ImmutableList;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.GenericSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SyncFileCleaner;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;

public class SpilledSortExecTest extends BaseExecTest {

    private static SpillerFactory spillerFactory;
    private static Path tempPath = Paths.get("./tmp/" + UUID.randomUUID());

    @BeforeClass
    public static void beforeClass() {
        List<Path> spillPaths = new ArrayList<>();
        spillPaths.add(tempPath);
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().addAll(spillPaths);
        AsyncFileSingleStreamSpillerFactory singleStreamSpillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), ImmutableList.of(tempPath), 2);
        spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        MoreFiles.deleteRecursively(tempPath, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void testSort() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
                .row(null, 1).row(1, 2).row(3, 4).row(2, 3);
        MockExec input = rowChunksBuilder.buildExec();
        OrderByOption orderByOption = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST);

        SortExec sort = new SortExec(input.getDataTypes(), ImmutableList.of(orderByOption), context, spillerFactory);
        List<Chunk> expects = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(null, 1).row(1, 2).row(2, 3).row(3, 4).build();
        List<Chunk> actuals = execForMppMode(sort, input, -1, true);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testSpillSortByDesc() {
        RowChunksBuilder rowChunksBuilder = RowChunksBuilder.rowChunksBuilder(
            DataTypes.IntegerType, DataTypes.IntegerType);
        int rowNum = 200;
        int chunkLimit = 10;
        int step = 0;
        while (step < rowNum) {
            for (int j = 0; j < chunkLimit; j++) {
                rowChunksBuilder.row(step, step);
                step++;
            }
            rowChunksBuilder.chunkBreak();
        }
        MockExec input = rowChunksBuilder.buildExec();
        OrderByOption orderByOption = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST);
        SortExec sort = new SortExec(input.getDataTypes(), ImmutableList.of(orderByOption), context, spillerFactory);

        rowChunksBuilder = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType);
        for (int j = 0; j < rowNum; j++) {
            rowChunksBuilder.row(j, j);
        }
        List<Chunk> expects = rowChunksBuilder.build();
        List<Chunk> actuals = execForMppMode(sort, input, 5, true);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testSort1() {
        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0),
                ImmutableList.of(RelFieldCollation.Direction.DESCENDING));
        RowChunksBuilder inputBuilder = rowChunksBuilder(DataTypes.StringType, DataTypes.DoubleType)
            .addSequenceChunk(1024, 10000, 10000)
            .addSequenceChunk(1024, -300, -300)
            .addSequenceChunk(1024, 22000, 13000)
            .addSequenceChunk(1024, 30000, 14000)
            .addSequenceChunk(1024, 40000, 15000)
            .addSequenceChunk(200, 70, 70)
            .addSequenceChunk(1024, 50000, 16000)
            .addSequenceChunk(1024, 60000, 17000)
            .addSequenceChunk(1024, 70000, 18000)
            .addSequenceChunk(1024, 80000, 19000);

        baseTest(orderbys, inputBuilder, 3, false);
        baseTest(orderbys, inputBuilder, 3, true);
        baseTest(orderbys, inputBuilder, 10, true);
    }

    @Test
    public void testSort2() {
        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0),
                ImmutableList.of(RelFieldCollation.Direction.ASCENDING));
        RowChunksBuilder inputBuilder = rowChunksBuilder(DataTypes.StringType, DataTypes.DoubleType)
            .addSequenceChunk(1024, 10000, 10000)
            .addSequenceChunk(1024, -300, -300)
            .addSequenceChunk(1024, 22000, 13000)
            .addSequenceChunk(1024, 30000, 14000)
            .addSequenceChunk(1024, 40000, 15000)
            .addSequenceChunk(200, 70, 70)
            .addSequenceChunk(1024, 50000, 16000)
            .addSequenceChunk(1024, 60000, 17000)
            .addSequenceChunk(1024, 70000, 18000)
            .addSequenceChunk(10240, 80000, 19000);

        Executor input = inputBuilder.buildExec();
        SortExec sortExec = new SortExec(input.getDataTypes(), orderbys, context, spillerFactory);
        execForMppMode(sortExec, input, 5, true, new Callable() {
            @Override
            public Object call() throws Exception {
                Assert.assertEquals(0, sortExec.getMemoryAllocatorCtx().getAllAllocated());
                return null;
            }
        });
    }

    public void baseTest(List<OrderByOption> orderBys, RowChunksBuilder inputBuilder, int revokeChunkNum,
                         boolean revokeAfterBuild) {
        MockExec input1 = inputBuilder.buildExec();
        MockExec input2 = inputBuilder.buildExec();

        SortExec sortExec1 = new SortExec(input1.getDataTypes(), orderBys, context, spillerFactory);

        SortExec sortExec2 = new SortExec(input2.getDataTypes(), orderBys, context, null);

        List<Chunk> actuals = execForMppMode(sortExec1, input1, revokeChunkNum, revokeAfterBuild);
        List<Chunk> expects = execForMppMode(sortExec2, input2, 0, false);
        assertExecResultByRow(actuals, expects, true);
    }
}

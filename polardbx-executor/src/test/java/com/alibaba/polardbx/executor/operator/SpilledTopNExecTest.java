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
import com.alibaba.polardbx.executor.chunk.Chunk;
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
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.google.common.truth.Truth.assertWithMessage;
import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class SpilledTopNExecTest extends BaseExecTest {

    private static SpillerFactory spillerFactory;
    private static Path tempPath = Paths.get("./tmp/" + UUID.randomUUID());

    @Parameterized.Parameters(name = "MPP={0}")
    public static List<Boolean> prepare() {
        return Arrays.asList(TRUE, FALSE);
    }

    @BeforeClass
    public static void beforeClass() {
        List<Path> spillPaths = new ArrayList<>();
        spillPaths.add(tempPath);
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().addAll(spillPaths);
        AsyncFileSingleStreamSpillerFactory singleStreamSpillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), spillPaths, 4);
        spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        MoreFiles.deleteRecursively(tempPath, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    public void testSingleFieldKey() {
        MockExec input = rowChunksBuilder(DataTypes.LongType, DataTypes.DoubleType)
            .row(1L, 0.1)
            .row(2L, 0.2)
            .chunkBreak()
            .row(-1L, -0.1)
            .row(4L, 0.4)
            .chunkBreak()
            .row(5L, 0.5)
            .row(6L, 0.6)
            .row(7L, 0.7)
            .chunkBreak()
            .buildExec();

        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0), ImmutableList.of(RelFieldCollation.Direction.DESCENDING));
        SpilledTopNExec topNExec = new SpilledTopNExec(input.getDataTypes(), orderbys, 2, context);
        List<Chunk> expects = rowChunksBuilder(DataTypes.LongType, DataTypes.DoubleType)
            .row(7L, 0.7)
            .row(6L, 0.6)
            .build();
        List<Chunk> actuals = execForMppMode(topNExec, input, 0, false);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testMultiFieldKey() {
        MockExec input = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .row("a", 1L)
            .row("b", 2L)
            .chunkBreak()
            .row("f", 3L)
            .row("a", 4L)
            .chunkBreak()
            .row("d", 5L)
            .row("d", 7L)
            .row("e", 6L)
            .buildExec();

        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0, 1),
                ImmutableList.of(RelFieldCollation.Direction.DESCENDING, RelFieldCollation.Direction.DESCENDING));
        SpilledTopNExec topNExec = new SpilledTopNExec(input.getDataTypes(), orderbys, 3, context);
        List<Chunk> expects = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .row("f", 3L)
            .row("e", 6L)
            .row("d", 7L)
            .build();
        List<Chunk> actuals = execForMppMode(topNExec, input, 0, false);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testReverseOrder() {
        MockExec input = rowChunksBuilder(DataTypes.LongType, DataTypes.DoubleType)
            .row(1L, 0.1)
            .row(2L, 0.2)
            .chunkBreak()
            .row(-1L, -0.1)
            .row(4L, 0.4)
            .chunkBreak()
            .row(5L, 0.5)
            .row(6L, 0.6)
            .row(7L, 0.7)
            .chunkBreak()
            .buildExec();

        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0),
                ImmutableList.of(RelFieldCollation.Direction.ASCENDING));
        SpilledTopNExec topNExec = new SpilledTopNExec(input.getDataTypes(), orderbys, 3, context);
        List<Chunk> expects = rowChunksBuilder(DataTypes.LongType, DataTypes.DoubleType)
            .row(-1L, -0.1)
            .row(1L, 0.1)
            .row(2L, 0.2)
            .build();
        List<Chunk> actuals = execForMppMode(topNExec, input, 0, false);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testCompaction() {
        MockExec input = rowChunksBuilder(DataTypes.StringType, DataTypes.DoubleType)
            .addSequenceChunk(200, 70, 70)
            .addSequenceChunk(1024, -300, -300)
            .addSequenceChunk(1024, 10000, 10000)
            .addSequenceChunk(1024, 22000, 13000)
            .addSequenceChunk(1024, 30000, 14000)
            .addSequenceChunk(1024, 40000, 15000)
            .addSequenceChunk(1024, 50000, 16000)
            .addSequenceChunk(1024, 60000, 17000)
            .addSequenceChunk(1024, 70000, 18000)
            .addSequenceChunk(1024, 70000, 19000)
            .buildExec();

        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(1),
                ImmutableList.of(RelFieldCollation.Direction.DESCENDING));
        SpilledTopNExec topNExec = new SpilledTopNExec(input.getDataTypes(), orderbys, 5, context);
        List<Chunk> expects = rowChunksBuilder(DataTypes.StringType, DataTypes.DoubleType)
            .row("71023", 20023.0)
            .row("71022", 20022.0)
            .row("71021", 20021.0)
            .row("71020", 20020.0)
            .row("71019", 20019.0)
            .build();
        List<Chunk> actuals = execForMppMode(topNExec, input, 0, false);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testSingleSpillable() {
        int limit = 100000;
        MockExec input = rowChunksBuilder(DataTypes.LongType)
            .addSequenceChunk(limit, limit * 2)
            .addSequenceChunk(limit, limit)
            .addSequenceChunk(limit, 0)
            .buildExec();

        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0),
                ImmutableList.of(RelFieldCollation.Direction.DESCENDING));
        SpilledTopNExec topNExec = new SpilledTopNExec(input.getDataTypes(), orderbys, limit, context, spillerFactory);

        RowChunksBuilder expectRows = rowChunksBuilder(DataTypes.LongType);
        for (int i = limit * 3; i > limit * 2; i--) {
            expectRows.row((long) (i - 1));
        }
        List<Chunk> expects = expectRows.build();
        List<Chunk> actuals = execForMppMode(topNExec, input, 1, false);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testTwoSpillable() {
        int limit = 100000;
        MockExec input = rowChunksBuilder(DataTypes.LongType)
            .addSequenceChunk(limit, limit * 2)
            .addSequenceChunk(limit, limit)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .buildExec();

        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0),
                ImmutableList.of(RelFieldCollation.Direction.DESCENDING));
        SpilledTopNExec topNExec = new SpilledTopNExec(input.getDataTypes(), orderbys, limit, context, spillerFactory);

        RowChunksBuilder expectRows = rowChunksBuilder(DataTypes.LongType);
        for (int i = limit * 3; i > limit * 2; i--) {
            expectRows.row((long) (i - 1));
        }
        List<Chunk> expects = expectRows.build();
        List<Chunk> actuals = execForMppMode(topNExec, input, 2, false);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testSingleSpillableWithBuild() {
        int limit = 100000;
        MockExec input = rowChunksBuilder(DataTypes.LongType)
            .addSequenceChunk(limit, limit * 2)
            .addSequenceChunk(limit, limit)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .buildExec();

        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0),
                ImmutableList.of(RelFieldCollation.Direction.DESCENDING));
        SpilledTopNExec topNExec = new SpilledTopNExec(input.getDataTypes(), orderbys, limit, context, spillerFactory);

        RowChunksBuilder expectRows = rowChunksBuilder(DataTypes.LongType);
        for (int i = limit * 3; i > limit * 2; i--) {
            expectRows.row((long) (i - 1));
        }
        List<Chunk> expects = expectRows.build();
        List<Chunk> actuals = execForMppMode(topNExec, input, 1, true);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testTwoSpillableWithBuild() {
        int limit = 100000;
        MockExec input = rowChunksBuilder(DataTypes.LongType)
            .addSequenceChunk(limit, limit * 2)
            .addSequenceChunk(limit, limit)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .addSequenceChunk(limit, 0)
            .buildExec();

        List<OrderByOption> orderbys =
            getOrderBys(ImmutableList.of(0),
                ImmutableList.of(RelFieldCollation.Direction.DESCENDING));
        SpilledTopNExec topNExec = new SpilledTopNExec(input.getDataTypes(), orderbys, limit, context, spillerFactory);

        RowChunksBuilder expectRows = rowChunksBuilder(DataTypes.LongType);
        for (int i = limit * 3; i > limit * 2; i--) {
            expectRows.row((long) (i - 1));
        }
        List<Chunk> expects = expectRows.build();
        List<Chunk> actuals = execForMppMode(topNExec, input, 2, true);
        assertExecResultByRow(actuals, expects, true);
    }

    @Test
    public void testTopN1() {
        int limit = 100;
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

        baseTest(orderbys, inputBuilder, limit, 3, false);
        baseTest(orderbys, inputBuilder, limit, 3, true);
        baseTest(orderbys, inputBuilder, limit, 10, true);
    }

    @Test
    public void testTopN2() {
        int limit = 1027;
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
            .addSequenceChunk(1024, 80000, 19000);

        baseTest(orderbys, inputBuilder, limit, 3, false);
        baseTest(orderbys, inputBuilder, limit, 3, true);
        baseTest(orderbys, inputBuilder, limit, 10, true);
    }

    @Test
    public void testTopN3() {
        int limit = 2033;
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
        SpilledTopNExec topNExec1 = new SpilledTopNExec(input.getDataTypes(), orderbys, limit, context, spillerFactory);
        execForMppMode(topNExec1, input, 5, true, new Callable() {
            @Override
            public Object call() throws Exception {
                Assert.assertEquals(0, topNExec1.getMemoryAllocatorCtx().getAllAllocated());
                return null;
            }
        });
    }

    public void baseTest(List<OrderByOption> orderbys, RowChunksBuilder inputBuilder, int limit, int revokeChunkNum,
                         boolean revokeAfterBuild) {
        MockExec input1 = inputBuilder.buildExec();
        MockExec input2 = inputBuilder.buildExec();

        SpilledTopNExec topNExec1 = new SpilledTopNExec(
            input1.getDataTypes(), orderbys, limit, context, spillerFactory);

        SpilledTopNExec topNExec2 = new SpilledTopNExec(input2.getDataTypes(), orderbys, limit, context, null);

        List<Chunk> actuals = execForMppMode(topNExec1, input1, revokeChunkNum, revokeAfterBuild);
        List<Chunk> expects = execForMppMode(topNExec2, input2, 0, false);
        assertExecResultByRow(actuals, expects, limit);
    }

    private void assertExecResultByRow(List<Chunk> actuals, List<Chunk> expects, int limit) {
        List<String> actualRows = new ArrayList<>();

        for (Chunk actualChunk : actuals) {
            for (int i = 0; i < actualChunk.getPositionCount(); i++) {
                if (--limit >= 0) {
                    actualRows.add(actualChunk.rowAt(i).toString());
                }
            }
        }

        List<String> expectRows = new ArrayList<>();
        for (Chunk expectChunk : expects) {
            for (int i = 0; i < expectChunk.getPositionCount(); i++) {
                expectRows.add(expectChunk.rowAt(i).toString());
            }
        }
        assertWithMessage(" 顺序情况下返回结果不一致").that(actualRows.size())
            .isEqualTo(expectRows.size());
        for (int i = 0; i < actualRows.size(); i++) {
            assertWithMessage(" 顺序情况下返回结果不一致").that(actualRows.get(i))
                .isEqualTo(expectRows.get(i));
        }
    }
}

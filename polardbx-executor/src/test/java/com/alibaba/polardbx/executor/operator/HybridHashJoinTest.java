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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SyncFileCleaner;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.google.common.collect.ImmutableList;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;

@RunWith(value = Parameterized.class)
public class HybridHashJoinTest extends BaseExecTest {

    private static AsyncFileSingleStreamSpillerFactory spillerFactory;
    private static Path tempPath = Paths.get("./tmp/" + UUID.randomUUID());

    private List<Integer> partitionChannels = ImmutableList.of(0);
    private DataType keyType;
    private int bucketNum;
    private int partititionNum = 1;
    private List<DataType> types;

    private final int kindOfTestKeyNum = 32;
    private int[] keysWithHash;

    public HybridHashJoinTest(DataType keyType, int bucketNum) {
        this.bucketNum = bucketNum;
        this.keyType = keyType;
        this.types = ImmutableList.of(keyType, DataTypes.LongType);
        keysWithHash = new int[kindOfTestKeyNum];
        for (int hashcode = 0; hashcode < kindOfTestKeyNum; hashcode++) {
            keysWithHash[hashcode] = -1;
            for (int key = 0; key < 1000000; key++) {
                if (ExecUtils.partition(keyType.convertFrom(key).hashCode(), kindOfTestKeyNum, true) == hashcode) {
                    keysWithHash[hashcode] = key;
                    String x = (String) keyType.convertFrom(key);
                    break;
                }
            }
        }

    }

    @Parameterized.Parameters(name = "key={0},bucketNum={1}")
    public static List<Object[]> prepare() {

        Object[][] object = {
            {DataTypes.StringType, 1},
            {DataTypes.StringType, 2},
            {DataTypes.StringType, 4},
            {DataTypes.StringType, 8}};

        return Arrays.asList(object);
    }

    @BeforeClass
    public static void beforeClass() {
        List<Path> spillPaths = new ArrayList<>();
        spillPaths.add(tempPath);
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().addAll(spillPaths);
        spillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), spillPaths, 4);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        MoreFiles.deleteRecursively(tempPath, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 2);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testMemoryHybridHashInnerJoin() {

        RowChunksBuilder innerInputBuilder = rowChunksBuilder(types)
            .addSequenceChunk(20, 20, 200)
            .addSequenceChunk(20, 20, 200)
            .addSequenceChunk(20, 30, 300)
            .addSequenceChunk(20, 40, 400);

        RowChunksBuilder outerInputBuilder = rowChunksBuilder(types)
            .row("20", 123_000L)
            .row("20", 123_000L)
            .chunkBreak()
            .addSequenceChunk(20, 0, 123_000)
            .addSequenceChunk(30, 30, 123_000);

        Executor innerInput = innerInputBuilder.buildBucketExec(bucketNum, partitionChannels);
        Executor outerInput = outerInputBuilder.buildBucketExec(bucketNum, partitionChannels);

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, context,
            1, 0,
            bucketNum, spillerFactory);

        ParallelHashJoinExec parallelHashJoinExec = new ParallelHashJoinExec(
            new ParallelHashJoinExec.Synchronizer(1, false),
            outerInputBuilder.buildExec(), innerInputBuilder.buildExec(), JoinRelType.INNER, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, false, context, 0);

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 0, false, 0);
        List<Chunk> expects = execForJoinMppMode(parallelHashJoinExec, innerInputBuilder.buildExec());
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testHybridHashInnerJoin1() {

        RowChunksBuilder innerInputBuilder = rowChunksBuilder(types)
            .addSequenceChunk(20, 20, 200)
            .addSequenceChunk(20, 20, 200)
            .addSequenceChunk(20, 30, 300)
            .addSequenceChunk(20, 40, 400);

        RowChunksBuilder outerInputBuilder = rowChunksBuilder(types)
            .row("20", 123_000L)
            .row("20", 123_000L)
            .chunkBreak()
            .addSequenceChunk(20, 0, 123_000)
            .addSequenceChunk(30, 30, 123_000);

        Executor innerInput = innerInputBuilder.buildBucketExec(bucketNum, partitionChannels);
        Executor outerInput = outerInputBuilder.buildBucketExec(bucketNum, partitionChannels);

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, context,
            1, 0,
            bucketNum, spillerFactory);

        ParallelHashJoinExec parallelHashJoinExec = new ParallelHashJoinExec(
            new ParallelHashJoinExec.Synchronizer(1, false),
            outerInputBuilder.buildExec(), innerInputBuilder.buildExec(), JoinRelType.INNER, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, false, context, 0);

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 3, true, 3);
        List<Chunk> expects = execForJoinMppMode(parallelHashJoinExec, innerInputBuilder.buildExec());
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testMemoryHybridHashLeftJoin() {

        RowChunksBuilder innerInputBuilder = rowChunksBuilder(types)
            .addSequenceChunk(20, 20, 200)
            .addSequenceChunk(20, 20, 200)
            .addSequenceChunk(20, 50, 300)
            .addSequenceChunk(20, 40, 400)
            .addSequenceChunk(20, 80, 200)
            .addSequenceChunk(20, 30, 300)
            .addSequenceChunk(20, 100, 400)
            .row(null, 123_000L)
            .chunkBreak();

        RowChunksBuilder outerInputBuilder = rowChunksBuilder(types)
            .row("20", 123_000L)
            .row("20", 123_000L)
            .row(null, 123_000L)
            .chunkBreak()
            .addSequenceChunk(1, 50, 123_000)
            .addSequenceChunk(10, 50, 123_000)
            .addSequenceChunk(20, 100, 123_000);

        Executor innerInput = innerInputBuilder.buildBucketExec(bucketNum, partitionChannels);
        Executor outerInput = outerInputBuilder.buildBucketExec(bucketNum, partitionChannels);

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.LEFT, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, context,
            1, 0,
            bucketNum, spillerFactory);

        ParallelHashJoinExec parallelHashJoinExec = new ParallelHashJoinExec(
            new ParallelHashJoinExec.Synchronizer(1, false),
            outerInputBuilder.buildExec(), innerInputBuilder.buildExec(), JoinRelType.LEFT, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, false, context, 0);

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 0, false, 0);
        List<Chunk> expects = execForJoinMppMode(parallelHashJoinExec, innerInputBuilder.buildExec());
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testMemoryHybridHashLeftJoin1() {

        RowChunksBuilder innerInputBuilder = rowChunksBuilder(types)
            .addSequenceChunk(20, 20, 200)
            .addSequenceChunk(20, 20, 200)
            .addSequenceChunk(20, 50, 300)
            .addSequenceChunk(20, 40, 400)
            .addSequenceChunk(20, 80, 200)
            .addSequenceChunk(20, 30, 300)
            .addSequenceChunk(20, 100, 400)
            .row(null, 123_000L)
            .chunkBreak();

        RowChunksBuilder outerInputBuilder = rowChunksBuilder(types)
            .row("20", 123_000L)
            .row("20", 123_000L)
            .row(null, 123_000L)
            .chunkBreak()
            .addSequenceChunk(1, 50, 123_000)
            .addSequenceChunk(10, 50, 123_000)
            .addSequenceChunk(20, 100, 123_000);

        Executor innerInput = innerInputBuilder.buildBucketExec(bucketNum, partitionChannels);
        Executor outerInput = outerInputBuilder.buildBucketExec(bucketNum, partitionChannels);

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.LEFT, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, context,
            1, 0,
            bucketNum, spillerFactory);

        ParallelHashJoinExec parallelHashJoinExec = new ParallelHashJoinExec(
            new ParallelHashJoinExec.Synchronizer(1, false),
            outerInputBuilder.buildExec(), innerInputBuilder.buildExec(), JoinRelType.LEFT, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, false, context, 0);

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 3, false, 3);
        List<Chunk> expects = execForJoinMppMode(parallelHashJoinExec, innerInputBuilder.buildExec());
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testMemoryHybridHashRightJoin() {
        RowChunksBuilder innerInputBuilder = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 30, 300)
            .addSequenceChunk(4, 40, 400)
            .addSequenceChunk(20, 80, 123_000)
            .addSequenceChunk(100, 100, 123_000)
            .row(null, 123_000L)
            .chunkBreak();

        RowChunksBuilder outerInputBuilder = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L)
            .chunkBreak()
            .row(null, 123_000L)
            .chunkBreak()
            .addSequenceChunk(2, 50, 123_000)
            .addSequenceChunk(20, 80, 123_000)
            .addSequenceChunk(100, 100, 123_000);

        Executor innerInput = innerInputBuilder.buildBucketExec(bucketNum, partitionChannels);
        Executor outerInput = outerInputBuilder.buildBucketExec(bucketNum, partitionChannels);

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.RIGHT, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, context,
            1, 0,
            bucketNum, spillerFactory);

        ParallelHashJoinExec parallelHashJoinExec = new ParallelHashJoinExec(
            new ParallelHashJoinExec.Synchronizer(1, false),
            outerInputBuilder.buildExec(), innerInputBuilder.buildExec(), JoinRelType.RIGHT, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, false, context, 0);

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 0, false, 0);
        List<Chunk> expects = execForJoinMppMode(parallelHashJoinExec, innerInputBuilder.buildExec());
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testMemoryHybridHashRightJoin1() {
        RowChunksBuilder innerInputBuilder = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 30, 300)
            .addSequenceChunk(4, 40, 400)
            .addSequenceChunk(20, 80, 123_000)
            .addSequenceChunk(100, 100, 123_000)
            .row(null, 123_000L)
            .chunkBreak();

        RowChunksBuilder outerInputBuilder = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L)
            .chunkBreak()
            .row(null, 123_000L)
            .chunkBreak()
            .addSequenceChunk(2, 50, 123_000)
            .addSequenceChunk(20, 80, 123_000)
            .addSequenceChunk(100, 100, 123_000);

        Executor innerInput = innerInputBuilder.buildBucketExec(bucketNum, partitionChannels);
        Executor outerInput = outerInputBuilder.buildBucketExec(bucketNum, partitionChannels);

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.RIGHT, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, context,
            1, 0,
            bucketNum, spillerFactory);

        ParallelHashJoinExec parallelHashJoinExec = new ParallelHashJoinExec(
            new ParallelHashJoinExec.Synchronizer(1, false),
            outerInputBuilder.buildExec(), innerInputBuilder.buildExec(), JoinRelType.RIGHT, false,
            ImmutableList.of(new EquiJoinKey(0, 0, keyType, false)), null, null, false, context, 0);

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 3, true, 3);
        List<Chunk> expects = execForJoinMppMode(parallelHashJoinExec, innerInputBuilder.buildExec());
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testSpillInBuild() {
        MockExec innerInput = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 30, 300)
            .addSequenceChunk(4, 40, 400)
            .addSequenceChunk(8024, 100, 400)
            .addSequenceChunk(4, 70, 400)
            .addSequenceChunk(4, 70, 400)
            .buildBucketExec(bucketNum, ImmutableList.of(0));

        MockExec outerInput = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L)
            .row("20", 123_000L)
            .chunkBreak()
            .addSequenceChunk(20, 0, 123_000)
            .addSequenceChunk(10, 30, 123_000)
            .buildBucketExec(bucketNum, ImmutableList.of(0));

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false,
            ImmutableList.of(new EquiJoinKey(0, 0, DataTypes.StringType, false)), null, null, context,
            partititionNum, 0,
            bucketNum, spillerFactory);

        List<Chunk> expects = rowChunksBuilder(
            DataTypes.StringType, DataTypes.LongType, DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("30", 123_000L, "30", 300L)
            .row("31", 123_001L, "31", 301L)
            .row("32", 123_002L, "32", 302L)
            .row("33", 123_003L, "33", 303L)
            .build();

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 2, false, 0);
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testSpillAfterBuild() {
        MockExec innerInput = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 30, 300)
            .addSequenceChunk(4, 40, 400)
            .addSequenceChunk(8024, 100, 400)
            .addSequenceChunk(4, 70, 400)
            .addSequenceChunk(4, 70, 400)
            .buildBucketExec(bucketNum, ImmutableList.of(0));

        MockExec outerInput = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L)
            .row("20", 123_000L)
            .chunkBreak()
            .addSequenceChunk(20, 0, 123_000)
            .addSequenceChunk(10, 30, 123_000)
            .buildBucketExec(bucketNum, ImmutableList.of(0));

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false,
            ImmutableList.of(new EquiJoinKey(0, 0, DataTypes.StringType, false)), null, null, context,
            partititionNum, 0,
            bucketNum, spillerFactory);

        List<Chunk> expects = rowChunksBuilder(
            DataTypes.StringType, DataTypes.LongType, DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("30", 123_000L, "30", 300L)
            .row("31", 123_001L, "31", 301L)
            .row("32", 123_002L, "32", 302L)
            .row("33", 123_003L, "33", 303L)
            .build();

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 0, true, 0);
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testSpillInProbe() {
        MockExec innerInput = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 30, 300)
            .addSequenceChunk(4, 40, 400)
            .addSequenceChunk(8024, 100, 400)
            .addSequenceChunk(4, 70, 400)
            .addSequenceChunk(4, 70, 400)
            .buildBucketExec(bucketNum, ImmutableList.of(0));

        MockExec outerInput = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L)
            .row("20", 123_000L)
            .chunkBreak()
            .addSequenceChunk(20, 0, 123_000)
            .addSequenceChunk(10, 30, 123_000)
            .buildBucketExec(bucketNum, ImmutableList.of(0));

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false,
            ImmutableList.of(new EquiJoinKey(0, 0, DataTypes.StringType, false)), null, null, context,
            partititionNum, 0,
            bucketNum, spillerFactory);

        List<Chunk> expects = rowChunksBuilder(
            DataTypes.StringType, DataTypes.LongType, DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("30", 123_000L, "30", 300L)
            .row("31", 123_001L, "31", 301L)
            .row("32", 123_002L, "32", 302L)
            .row("33", 123_003L, "33", 303L)
            .build();

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 0, false, 2);
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testSpillInAllStages() {
        MockExec innerInput = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 30, 300)
            .addSequenceChunk(4, 40, 400)
            .addSequenceChunk(8024, 100, 400)
            .addSequenceChunk(4, 70, 400)
            .addSequenceChunk(4, 70, 400)
            .buildBucketExec(bucketNum, ImmutableList.of(0));

        MockExec outerInput = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L)
            .row("20", 123_000L)
            .chunkBreak()
            .addSequenceChunk(20, 0, 123_000)
            .addSequenceChunk(10, 30, 123_000)
            .buildBucketExec(bucketNum, ImmutableList.of(0));

        HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false,
            ImmutableList.of(new EquiJoinKey(0, 0, DataTypes.StringType, false)), null, null, context,
            partititionNum, 0,
            bucketNum, spillerFactory);

        List<Chunk> expects = rowChunksBuilder(
            DataTypes.StringType, DataTypes.LongType, DataTypes.StringType, DataTypes.LongType)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("20", 123_000L, "20", 200L)
            .row("30", 123_000L, "30", 300L)
            .row("31", 123_001L, "31", 301L)
            .row("32", 123_002L, "32", 302L)
            .row("33", 123_003L, "33", 303L)
            .build();

        List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 2, true, 2);
        assertExecResultByRow(actuals, expects, false);
    }

    @Test
    public void testFewSameKey() {

        String[] keyRow = new String[8];
        long[] valRow = new long[8];

        for (int key = 0; key < 4; key++) {
            for (int i = 0; i < 2; i++) {
                keyRow[key * 2 + i] = String.valueOf(keysWithHash[key]);
                valRow[key * 2 + i] = 100_000_000 + +1_000 * key + i;
            }
        }
        RowChunksBuilder expectBuilder =
            rowChunksBuilder(DataTypes.StringType, DataTypes.LongType, DataTypes.StringType, DataTypes.LongType);
        for (int key = 0; key < 4; key++) {
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 2; j++) {
                    int ii = key * 2 + i;
                    int jj = key * 2 + j;
                    expectBuilder.row(keyRow[ii], valRow[ii], keyRow[jj], valRow[jj]);
                }
            }
        }
        List<Chunk> expects = expectBuilder.build();

        int builderNum = 35;
        int curBuilderId = 0;
        RowChunksBuilder[] builders = new RowChunksBuilder[builderNum];
        for (int i = 0; i < builderNum; i++) {
            builders[i] = rowChunksBuilder(DataTypes.StringType, DataTypes.LongType);
        }
        for (int mask = 0; mask < (1 << 7); mask++) {
            if (Integer.bitCount(mask) != 4) {
                continue;
            }
            int curKey0row = 0;
            int curKey1row = 4;
            for (int i = 0; i < 8; i++) {
                if ((mask & (1 << i)) == 0) {
                    builders[curBuilderId].row(keyRow[curKey0row], valRow[curKey0row]);
                    curKey0row++;
                } else {
                    builders[curBuilderId].row(keyRow[curKey1row], valRow[curKey1row]);
                    curKey1row++;
                }
                if (i % 2 == 1 && i != 7) {
                    builders[curBuilderId].chunkBreak();
                }
            }
            curBuilderId++;
        }

        for (int i = 0; i < 1; i++) {
            for (int j = 0; j < 1; j++) {
                MockExec innerInput = builders[i].buildBucketExec(bucketNum, ImmutableList.of(0));
                MockExec outerInput = builders[j].buildBucketExec(bucketNum, ImmutableList.of(0));
                HybridHashJoinExec joinExec = new HybridHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false,
                    ImmutableList.of(new EquiJoinKey(0, 0, DataTypes.StringType, false)), null, null, context,
                    partititionNum, 0,
                    bucketNum, spillerFactory);
                List<Chunk> actuals = execForHybridJoinMppMode(joinExec, innerInput, 1, true, 1);
                assertExecResultByRow(actuals, expects, false);

            }
        }
    }

    private List<Chunk> execForHybridJoinMppMode(
        HybridHashJoinExec join, Executor producer, int revokeNumInBuild, boolean revokeAfterBuild,
        int revokeNumInProbe) {

        SingleExecTest test = new SingleExecTest.Builder(join, producer)
            .setRevoke(revokeNumInBuild, revokeAfterBuild, revokeNumInProbe, 99).build();
        test.exec();
        return test.result();
    }

    private List<Chunk> execForJoinMppMode(ParallelHashJoinExec join, Executor producer) {
        SingleExecTest test = new SingleExecTest.Builder(join, producer).build();
        test.exec();
        return test.result();
    }
}

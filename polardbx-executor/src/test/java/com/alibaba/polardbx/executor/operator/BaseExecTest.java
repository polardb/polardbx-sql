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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;
import static io.airlift.concurrent.MoreFutures.getFutureValue;

public abstract class BaseExecTest {

    public final ExecutionContext context;

    {
        // mock context
        context = new ExecutionContext();
        context.setMemoryPool(
            MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "test", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));
        context.setQuerySpillSpaceMonitor(new QuerySpillSpaceMonitor("test"));
    }

    public static void assertExecResults(Executor exec, Chunk... results) {
        assertExecResults(exec, false, results);
    }

    public static void assertExecResults(Executor exec, boolean checkPartition, Chunk... results) {
        exec.open();
        try {
            Chunk chunk;
            int chunkIndex = 0;
            while (!exec.produceIsFinished() && (chunk = exec.nextChunk()) != null) {
                if (chunkIndex < results.length) {
                    assertChunkEquals(results[chunkIndex], chunk);
                    if (checkPartition) {
                        Assert.assertTrue(chunk.getPartIndex() == results[chunkIndex].getPartIndex(),
                            "part index not matched");
                        Assert.assertTrue(chunk.getPartCount() == results[chunkIndex].getPartCount(),
                            "part count not matched");
                    }
                }
                chunkIndex++;
            }
            int length = 0;
            if (results != null) {
                length = results.length;
            }
            if (chunkIndex != length) {
                throw new AssertionError("Expect " + results.length + " chunks but actually got " + chunkIndex);
            }
        } finally {
            exec.close();
        }
    }

    public static void assertExecResultByRow(List<Chunk> actuals, List<Chunk> expects, boolean order) {
        List<String> actualRows = new ArrayList<>();
        for (Chunk actualChunk : actuals) {
            for (int i = 0; i < actualChunk.getPositionCount(); i++) {
                actualRows.add(stringify(actualChunk.rowAt(i)));
            }
        }

        List<String> expectRows = new ArrayList<>();
        for (Chunk expectChunk : expects) {
            for (int i = 0; i < expectChunk.getPositionCount(); i++) {
                expectRows.add(stringify(expectChunk.rowAt(i)));
            }
        }
        if (order) {
            assertWithMessage(" 顺序情况下返回结果不一致").that(actualRows.size())
                .isEqualTo(expectRows.size());
            for (int i = 0; i < actualRows.size(); i++) {
                assertWithMessage(" 顺序情况下返回结果不一致").that(actualRows.get(i))
                    .isEqualTo(expectRows.get(i));
            }
        } else {
            assertWithMessage(" 非顺序情况下返回结果不一致").that(actualRows)
                .containsExactlyElementsIn(expectRows);
        }
    }

    public static String stringify(Chunk.ChunkRow row) {
        StringBuilder sb = new StringBuilder();

        for (int col = 0; col < row.getColNum(); col++) {
            Object o = row.getObject(col);
            String str;
            if (o instanceof Slice) {
                str = ((Slice) o).toStringUtf8();
            } else {
                str = Objects.toString(o);
            }
            sb.append(col + ":" + str + " ");
        }

        return sb.toString();
    }

    static void assertExecResultChunksSize(List<Chunk> actuals, int expectedChunks, int expectedRows) {
        int countChunks = actuals.size();
        if (countChunks != expectedChunks) {
            throw new AssertionError("Expect " + expectedChunks + " chunks but actually got " + countChunks);
        }
        int countRows = 0;
        for (Chunk c : actuals) {
            countRows += c.getPositionCount();
        }
        if (countRows != expectedRows) {
            throw new AssertionError("Expect " + expectedRows + " rows but actually got " + countRows);
        }
    }

    static void assertExecResultSize(Executor exec, int expectedChunks, int expectedRows) {
        exec.open();
        try {
            Chunk chunk;
            int countChunks = 0;
            int countRows = 0;
            while ((chunk = exec.nextChunk()) != null) {
                countChunks++;
                countRows += chunk.getPositionCount();
            }
            if (countChunks != expectedChunks) {
                throw new AssertionError("Expect " + expectedChunks + " chunks but actually got " + countChunks);
            }
            if (countRows != expectedRows) {
                throw new AssertionError("Expect " + expectedRows + " rows but actually got " + countRows);
            }
        } finally {
            exec.close();
        }
    }

    static void assertExecError(Executor exec, String errorMessage) {
        exec.open();
        try {
            while (exec.nextChunk() != null) {
                // do nothing
            }
        } catch (Throwable ex) {
            if (ex.getMessage().contains(errorMessage)) {
                return;
            } else {
                throw new AssertionError("Expect exception with error message \""
                    + errorMessage + "\" but actual error message is \"" + ex.getMessage() + "\"");
            }
        } finally {
            exec.close();
        }
        throw new AssertionError("Expect an exception");
    }

    public static void assertChunkEquals(Chunk expected, Chunk actual) {
        if (!checkChunkEquals(expected, actual)) {
            StringBuilder s = new StringBuilder();
            s.append("Expected:\n");
            for (int i = 0; i < expected.getPositionCount(); i++) {
                s.append(i).append("\t:[").append(expected.rowAt(i).toString()).append("]\n");
            }
            s.append("Actual:\n");
            for (int i = 0; i < actual.getPositionCount(); i++) {
                s.append(i).append("\t:[").append(actual.rowAt(i).toString()).append("]\n");
            }
            throw new AssertionError("Chunks not equal\n" + s);
        }
    }

    private static boolean checkChunkEquals(Chunk c1, Chunk c2) {
        if (c1.getBlockCount() != c2.getBlockCount() || c1.getPositionCount() != c2.getPositionCount()) {
            return false;
        }
        int n = c1.getPositionCount();
        for (int i = 0; i < n; i++) {
            if (!c1.equals(i, c2, i)) {
                return false;
            }
        }
        return true;
    }

    protected static List<ColumnMeta> getColumnMetas(DataType... dataTypes) {
        return getColumnMetas(Arrays.asList(dataTypes));
    }

    public static List<ColumnMeta> getColumnMetas(Iterable<DataType<?>> dataTypes) {
        int i = 0;
        List<ColumnMeta> columns = new ArrayList<>();
        for (DataType<?> dataType : dataTypes) {
            columns.add(new ColumnMeta("MOCK_TABLE", "COLUMN_" + i, null,
                new Field("MOCK_TABLE", "COLUMN_" + i, dataType)));
            i += 1;
        }

        return columns;
    }

    protected static List<OrderByOption> getOrderBys(List<Integer> indexs,
                                                     List<RelFieldCollation.Direction> directions) {
        List<OrderByOption> orderByOptions = new ArrayList<>();
        for (int i = 0; i < indexs.size(); i++) {
            OrderByOption orderByOption = new OrderByOption(indexs.get(i),
                directions.get(i),
                RelFieldCollation.NullDirection.UNSPECIFIED);
            orderByOptions.add(orderByOption);
        }
        return orderByOptions;
    }

    public static void execForSmpMode(Executor exec, List<Chunk> expects, boolean order) {
        ImmutableList.Builder<Chunk> outputChunks = ImmutableList.builder();
        try {
            exec.open();
            Chunk chunk;
            while ((chunk = exec.nextChunk()) != null) {
                outputChunks.add(chunk);
            }
        } finally {
            exec.close();
        }
        assertExecResultByRow(outputChunks.build(), expects, order);
    }

    public static void execForSmpMode(Executor exec, List<Chunk> expects, int partIndex, int partCount) {
        ImmutableList.Builder<Chunk> outputChunks = ImmutableList.builder();
        try {
            exec.open();
            Chunk chunk;
            while ((chunk = exec.nextChunk()) != null) {
                Assert.assertTrue(chunk.getPartIndex() == partIndex, "part index not match");
                Assert.assertTrue(chunk.getPartCount() == partCount, "part count not match");
                outputChunks.add(chunk);
            }
        } finally {
            exec.close();
        }
        assertExecResultByRow(outputChunks.build(), expects, false);
    }

    protected static void handleMemoryRevoking(MemoryRevoker memoryRevoker) {
        ListenableFuture<?> future = memoryRevoker.startMemoryRevoke();
        getFutureValue(future);
        memoryRevoker.finishMemoryRevoke();
        memoryRevoker.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
    }

    public static List<Chunk> execForMppMode(
        Executor output, Executor input, int revokeChunkNum, boolean revokeAfterBuild) {
        return execForMppMode(output, input, revokeChunkNum, revokeAfterBuild, null);
    }

    public static List<Chunk> execForMppMode(
        Executor output, Executor input, int revokeChunkNum, boolean revokeAfterBuild, Callable afterRevoker) {
        MemoryRevoker memoryRevoker = (MemoryRevoker) output;
        ConsumerExecutor consumer = (ConsumerExecutor) output;
        boolean revokeMemory = false;
        int chunkCnt = 0;
        input.open();
        consumer.openConsume();
        while (true) {
            if (revokeMemory) {
                handleMemoryRevoking(memoryRevoker);
                if (afterRevoker != null) {
                    try {
                        afterRevoker.call();
                    } catch (Exception t) {
                        throw new RuntimeException(t);
                    }
                }
                revokeMemory = false;
            } else {
                if (consumer.needsInput()) {
                    Chunk ret = input.nextChunk();
                    chunkCnt++;
                    if (revokeChunkNum > 0 && chunkCnt % revokeChunkNum == 0) {
                        revokeMemory = true;
                    }
                    if (ret == null) {
                        break;
                    }
                    consumer.consumeChunk(ret);
                }
            }
        }
        consumer.buildConsume();
        if (revokeAfterBuild) {
            handleMemoryRevoking(memoryRevoker);
            if (afterRevoker != null) {
                try {
                    afterRevoker.call();
                } catch (Exception t) {
                    throw new RuntimeException(t);
                }
            }
        }
        output.open();
        ImmutableList.Builder<Chunk> outputChunks = ImmutableList.builder();
        try {
            Chunk chunk;
            while ((chunk = output.nextChunk()) != null) {
                outputChunks.add(chunk);
            }
        } finally {
            output.close();
        }
        return outputChunks.build();
    }

    private static final Random RANDOM = new Random();

    protected IntegerBlock randomIntegerBlock(int lowerBound, int upperBound, int rowCount, int nullCount) {
        Integer[] results = RANDOM.ints(rowCount, lowerBound, upperBound).boxed().toArray(Integer[]::new);

        // inject nulls.
        if (nullCount > 0) {
            for (int i = 0; i < results.length; i++) {
                if (RANDOM.nextInt(rowCount) <= nullCount) {
                    results[i] = null;
                }
            }
        }

        // System.out.println(Arrays.toString(results));

        return IntegerBlock.of(results);
    }

    protected LongBlock randomLongBlock(int lowerBound, int upperBound, int rowCount, int nullCount) {
        Long[] results = RANDOM.ints(rowCount, lowerBound, upperBound).boxed().map(Integer::longValue)
            .collect(Collectors.toList()).toArray(new Long[0]);

        // inject nulls.
        if (nullCount > 0) {
            for (int i = 0; i < results.length; i++) {
                if (RANDOM.nextInt(rowCount) <= nullCount) {
                    results[i] = null;
                }
            }
        }

        // System.out.println(Arrays.toString(results));

        return LongBlock.of(results);
    }

    public static LocalBlockDictionary dictOf(String... dictValues) {
        Slice[] slices = Arrays.stream(dictValues).map(s -> Slices.utf8Slice(s))
            .collect(Collectors.toList()).toArray(new Slice[0]);

        return new LocalBlockDictionary(slices);
    }

    public static SliceBlock sliceOf(ExecutionContext context, String... directValues) {
        Slice[] slices = Arrays.stream(directValues).map(s -> Slices.utf8Slice(s))
            .collect(Collectors.toList()).toArray(new Slice[0]);

        return sliceOf(false, context, slices);
    }

    public static SliceBlock sliceOf(boolean compatible, ExecutionContext context, String... directValues) {
        Slice[] slices = Arrays.stream(directValues).map(s -> Slices.utf8Slice(s))
            .collect(Collectors.toList()).toArray(new Slice[0]);

        return sliceOf(compatible, context, slices);
    }

    public static SliceBlock sliceOfDict(BlockDictionary dictionary, int... dictIds) {
        return sliceOfDict(false, dictionary, dictIds);
    }

    public static SliceBlock sliceOf(ExecutionContext context, Slice... directValues) {
        return sliceOf(false, context, directValues);
    }

    public static SliceBlock sliceOfDict(boolean compatible, BlockDictionary dictionary, int... dictIds) {
        return new SliceBlock((SliceType) DataTypes.VarcharType, 0, dictIds.length,
            null, dictionary, dictIds, compatible);
    }

    public static SliceBlock sliceOf(boolean compatible, ExecutionContext context, Slice... directValues) {
        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, 8,
            context, compatible);

        for (Slice slice : directValues) {
            blockBuilder.writeSlice(slice);
        }
        return (SliceBlock) blockBuilder.build();
    }
}

package com.alibaba.polardbx.qatest.dql.sharding.enums;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.LocalHashBucketFunction;
import com.alibaba.polardbx.executor.mpp.operator.PartitionedOutputCollector;
import com.alibaba.polardbx.executor.mpp.operator.RemotePartitionFunction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.dql.sharding.type.numeric.NumericTestBase;
import com.google.common.collect.Lists;
import org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(CommonCaseRunner.class)
public class TwoPhaseShuffleSkewTest {
    private static double EXPECT_SKEW_RATIO = 0.15;
    private static int BATCH_SIZE = 1024 * 100;
    private Chunk chunk;

    private String colName;

    private DataType type;

    private boolean isMultiCol;

    private int localShuffleCount;

    private int targetRemotePartition;

    public TwoPhaseShuffleSkewTest(Object colAndType, Object isMultiCol, Object localShuffleCount,
                                   Object targetRemotePartition) {
        this.colName = (String) ((Pair) colAndType).getKey();
        this.type = (DataType) ((Pair) colAndType).getValue();
        this.isMultiCol = (Boolean) isMultiCol;
        this.localShuffleCount = (Integer) localShuffleCount;
        this.targetRemotePartition = (Integer) targetRemotePartition;
    }

    @Parameterized.Parameters(name = "{index}:{0},{1},{2},{3}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(
            colAndTypes(), multiColMode(), localShuffleCount(), targetRemotePartition());
    }

    // TODO add datetime type check
    // TODO should check collation
    public static Object[] colAndTypes() {
        return new Pair[] {
            Pair.of(NumericTestBase.USMALLINT_TEST, DataTypes.USmallIntType),
            Pair.of(NumericTestBase.MEDIUMINT_TEST, DataTypes.MediumIntType),
            Pair.of(NumericTestBase.UMEDIUMINT_TEST, DataTypes.UMediumIntType),
            Pair.of(NumericTestBase.INT_TEST, DataTypes.IntegerType),
            Pair.of(NumericTestBase.UINT_TEST, DataTypes.UIntegerType),
            Pair.of(NumericTestBase.BIGINT_TEST, DataTypes.LongType),
            Pair.of(NumericTestBase.UBIGINT_TEST, DataTypes.ULongType),
            Pair.of(NumericTestBase.VARCHAR_TEST, DataTypes.VarcharType),
            Pair.of(NumericTestBase.CHAR_TEST, DataTypes.CharType),
        };
    }

    public static Object[] multiColMode() {
        return new Boolean[] {
            Boolean.FALSE,
            Boolean.TRUE
        };
    }

    public static Object[] localShuffleCount() {
        return new Integer[] {
            2,
            4,
            13,
            16,
        };
    }

    public static Object[] targetRemotePartition() {
        return new Integer[] {
            0,
            1,
            2,
            3,
        };
    }

    public static List<Object[]> cartesianProduct(Object[]... arrays) {
        List[] lists = Arrays.stream(arrays)
            .map(Arrays::asList)
            .toArray(List[]::new);
        List<List<Object>> result = Lists.cartesianProduct(lists);
        return result.stream()
            .map(List::toArray)
            .collect(Collectors.toList());
    }

    @Before
    public void prepare() {
        ExecutionContext ec = new ExecutionContext();
        ec.setEnableOssCompatible(Boolean.FALSE);
        ChunkBuilder chunkBuilder =
            new ChunkBuilder(isMultiCol ? Arrays.asList(type, type) : Arrays.asList(type), BATCH_SIZE,
                ec);
        for (int i = 0; i < BATCH_SIZE; ++i) {
            Object val = type.convertFrom(i);
            chunkBuilder.getBlockBuilders()[0].writeObject(val);
            if (isMultiCol) {
                val = type.convertFrom(i + 31);
                chunkBuilder.getBlockBuilders()[1].writeObject(val);
            }
            chunkBuilder.declarePosition();
        }
        chunk = chunkBuilder.build();
    }

    @Test
    public void checkSkew() {
        ExecutionContext ec = new ExecutionContext();
        ec.setEnableOssCompatible(Boolean.FALSE);
        ChunkBuilder chunkBuilder =
            new ChunkBuilder(isMultiCol ? Arrays.asList(type, type) : Arrays.asList(type), BATCH_SIZE,
                ec);

        // remote shuffle
        RemotePartitionFunction partitionFunction = new PartitionedOutputCollector.HashPartitionFunction(4,
            isMultiCol ? Arrays.asList(0, 1) : Arrays.asList(0));
        for (int pos = 0; pos < BATCH_SIZE; ++pos) {
            int partition = partitionFunction.getPartition(chunk, pos);
            if (partition == targetRemotePartition) {
                chunkBuilder.getBlockBuilders()[0].writeObject(chunk.getBlock(0).getObject(pos));
                if (isMultiCol) {
                    chunkBuilder.getBlockBuilders()[1].writeObject(chunk.getBlock(1).getObject(pos));
                }
                chunkBuilder.declarePosition();
            }
        }

        Chunk localChunk = chunkBuilder.build();

        int[] partitionCount = new int[localShuffleCount];

        // local shuffle
        LocalHashBucketFunction bucketFunction = new LocalHashBucketFunction(localShuffleCount);
        for (int pos = 0; pos < localChunk.getPositionCount(); ++pos) {
            int partition = bucketFunction.getPartition(localChunk, pos);
            partitionCount[partition]++;
        }

        // check local shuffle skew
        boolean allPartitionHit = Arrays.stream(partitionCount).allMatch(count -> count > 0);
        if (!allPartitionHit) {
            Assert.fail(String.format("data skewed, some partition has no data, partition result is %s",
                Arrays.stream(partitionCount).boxed().map(Object::toString).collect(Collectors.joining(","))));
        }
        double max = Arrays.stream(partitionCount).max().getAsInt();
        double min = Arrays.stream(partitionCount).min().getAsInt();
        double realSkewRatio = (max - min) / max;
        if (realSkewRatio >= EXPECT_SKEW_RATIO) {
            Assert.fail(String.format(
                "data skewed under tow phase shuffle, expect under %s, but %s, and max values is %s, min values is %s",
                EXPECT_SKEW_RATIO, realSkewRatio, max, min));
        }
    }
}

package com.alibaba.polardbx.qatest.dql.sharding.enums;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.NonConsistencyHasherUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@RunWith(CommonCaseRunner.class)
public class DateTimeFieldShuffleTest {
    private static int BATCH_SIZE = 1024;
    private Chunk chunk;

    private SearchDatumInfo[] searchDatumInfos = new SearchDatumInfo[BATCH_SIZE];

    private String colName;

    private DataType type;

    private boolean isMultiCol;

    public DateTimeFieldShuffleTest(Object colAndType, Object isMultiCol) {
        this.colName = (String) ((Pair) colAndType).getKey();
        this.type = (DataType) ((Pair) colAndType).getValue();
        this.isMultiCol = (Boolean) isMultiCol;
    }

    @Before
    public void prepare() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setEnableOssCompatible(Boolean.FALSE);
        ChunkBuilder chunkBuilder =
            new ChunkBuilder(isMultiCol ? Arrays.asList(type, type) : Arrays.asList(type), BATCH_SIZE,
                executionContext);
        for (int i = 0; i < BATCH_SIZE; ++i) {
            Object val = type.convertFrom(getGenerator(colName, i % 4 == 0).get());
            searchDatumInfos[i] = createDatum(val, type);
            Arrays.stream(chunkBuilder.getBlockBuilders()).forEach(builder -> builder.writeObject(val));
            chunkBuilder.declarePosition();
        }
        chunk = chunkBuilder.build();
    }

    @Test
    public void checkConsistency() {
        for (int i = 0; i < BATCH_SIZE; ++i) {
            long hashCodeForChunk = isMultiCol ? chunk.getBlock(0).hashCodeUseXxhash(i) : chunk.hashCodeUseXxhash(i);
            long hashCodeForPart = NonConsistencyHasherUtils.calcHashCode(searchDatumInfos[i]);
            Assert.assertTrue(hashCodeForChunk == hashCodeForPart, String.format(
                "hash code not match, chunk row is %s and it's hashcode is %s, while search datum is %s and it's hashcode is %s",
                chunk.rowAt(i).toString(), hashCodeForChunk, searchDatumInfos[i].toString(), hashCodeForPart));
        }
    }

    public static SearchDatumInfo createDatum(Object val, DataType type) {
        PartitionField partFiled = PartitionFieldBuilder.createField(type);
        String value = val instanceof Slice ? ((Slice) val).toStringUtf8() : String.valueOf(val);
        partFiled.store(val == null ? null : value, DataTypes.StringType);
        PartitionBoundVal boundVal = PartitionBoundVal.createPartitionBoundVal(partFiled,
            PartitionBoundValueKind.DATUM_NORMAL_VALUE);
        return new SearchDatumInfo(boundVal);
    }

    @Parameterized.Parameters(name = "{index}:{0},{1}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(
            colAndTypes(), multiColMode());
    }

    // TODO zero time of timestamp type can not be test like this
    public static Object[] colAndTypes() {
        return new Pair[] {
            Pair.of(TimeTypeUtil.DATE_TEST, DataTypes.DateType),
            // partition field 暂时不支持time类型
            // Pair.of(TimeTypeUtil.TIME_TEST, DataTypes.TimeType),
            // Pair.of(TimeTypeUtil.TIMESTAMP_TEST, DataTypes.TimestampType),
            Pair.of(TimeTypeUtil.DATETIME_TEST, DataTypes.DatetimeType)
        };
    }

    public static Object[] multiColMode() {
        return new Boolean[] {
            Boolean.FALSE,
            Boolean.TRUE
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

    public static Supplier<Object> getGenerator(final String col, boolean invalid) {
        if (invalid) {
            switch (col) {
            case TimeTypeUtil.DATETIME_TEST:
                return () -> {
                    return "0000-00-00 00:00:00";// 设置为0年0月0日
                };
            case TimeTypeUtil.TIMESTAMP_TEST:
                return () -> 0;
            case TimeTypeUtil.DATE_TEST:
                return () -> "0000-00-00";
            }

        }
        return TimeTypeUtil.getGenerator(col, false);
    }
}

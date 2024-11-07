package com.alibaba.polardbx.executor.shuffle;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.NonConsistencyHasherUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DirectHashRouteCollationTest {
    @Test
    public void testCharLatin1() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setEnableOssCompatible(Boolean.TRUE);
        DataType type = new CharType(CharsetName.LATIN1, CollationName.LATIN1_GENERAL_CI);
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(type, 1024, executionContext, executionContext.getParamManager().getBoolean(
                ConnectionParams.ENABLE_OSS_COMPATIBLE));
        List<SearchDatumInfo> searchDatumInfos = new ArrayList<>();
        writeBytes(type, new byte[] {'a', 'B', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'A', 'B', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'A', 'b', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b'}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b', ' ', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));

        Block block = blockBuilder.build();
        checkConsistency(block, searchDatumInfos);
    }

    @Test
    public void testCharGbk() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setEnableOssCompatible(Boolean.TRUE);
        DataType type = new CharType(CharsetName.GBK, CollationName.GBK_CHINESE_CI);
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(type, 1024, executionContext, executionContext.getParamManager().getBoolean(
                ConnectionParams.ENABLE_OSS_COMPATIBLE));
        List<SearchDatumInfo> searchDatumInfos = new ArrayList<>();
        writeBytes(type, new byte[] {'a', 'B', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'A', 'B', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'A', 'b', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b'}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b', ' ', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));

        Block block = blockBuilder.build();
        checkConsistency(block, searchDatumInfos);
    }

    @Test
    public void testCharGbk2() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setEnableOssCompatible(Boolean.TRUE);
        DataType type = new CharType(CharsetName.GBK, CollationName.GBK_CHINESE_CI);
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(type, 1024, executionContext, executionContext.getParamManager().getBoolean(
                ConnectionParams.ENABLE_OSS_COMPATIBLE));
        List<SearchDatumInfo> searchDatumInfos = new ArrayList<>();
        writeBytes(type, "我们".getBytes(Charset.forName("GBK")), searchDatumInfos, blockBuilder,
            Charset.forName("GBK"));
        writeBytes(type, "我们 ".getBytes(Charset.forName("GBK")), searchDatumInfos, blockBuilder,
            Charset.forName("GBK"));

        Block block = blockBuilder.build();
        checkConsistency(block, searchDatumInfos);
    }

    @Test
    public void testCharUtf8() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setEnableOssCompatible(Boolean.TRUE);
        DataType type = new CharType(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(type, 1024, executionContext, executionContext.getParamManager().getBoolean(
                ConnectionParams.ENABLE_OSS_COMPATIBLE));
        List<SearchDatumInfo> searchDatumInfos = new ArrayList<>();
        writeBytes(type, new byte[] {'a', 'B', ' '}, searchDatumInfos, blockBuilder, StandardCharsets.UTF_8);
        writeBytes(type, new byte[] {'A', 'B', ' '}, searchDatumInfos, blockBuilder, StandardCharsets.UTF_8);
        writeBytes(type, new byte[] {'A', 'b', ' '}, searchDatumInfos, blockBuilder, StandardCharsets.UTF_8);
        writeBytes(type, new byte[] {'a', 'b', ' '}, searchDatumInfos, blockBuilder, StandardCharsets.UTF_8);
        writeBytes(type, new byte[] {'a', 'b'}, searchDatumInfos, blockBuilder, StandardCharsets.UTF_8);
        writeBytes(type, new byte[] {'a', 'b', ' ', ' '}, searchDatumInfos, blockBuilder, StandardCharsets.UTF_8);

        Block block = blockBuilder.build();
        checkConsistency(block, searchDatumInfos);
    }

    @Test
    public void testCharUtf82() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setEnableOssCompatible(Boolean.TRUE);
        DataType type = new CharType(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(type, 1024, executionContext, executionContext.getParamManager().getBoolean(
                ConnectionParams.ENABLE_OSS_COMPATIBLE));
        List<SearchDatumInfo> searchDatumInfos = new ArrayList<>();
        writeBytes(type, "我们".getBytes(Charset.forName("GBK")), searchDatumInfos, blockBuilder,
            StandardCharsets.UTF_8);
        writeBytes(type, "我们 ".getBytes(Charset.forName("GBK")), searchDatumInfos, blockBuilder,
            StandardCharsets.UTF_8);

        Block block = blockBuilder.build();
        checkConsistency(block, searchDatumInfos);
    }

    @Test
    public void testVarCharLatin1() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setEnableOssCompatible(Boolean.TRUE);
        DataType type = new VarcharType(CharsetName.LATIN1, CollationName.LATIN1_GENERAL_CI);
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(type, 1024, executionContext, executionContext.getParamManager().getBoolean(
                ConnectionParams.ENABLE_OSS_COMPATIBLE));
        List<SearchDatumInfo> searchDatumInfos = new ArrayList<>();
        writeBytes(type, new byte[] {'a', 'B', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'A', 'B', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'A', 'b', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b'}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));
        writeBytes(type, new byte[] {'a', 'b', ' ', ' '}, searchDatumInfos, blockBuilder, Charset.forName("latin1"));

        Block block = blockBuilder.build();
        checkConsistency(block, searchDatumInfos);
    }

    @Test
    public void testVarCharUtf8Mb4() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setEnableOssCompatible(Boolean.TRUE);
        DataType type = new VarcharType(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        SliceBlockBuilder blockBuilder =
            new SliceBlockBuilder(type, 1024, executionContext, executionContext.getParamManager().getBoolean(
                ConnectionParams.ENABLE_OSS_COMPATIBLE));
        List<SearchDatumInfo> searchDatumInfos = new ArrayList<>();
        writeBytes(type, new byte[] {'a', 'B', ' '}, searchDatumInfos, blockBuilder, Charset.forName("utf8"));
        writeBytes(type, new byte[] {'A', 'B', ' '}, searchDatumInfos, blockBuilder, Charset.forName("utf8"));
        writeBytes(type, new byte[] {'A', 'b', ' '}, searchDatumInfos, blockBuilder, Charset.forName("utf8"));
        writeBytes(type, new byte[] {'a', 'b', ' '}, searchDatumInfos, blockBuilder, Charset.forName("utf8"));
        writeBytes(type, new byte[] {'a', 'b'}, searchDatumInfos, blockBuilder, Charset.forName("utf8"));
        writeBytes(type, new byte[] {'a', 'b', ' ', ' '}, searchDatumInfos, blockBuilder, Charset.forName("utf8"));

        Block block = blockBuilder.build();
        checkConsistency(block, searchDatumInfos);
    }

    private void writeBytes(DataType type, byte[] bytes, List<SearchDatumInfo> searchDatumInfos,
                            SliceBlockBuilder blockBuilder, Charset charset) {
        blockBuilder.writeString(new String(bytes, charset));
        PartitionField partFiled = PartitionFieldBuilder.createField(type);
        partFiled.store(bytes, DataTypes.CharType);
        PartitionBoundVal boundVal = PartitionBoundVal.createPartitionBoundVal(partFiled,
            PartitionBoundValueKind.DATUM_NORMAL_VALUE);
        searchDatumInfos.add(new SearchDatumInfo(boundVal));
    }

    public void checkConsistency(Block block, List<SearchDatumInfo> searchDatumInfos) {
        for (int i = 0; i < block.getPositionCount(); ++i) {
            long hashCodeForChunk = block.hashCodeUnderPairWise(i, true);
            long hashCodeForPart = NonConsistencyHasherUtils.calcHashCode(searchDatumInfos.get(i));
            Assert.assertTrue(hashCodeForChunk == hashCodeForPart, String.format(
                "hash code not match, chunk row is %s and it's hashcode is %s, while search datum is %s and it's hashcode is %s",
                block.getObject(i).toString(), hashCodeForChunk, searchDatumInfos.get(i).toString(), hashCodeForPart));
        }

        Assert.assertTrue(
            IntStream.range(0, block.getPositionCount()).mapToLong(block::hashCodeUseXxhash).distinct().count() == 1);
        Assert.assertTrue(IntStream.range(0, block.getPositionCount())
            .mapToLong(i -> NonConsistencyHasherUtils.calcHashCode(searchDatumInfos.get(i))).distinct().count() == 1);
    }

    @Test
    public void testCharFieldSkew() {
        DataType type = new CharType(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        testSkew(type);
        type = new CharType(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        testSkew(type);
    }

    @Test
    public void testVarCharFieldSkew() {
        DataType type = new VarcharType(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        testSkew(type);
        type = new VarcharType(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        testSkew(type);
    }

    private void testSkew(DataType type) {
        List<PartitionField> partitionFields = Arrays.asList(
            getPartitionField(type, "JPBS21082984490347130065".getBytes()),
            getPartitionField(type, "JPBR21073100350927170621".getBytes()),
            getPartitionField(type, "JPBS21082984340345560593".getBytes()),
            getPartitionField(type, "JPBS22080617230503440904".getBytes()),
            getPartitionField(type, "JPBS22071902121219350135".getBytes()),
            getPartitionField(type, "JPBS22071018910901310901".getBytes()),
            getPartitionField(type, "JPBS22043001810618580367".getBytes()),
            getPartitionField(type, "JPBS22072026671000490511".getBytes()),
            getPartitionField(type, "JPBS22072090520333570941".getBytes()),
            getPartitionField(type, "JPBS22072591990853040266".getBytes()),
            getPartitionField(type, "JPBS22072735210846060183".getBytes()),
            getPartitionField(type, "JPBS22070126010737400679".getBytes()),
            getPartitionField(type, "JPBS22071798090713090622".getBytes()),
            getPartitionField(type, "JPBS22072425270837500933".getBytes()),
            getPartitionField(type, "JPBS22072169520632570863".getBytes()),
            getPartitionField(type, "JPBS22043083710432430348".getBytes()),
            getPartitionField(type, "JPBS22042369890646300579".getBytes()),
            getPartitionField(type, "JPBS22072724010731420761".getBytes()),
            getPartitionField(type, "JPBS22072415891155260120".getBytes()),
            getPartitionField(type, "JPBS22072074361242110099".getBytes())
        );
        List<Long> partition = partitionFields.stream().map(filed -> filed.xxHashCode()).map(code -> code & 3).collect(
            Collectors.toList());
        Assert.assertTrue(partition.stream().distinct().count() == 4);

        Map<Long, Long> countMap = partition.stream()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        Long maxCount = Collections.max(countMap.values());
        Long minCount = Collections.min(countMap.values());

        Assert.assertTrue(maxCount <= 10 && minCount >= 2, "char filed skew");
    }

    private PartitionField getPartitionField(DataType type, byte[] bytes) {
        PartitionField partFiled = PartitionFieldBuilder.createField(type);
        partFiled.store(bytes, DataTypes.CharType);
        return partFiled;
    }
}

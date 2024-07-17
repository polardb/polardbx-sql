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
import java.util.List;
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
}

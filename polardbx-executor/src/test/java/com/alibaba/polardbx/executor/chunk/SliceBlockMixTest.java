package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SliceBlockMixTest extends BaseExecTest {

    private boolean compatible = false;
    private SliceBlock sliceBlock1;
    private SliceBlock sliceBlock2;
    private LocalBlockDictionary dict1;
    private SliceBlock dictBlock1;
    private LocalBlockDictionary dict2;
    private SliceBlock dictBlock2;

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1000);
        connectionMap.put(ConnectionParams.ENABLE_OSS_COMPATIBLE.getName(), compatible);
        context.setParamManager(new ParamManager(connectionMap));

        // prepare slice blocks.
        sliceBlock1 = sliceOf(compatible, context, "a", "b", "c", "ab", "bc", "ac");
        sliceBlock2 = sliceOf(compatible, context, "b", "a", "f", "ab", "bf", "af");

        dict1 = dictOf("a", "b", "f", "x");
        // "b", "a", "b", "f", "a", "x"
        dictBlock1 = sliceOfDict(dict1, 1, 0, 1, 2, 0, 3);

        dict2 = dictOf("x", "b", "c", "f");
        // "b", "x", "b", "c", "x", "c"
        dictBlock2 = sliceOfDict(dict2, 1, 0, 1, 2, 0, 2);
    }

    // case 1: slice -> dict1 -> dict2 -> slice
    @Test
    public void testMix1() {
        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(new VarcharType(), 4, context, compatible);

        doWrite(sliceBlock1, blockBuilder);
        doWrite(dictBlock1, blockBuilder);
        doWrite(dictBlock2, blockBuilder);
        doWrite(sliceBlock2, blockBuilder);

        Block block = blockBuilder.build();
        List<Chunk> actualChunks = ImmutableList.of(new Chunk(block));

        List<Chunk> expectedChunks = ImmutableList.of(new Chunk(sliceOf(compatible, context,
            "a", "b", "c", "ab", "bc", "ac",
            "b", "a", "b", "f", "a", "x",
            "b", "x", "b", "c", "x", "c",
            "b", "a", "f", "ab", "bf", "af"
        )));

        assertExecResultByRow(actualChunks, expectedChunks, true);
    }

    // case 2: dict1 -> slice -> dict2 -> slice
    @Test
    public void testMix2() {
        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(new VarcharType(), 4, context, compatible);

        doWrite(dictBlock1, blockBuilder);
        doWrite(sliceBlock1, blockBuilder);
        doWrite(dictBlock2, blockBuilder);
        doWrite(sliceBlock2, blockBuilder);

        Block block = blockBuilder.build();
        List<Chunk> actualChunks = ImmutableList.of(new Chunk(block));

        List<Chunk> expectedChunks = ImmutableList.of(new Chunk(sliceOf(compatible, context,
            "b", "a", "b", "f", "a", "x",
            "a", "b", "c", "ab", "bc", "ac",
            "b", "x", "b", "c", "x", "c",
            "b", "a", "f", "ab", "bf", "af"
        )));

        assertExecResultByRow(actualChunks, expectedChunks, true);
    }

    // case 3: dict1 -> dict2 -> slice -> slice
    @Test
    public void testMix3() {
        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(new VarcharType(), 4, context, compatible);

        doWrite(dictBlock1, blockBuilder);
        doWrite(dictBlock2, blockBuilder);
        doWrite(sliceBlock1, blockBuilder);
        doWrite(sliceBlock2, blockBuilder);

        Block block = blockBuilder.build();
        List<Chunk> actualChunks = ImmutableList.of(new Chunk(block));

        List<Chunk> expectedChunks = ImmutableList.of(new Chunk(sliceOf(compatible, context,
            "b", "a", "b", "f", "a", "x",
            "b", "x", "b", "c", "x", "c",
            "a", "b", "c", "ab", "bc", "ac",
            "b", "a", "f", "ab", "bf", "af"
        )));

        assertExecResultByRow(actualChunks, expectedChunks, true);
    }

    // case 4: slice -> dict1 -> slice -> dict2
    @Test
    public void testMix4() {
        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(new VarcharType(), 4, context, compatible);

        doWrite(sliceBlock1, blockBuilder);
        doWrite(dictBlock1, blockBuilder);
        doWrite(sliceBlock2, blockBuilder);
        doWrite(dictBlock2, blockBuilder);

        Block block = blockBuilder.build();
        List<Chunk> actualChunks = ImmutableList.of(new Chunk(block));

        List<Chunk> expectedChunks = ImmutableList.of(new Chunk(sliceOf(compatible, context,
            "a", "b", "c", "ab", "bc", "ac",
            "b", "a", "b", "f", "a", "x",
            "b", "a", "f", "ab", "bf", "af",
            "b", "x", "b", "c", "x", "c"
        )));

        assertExecResultByRow(actualChunks, expectedChunks, true);
    }

    private void doWrite(SliceBlock sourceBlock, SliceBlockBuilder targetBuilder) {
        for (int pos = 0; pos < sourceBlock.getPositionCount(); pos++) {
            sourceBlock.writePositionTo(pos, targetBuilder);
        }
    }
}

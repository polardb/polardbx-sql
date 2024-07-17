package com.alibaba.polardbx.qatest.dql.sharding.enums;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.EnumBlock;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class EnumBlockShuffleTest {
    @Test
    public void hashCodeUseEnumIndex() {
        // prepare block
        Map<String, Integer> enumValues = new HashMap<>();
        enumValues.put("Aa", 1);
        enumValues.put("Ab", 2);
        char[] data = new char[] {'A', 'a', 'A', 'b', 'A', 'a'};
        int[] offset = new int[] {2, 4, 6};
        EnumBlock block = new EnumBlock(0, 3, null, offset, data, enumValues);

        Assert.assertTrue(block.hashCodeUseXxhash(0) == XxhashUtils.finalShuffle(1));
        Assert.assertTrue(block.hashCodeUseXxhash(1) == XxhashUtils.finalShuffle(2));
        Assert.assertTrue(block.hashCodeUseXxhash(2) == XxhashUtils.finalShuffle(1));
    }

    @Test
    public void testWithNull() {
        // prepare block
        Map<String, Integer> enumValues = new HashMap<>();
        enumValues.put("Aa", 1);
        enumValues.put("Ab", 2);
        char[] data = new char[] {'A', 'a', 'A', 'b', 'A', 'a'};
        int[] offset = new int[] {2, 2, 4, 6};
        boolean[] valueIsNull = new boolean[] {false, true, false, false};
        EnumBlock block = new EnumBlock(0, 4, valueIsNull, offset, data, enumValues);

        Assert.assertTrue(block.hashCodeUseXxhash(0) == XxhashUtils.finalShuffle(1));
        Assert.assertTrue(block.hashCodeUseXxhash(1) == Block.NULL_HASH_CODE);
        Assert.assertTrue(block.hashCodeUseXxhash(2) == XxhashUtils.finalShuffle(2));
        Assert.assertTrue(block.hashCodeUseXxhash(3) == XxhashUtils.finalShuffle(1));
    }

    @Test
    public void testWithInvalidValue() {
        Map<String, Integer> enumValues = new HashMap<>();
        enumValues.put("Aa", 1);
        enumValues.put("Ab", 2);
        char[] data = new char[] {'A', 'a', 'A', 'b', 'A', 'a', 'c'};
        int[] offset = new int[] {2, 2, 4, 6, 7, 7};
        boolean[] valueIsNull = new boolean[] {false, true, false, false, false, false};
        EnumBlock block = new EnumBlock(0, 8, valueIsNull, offset, data, enumValues);

        Assert.assertTrue(block.hashCodeUseXxhash(0) == XxhashUtils.finalShuffle(1));
        Assert.assertTrue(block.hashCodeUseXxhash(1) == Block.NULL_HASH_CODE);
        Assert.assertTrue(block.hashCodeUseXxhash(2) == XxhashUtils.finalShuffle(2));
        Assert.assertTrue(block.hashCodeUseXxhash(3) == XxhashUtils.finalShuffle(1));
        Assert.assertTrue(block.hashCodeUseXxhash(4) == XxhashUtils.finalShuffle(0));
        Assert.assertTrue(block.hashCodeUseXxhash(5) == XxhashUtils.finalShuffle(0));
    }
}

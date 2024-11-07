package com.alibaba.polardbx.executor.chunk;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class EnumBlockTest extends BaseBlockTest {
    private final int count = 16;
    private Map<String, Integer> enumMap;
    private int[] sel;

    @Before
    public void before() {
        this.enumMap = new HashMap<>();
        this.enumMap.put("a", 1);
        this.enumMap.put("b", 2);
        this.enumMap.put("c", 3);
        this.sel = new int[count / 2];
        for (int i = 0; i < count / 2; i++) {
            sel[i] = i * 2;
        }
    }

    @Test
    public void test() {
        EnumBlockBuilder blockBuilder = new EnumBlockBuilder(count, 16, enumMap);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeString(String.valueOf((char) ('a' + i % enumMap.size())));
        }
        blockBuilder.appendNull();
        EnumBlock fromBlock = (EnumBlock) blockBuilder.build();

        EnumBlock newBlock = EnumBlock.from(fromBlock, fromBlock.positionCount, null);
        for (int i = 0; i < count; i++) {
            Assert.assertEquals("Failed at pos: " + i, fromBlock.getString(i), newBlock.getString(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCode(i), newBlock.hashCode(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCodeUseXxhash(i), newBlock.hashCodeUseXxhash(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.checksum(i), newBlock.checksum(i));
            Assert.assertTrue("Failed at pos: " + i, fromBlock.equals(i, newBlock, i));
            Assert.assertTrue("Failed at pos: " + i, fromBlock.equals(i, blockBuilder, i));
        }
        EnumBlock newBlock2 = EnumBlock.from(fromBlock, sel.length, sel);
        // should do nothing after compact
        fromBlock.compact(sel);
        for (int i = 0; i < sel.length; i++) {
            int j = sel[i];
            Assert.assertEquals("Failed at pos: " + i, fromBlock.getString(j), newBlock2.getString(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCode(j), newBlock2.hashCode(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCodeUseXxhash(j), newBlock2.hashCodeUseXxhash(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.checksum(j), newBlock2.checksum(i));
        }
        Assert.assertTrue(newBlock.estimateSize() > 0);

        EnumBlockBuilder blockBuilder2 = new EnumBlockBuilder(count, 16, enumMap);
        for (int i = 0; i < count; i++) {
            fromBlock.writePositionTo(i, blockBuilder2);
        }
        EnumBlock newBlock3 = (EnumBlock) blockBuilder2.build();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals("Failed at pos: " + i, fromBlock.getString(i), newBlock3.getString(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCode(i), newBlock3.hashCode(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCodeUseXxhash(i), newBlock3.hashCodeUseXxhash(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.checksum(i), newBlock3.checksum(i));
        }
    }

}

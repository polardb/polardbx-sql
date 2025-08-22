package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FloatBlockTest extends BaseBlockTest {
    private final int count = 16;
    private int[] sel;

    @Before
    public void before() {
        this.sel = new int[count / 2];
        for (int i = 0; i < count / 2; i++) {
            sel[i] = i * 2;
        }
    }

    @Test
    public void test() {
        FloatBlockBuilder blockBuilder = new FloatBlockBuilder(count);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeFloat(i * 1.23F);
        }
        blockBuilder.appendNull();
        MemoryCountable.checkDeviation(blockBuilder, .05d, true);
        FloatBlock fromBlock = (FloatBlock) blockBuilder.build();
        MemoryCountable.checkDeviation(fromBlock, .05d, true);

        FloatBlock newBlock = FloatBlock.from(fromBlock, fromBlock.positionCount, null);
        MemoryCountable.checkDeviation(newBlock, .05d, true);
        int[] hashCodes = fromBlock.hashCodeVector();
        int[] newHashCodes = new int[hashCodes.length];
        newBlock.hashCodeVector(newHashCodes, newHashCodes.length);
        for (int i = 0; i < count; i++) {
            if (fromBlock.isNull(i)) {
                Assert.assertTrue(newBlock.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, fromBlock.getFloat(i), newBlock.getFloat(i), 1e-10);
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCode(i), newBlock.hashCode(i));
            Assert.assertEquals("Failed at pos: " + i, hashCodes[i], newBlock.hashCode(i));
            Assert.assertEquals("Failed at pos: " + i, hashCodes[i], newHashCodes[i]);
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCodeUseXxhash(i), newBlock.hashCodeUseXxhash(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.checksum(i), newBlock.checksum(i));
        }
        FloatBlock newBlock2 = FloatBlock.from(fromBlock, sel.length, sel);
        MemoryCountable.checkDeviation(newBlock2, .05d, true);
        for (int i = 0; i < sel.length; i++) {
            int j = sel[i];
            if (fromBlock.isNull(j)) {
                Assert.assertTrue(newBlock2.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, fromBlock.getFloat(j), newBlock2.getFloat(i), 1e-10);
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCode(j), newBlock2.hashCode(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCodeUseXxhash(j), newBlock2.hashCodeUseXxhash(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.checksum(j), newBlock2.checksum(i));
        }
        Assert.assertTrue(newBlock.estimateSize() > 0);

        FloatBlockBuilder blockBuilder2 = new FloatBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            fromBlock.writePositionTo(i, blockBuilder2);
        }
        MemoryCountable.checkDeviation(blockBuilder2, .05d, true);
        FloatBlock newBlock3 = (FloatBlock) blockBuilder2.build();
        MemoryCountable.checkDeviation(newBlock3, .05d, true);
        for (int i = 0; i < count; i++) {
            if (fromBlock.isNull(i)) {
                Assert.assertTrue(newBlock3.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, fromBlock.getFloat(i), newBlock3.getFloat(i), 1e-10);
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCode(i), newBlock3.hashCode(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCodeUseXxhash(i), newBlock3.hashCodeUseXxhash(i));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.checksum(i), newBlock3.checksum(i));
        }

        FloatBlock newBlock4 = new FloatBlock(DataTypes.FloatType, count);
        MemoryCountable.checkDeviation(newBlock4, .05d, true);
        fromBlock.copySelected(false, null, count, newBlock4);
        for (int i = 0; i < count; i++) {
            if (fromBlock.isNull(i)) {
                Assert.assertTrue(newBlock4.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, fromBlock.getFloat(i), newBlock4.getFloat(i), 1e-10);
        }
        FloatBlock newBlock5 = new FloatBlock(DataTypes.FloatType, count);
        MemoryCountable.checkDeviation(newBlock5, .05d, true);
        fromBlock.copySelected(true, sel, sel.length, newBlock5);
        for (int i = 0; i < sel.length; i++) {
            int j = sel[i];
            if (fromBlock.isNull(j)) {
                Assert.assertTrue(newBlock5.isNull(j));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + j, fromBlock.getFloat(j), newBlock5.getFloat(j), 1e-10);
        }

        FloatBlock newBlock6 = new FloatBlock(DataTypes.FloatType, count);
        // just compare reference since it is a shallow copy
        fromBlock.shallowCopyTo(newBlock6);
        Assert.assertSame(fromBlock.floatArray(), newBlock6.floatArray());
        MemoryCountable.checkDeviation(newBlock6, .05d, true);

        // compact should work
        fromBlock.compact(sel);
        for (int i = 0; i < sel.length; i++) {
            int j = sel[i];
            if (fromBlock.isNull(i)) {
                Assert.assertTrue(newBlock.isNull(j));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, fromBlock.getFloat(i), newBlock3.getFloat(j), 1e-10);
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCode(i), newBlock3.hashCode(j));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.hashCodeUseXxhash(i), newBlock3.hashCodeUseXxhash(j));
            Assert.assertEquals("Failed at pos: " + i, fromBlock.checksum(i), newBlock3.checksum(j));
        }
    }

}

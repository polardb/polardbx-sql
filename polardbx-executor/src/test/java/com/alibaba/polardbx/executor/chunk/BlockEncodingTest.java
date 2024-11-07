package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.collect.Lists;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockEncodingTest {

    @Test
    public void testEncodingBuilders() {
        ExecutionContext context = new ExecutionContext();
        List<DataType> typeList = Lists.newArrayList(DataTypes.IntegerType, DataTypes.LongType,
            DataTypes.ShortType, DataTypes.ByteType, DataTypes.DoubleType, DataTypes.FloatType,
            DataTypes.StringType, DataTypes.TimestampType, DataTypes.DateType, DataTypes.TimeType,
            new DecimalType(16, 2), DataTypes.BigBitType, DataTypes.BytesType, DataTypes.BlobType,
            DataTypes.ClobType, new EnumType(Lists.newArrayList("a", "b", "c")), new SliceType(),
            DataTypes.ULongType);
        List<BlockEncoding> blockEncodings = BlockEncodingBuilders.create(typeList, context);
        Assert.assertEquals(typeList.size(), blockEncodings.size());
        List<Class> expectClasses = Lists.newArrayList(IntegerBlockEncoding.class, LongBlockEncoding.class,
            ShortBlockEncoding.class, ByteBlockEncoding.class, DoubleBlockEncoding.class, FloatBlockEncoding.class,
            StringBlockEncoding.class, TimestampBlockEncoding.class, DateBlockEncoding.class, TimeBlockEncoding.class,
            DecimalBlockEncoding.class, BigIntegerBlockEncoding.class, ByteArrayBlockEncoding.class,
            BlobBlockEncoding.class, ClobBlockEncoding.class, EnumBlockEncoding.class, SliceBlockEncoding.class,
            ULongBlockEncoding.class);
        for (int i = 0; i < typeList.size(); i++) {
            Assert.assertEquals("Not match at pos: " + i, expectClasses.get(i), blockEncodings.get(i).getClass());
        }

        context.getParamManager().getProps().put("ENABLE_ORC_RAW_TYPE_BLOCK", "true");
        List<BlockEncoding> blockEncodings2 = BlockEncodingBuilders.create(typeList, context);
        Assert.assertEquals(typeList.size(), blockEncodings.size());
        List<Class> expectClasses2 = Lists.newArrayList(LongBlockEncoding.class, LongBlockEncoding.class,
            LongBlockEncoding.class, LongBlockEncoding.class, DoubleBlockEncoding.class, DoubleBlockEncoding.class,
            ByteArrayBlockEncoding.class, LongBlockEncoding.class, LongBlockEncoding.class, LongBlockEncoding.class,
            LongBlockEncoding.class, LongBlockEncoding.class, ByteArrayBlockEncoding.class,
            ByteArrayBlockEncoding.class,
            ByteArrayBlockEncoding.class, ByteArrayBlockEncoding.class, ByteArrayBlockEncoding.class,
            LongBlockEncoding.class);
        for (int i = 0; i < typeList.size(); i++) {
            Assert.assertEquals("Not match at pos: " + i, expectClasses2.get(i), blockEncodings2.get(i).getClass());
        }
    }

    @Test
    public void testNullEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        NullBlock nullBlock = new NullBlock(count);
        NullBlockEncoding encoding = new NullBlockEncoding();
        Assert.assertEquals("NULL", encoding.getName());
        encoding.writeBlock(sliceOutput, nullBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertEquals(count, block.getPositionCount());
    }

    @Test
    public void testReferenceEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        ReferenceBlock refBlock = new ObjectBlock(0, count, new boolean[count], new Object[count]);
        ReferenceBlockEncoding encoding = new ReferenceBlockEncoding();
        Assert.assertEquals("ReferenceObject", encoding.getName());
        try {
            encoding.writeBlock(sliceOutput, refBlock);
            Assert.fail("Expect failed");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
        }
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        try {
            Block block = encoding.readBlock(sliceInput);
            Assert.fail("Expect failed");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    @Test
    public void testDecimal64Encoding() {
        final int count = 16;
        final DataType decimalType = new DecimalType(16, 2);
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(count / 2, decimalType);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeLong(i * 200);
        }
        blockBuilder.appendNull();
        DecimalBlock sourceBlock = (DecimalBlock) blockBuilder.build();
        Assert.assertTrue(sourceBlock.isDecimal64());
        DecimalBlockEncoding encoding = new DecimalBlockEncoding();
        Assert.assertEquals("DECIMAL", encoding.getName());
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof DecimalBlock);
        Assert.assertTrue(((DecimalBlock) block).isDecimal64());
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getLong(i), block.getLong(i));
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getDecimal(i), block.getDecimal(i));
        }
    }

    @Test
    public void testDecimal128Encoding() {
        final int count = 16;
        final DataType decimalType = new DecimalType(30, 4);
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(count / 2, decimalType);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeDecimal128(i * 200, i);
        }
        blockBuilder.appendNull();
        DecimalBlock sourceBlock = (DecimalBlock) blockBuilder.build();
        Assert.assertTrue(sourceBlock.isDecimal128());
        DecimalBlockEncoding encoding = new DecimalBlockEncoding();
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof DecimalBlock);
        Assert.assertTrue(((DecimalBlock) block).isDecimal128());
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getDecimal128Low(i),
                ((DecimalBlock) block).getDecimal128Low(i));
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getDecimal128High(i),
                ((DecimalBlock) block).getDecimal128High(i));
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getDecimal(i), block.getDecimal(i));
        }
    }

    @Test
    public void testNormalDecimalEncoding() {
        final int count = 16;
        final DataType decimalType = new DecimalType(16, 2);
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(count / 2, decimalType);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeDecimal(new Decimal(i * 500L, decimalType.getScale()));
        }
        blockBuilder.appendNull();
        DecimalBlock sourceBlock = (DecimalBlock) blockBuilder.build();
        Assert.assertTrue(sourceBlock.getState().isNormal());
        DecimalBlockEncoding encoding = new DecimalBlockEncoding();
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof DecimalBlock);
        Assert.assertTrue(((DecimalBlock) block).getState().isNormal());
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getDecimal(i), block.getDecimal(i));
        }
    }

    @Test
    public void testIllegalDecimalTypeEncoding() {
        final byte illegalType = 3;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        sliceOutput.writeInt(16);
        sliceOutput.writeByte(illegalType);
        DecimalBlockEncoding encoding = new DecimalBlockEncoding();
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        try {
            Block block = encoding.readBlock(sliceInput);
            Assert.fail("Expect failed");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unexpected decimal block encoding type: " + illegalType));
        }
    }

    @Test
    public void testClobEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        ClobBlock clobBlock = new ClobBlock(0, count, new boolean[count], new Object[count]);
        ClobBlockEncoding encoding = new ClobBlockEncoding();
        Assert.assertEquals("CLOB", encoding.getName());
        try {
            encoding.writeBlock(sliceOutput, clobBlock);
            Assert.fail("Expect failed");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
        }
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        try {
            Block block = encoding.readBlock(sliceInput);
            Assert.fail("Expect failed");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    @Test
    public void testBigIntegerEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        BigIntegerBlockBuilder blockBuilder = new BigIntegerBlockBuilder(count / 2);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeBigInteger(BigInteger.valueOf(i * 100));
        }
        blockBuilder.appendNull();
        BigIntegerBlock sourceBlock = (BigIntegerBlock) blockBuilder.build();
        BigIntegerBlockEncoding encoding = new BigIntegerBlockEncoding();
        Assert.assertEquals("BIGINTEGER", encoding.getName());
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof BigIntegerBlock);
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getBigInteger(i), block.getBigInteger(i));
        }
    }

    @Test
    public void testByteArrayEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        ByteArrayBlockBuilder blockBuilder = new ByteArrayBlockBuilder(count / 2, 32);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeByteArray(String.valueOf(i * 999).getBytes());
        }
        blockBuilder.appendNull();
        ByteArrayBlock sourceBlock = (ByteArrayBlock) blockBuilder.build();
        ByteArrayBlockEncoding encoding = new ByteArrayBlockEncoding();
        Assert.assertEquals("BYTE_ARRAY", encoding.getName());
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof ByteArrayBlock);
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertArrayEquals("Failed at pos: " + i, sourceBlock.getByteArray(i), block.getByteArray(i));
        }
    }

    @Test
    public void testFloatEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        FloatBlockBuilder blockBuilder = new FloatBlockBuilder(count / 2);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeFloat(i * 1.1F);
        }
        blockBuilder.appendNull();
        FloatBlock sourceBlock = (FloatBlock) blockBuilder.build();
        FloatBlockEncoding encoding = new FloatBlockEncoding(XResultUtil.DECIMAL_NOT_SPECIFIED);
        Assert.assertEquals("FLOAT", encoding.getName());
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof FloatBlock);
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getFloat(i), block.getFloat(i), 1e-10);
        }
    }

    @Test
    public void testBooleanEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        BooleanBlockBuilder blockBuilder = new BooleanBlockBuilder(count / 2);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeBoolean(i % 2 == 0);
        }
        blockBuilder.appendNull();
        BooleanBlock sourceBlock = (BooleanBlock) blockBuilder.build();
        BooleanBlockEncoding encoding = new BooleanBlockEncoding();
        Assert.assertEquals("BOOLEAN", encoding.getName());
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof BooleanBlock);
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getBoolean(i), block.getBoolean(i));
        }
    }

    @Test
    public void testShortEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        ShortBlockBuilder blockBuilder = new ShortBlockBuilder(count / 2);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeShort((short) i);
        }
        blockBuilder.appendNull();
        ShortBlock sourceBlock = (ShortBlock) blockBuilder.build();
        ShortBlockEncoding encoding = new ShortBlockEncoding();
        Assert.assertEquals("SHORT", encoding.getName());
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof ShortBlock);
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getShort(i), block.getShort(i));
        }
    }

    @Test
    public void testEnumEncoding() {
        final Map<String, Integer> enumMap = new HashMap<>();
        enumMap.put("a", 1);
        enumMap.put("b", 2);
        enumMap.put("c", 3);
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        EnumBlockBuilder blockBuilder = new EnumBlockBuilder(count / 2, 32, enumMap);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeString(String.valueOf((char) ('a' + i % enumMap.size())));
        }
        blockBuilder.appendNull();
        EnumBlock sourceBlock = (EnumBlock) blockBuilder.build();
        EnumBlockEncoding encoding = new EnumBlockEncoding(enumMap);
        Assert.assertEquals("ENUM", encoding.getName());
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof EnumBlock);
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getObject(i), block.getObject(i));
        }
    }

    @Test
    public void testByteEncoding() {
        final int count = 16;
        SliceOutput sliceOutput = new DynamicSliceOutput(128);
        ByteBlockBuilder blockBuilder = new ByteBlockBuilder(count / 2);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeByte((byte) i);
        }
        blockBuilder.appendNull();
        ByteBlock sourceBlock = (ByteBlock) blockBuilder.build();
        ByteBlockEncoding encoding = new ByteBlockEncoding();
        Assert.assertEquals("BYTE", encoding.getName());
        encoding.writeBlock(sliceOutput, sourceBlock);
        SliceInput sliceInput = new BasicSliceInput(sliceOutput.slice());
        Block block = encoding.readBlock(sliceInput);
        Assert.assertTrue(block instanceof ByteBlock);
        Assert.assertEquals(count, block.getPositionCount());
        for (int i = 0; i < count; i++) {
            if (sourceBlock.isNull(i)) {
                Assert.assertTrue("Failed at pos: " + i, block.isNull(i));
                continue;
            }
            Assert.assertEquals("Failed at pos: " + i, sourceBlock.getByte(i), block.getByte(i));
        }
    }
}

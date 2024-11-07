package com.alibaba.polardbx.executor.chunk;

import com.alibaba.druid.mock.MockClob;
import com.alibaba.polardbx.common.datatype.Decimal;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class BlackHoleBlockTest extends BaseBlockTest {
    @Test
    public void testWrite() {
        BlackHoleBlockBuilder blockBuilder = new BlackHoleBlockBuilder();
        blockBuilder.writeBoolean(true);
        blockBuilder.writeByte((byte) 1);
        blockBuilder.writeShort((short) 1);
        blockBuilder.writeInt(1);
        blockBuilder.writeLong(1);
        blockBuilder.writeFloat(1.0f);
        blockBuilder.writeDouble(1.0d);
        blockBuilder.writeString("1");
        blockBuilder.writeBigInteger(BigInteger.ONE);
        blockBuilder.writeDate(new Date(2001, 10, 10));
        blockBuilder.writeDatetimeRawLong(System.currentTimeMillis());
        blockBuilder.writeTime(new Time(10, 1, 1));
        blockBuilder.writeTimestamp(new Timestamp(System.currentTimeMillis()));
        blockBuilder.writeBlob(new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {1}));
        blockBuilder.writeClob(new MockClob());
        blockBuilder.writeObject(new Object());
        blockBuilder.writeDecimal(new Decimal(1000, 2));
        blockBuilder.writeByteArray(new byte[] {1});
        blockBuilder.writeByteArray(new byte[] {1, 2}, 1, 1);
        blockBuilder.appendNull();
        Assert.assertEquals(20, blockBuilder.positionCount);

        blockBuilder = (BlackHoleBlockBuilder) blockBuilder.newBlockBuilder();
        Assert.assertEquals(0, blockBuilder.getPositionCount());
        blockBuilder.ensureCapacity(10);
        blockBuilder = (BlackHoleBlockBuilder) blockBuilder.newBlockBuilder(null, 1024);
    }

    @Test
    public void testFailMethod() {
        BlackHoleBlockBuilder blockBuilder = new BlackHoleBlockBuilder();
        notSupport(() -> blockBuilder.getObject(1));
        notSupport(() -> blockBuilder.isNull(1));
        notSupport(blockBuilder::build);
        notSupport(blockBuilder::mayHaveNull);
        notSupport(blockBuilder::estimateSize);
        notSupport(() -> blockBuilder.hashCodeUseXxhash(1));
        notSupport(() -> blockBuilder.writePositionTo(1, null));
    }

    private void notSupport(Runnable action) {
        try {
            action.run();
        } catch (UnsupportedOperationException e) {
            return;
        }
        Assert.fail(action + "should not support");
    }
}

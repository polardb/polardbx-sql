package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class BlackHoleBlockBuilder implements BlockBuilder {

    int positionCount = 0;

    public BlackHoleBlockBuilder() {
    }

    public void writeByte(byte value) {
        positionCount += 1;
    }

    public void writeShort(short value) {
        positionCount += 1;
    }

    public void writeInt(int value) {
        positionCount += 1;
    }

    public void writeLong(long value) {
        positionCount += 1;
    }

    public void writeDouble(double value) {
        positionCount += 1;
    }

    public void writeFloat(float value) {
        positionCount += 1;
    }

    public void writeTimestamp(Timestamp value) {
        positionCount += 1;
    }

    public void writeDate(Date value) {
        positionCount += 1;
    }

    public void writeTime(Time value) {
        positionCount += 1;
    }

    public void writeDatetimeRawLong(long val) {
        positionCount += 1;
    }

    public void writeString(String value) {
        positionCount += 1;
    }

    public void writeDecimal(Decimal value) {
        positionCount += 1;
    }

    public void writeBigInteger(BigInteger value) {
        positionCount += 1;
    }

    public void writeBoolean(boolean value) {
        positionCount += 1;
    }

    public void writeByteArray(byte[] value) {
        positionCount += 1;
    }

    public void writeByteArray(byte[] value, int offset, int length) {
        positionCount += 1;
    }

    public void writeBlob(Blob value) {
        positionCount += 1;
    }

    public void writeClob(Clob value) {
        positionCount += 1;
    }

    public void ensureCapacity(int capacity) {
    }

    /**
     * Write an object
     * <p>
     * Just to keep compatible with legacy code. Do NOT use this method as far as possible.
     */
    public void writeObject(Object value) {
        positionCount += 1;
    }

    /**
     * Appends a null value to the block.
     */
    public void appendNull() {
        positionCount += 1;
    }

    /**
     * Creates a new block builder of the same type with current block size as initial capacity
     */
    public BlockBuilder newBlockBuilder() {
        return new BlackHoleBlockBuilder();
    }

    @Override
    public Object getObject(int position) {
        throw new UnsupportedOperationException("black hole block builder can not build block");
    }

    @Override
    public boolean isNull(int position) {
        throw new UnsupportedOperationException("black hole block builder not support check is null");
    }

    @Override
    public Block build() {
        throw new UnsupportedOperationException("black hole block builder can not build block");
    }

    @Override
    public final long hashCodeUseXxhash(int pos) {
        throw new UnsupportedOperationException(
            "Block builder not support hash code calculated by xxhash, you should convert it to block first");
    }

    @Override
    public boolean mayHaveNull() {
        throw new UnsupportedOperationException("black hole block builder not support check is null");
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public final long estimateSize() {
        throw new UnsupportedOperationException();
    }

    public BlockBuilder newBlockBuilder(ObjectPools objectPools, int chunkLimit) {
        return newBlockBuilder();
    }

    @Override
    public final void writePositionTo(int position, BlockBuilder blockBuilder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMemoryUsage() {
        return getElementUsedBytes();
    }
}

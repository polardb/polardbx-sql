package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

public class CSVRow implements Row {
    /**
     * The continuous memory structure for the whole row.
     */
    private final Slice data;

    /**
     * The offsets of each column in the row.
     */
    private final int[] offsets;

    /**
     * The null value of each column in the row.
     */
    private final byte[] nulls;

    /**
     * The count of fields.
     */
    private final int fieldNum;

    public CSVRow(Slice data, int[] offsets, byte[] nulls, int fieldNum) {
        this.data = data;
        this.offsets = offsets;
        this.nulls = nulls;
        this.fieldNum = fieldNum;
    }

    public int getFieldNum() {
        return fieldNum;
    }

    private int beginOffset(int position) {
        return position > 0 ? offsets[position - 1] : 0;
    }

    private int endOffset(int position) {
        return offsets[position];
    }

    @Override
    public byte[] getBytes(int pos) {
        int beginOffset = beginOffset(pos);
        int endOffset = endOffset(pos);

        return data.getBytes(beginOffset, endOffset - beginOffset);
    }

    @Override
    public Long getLong(int pos) {
        int beginOffset = beginOffset(pos);
        int endOffset = endOffset(pos);

        // Check data length equal to Long.BYTES
        Preconditions.checkArgument(beginOffset + Long.BYTES == endOffset);

        return data.getLong(beginOffset);
    }

    @Override
    public Integer getInteger(int pos) {
        int beginOffset = beginOffset(pos);
        int endOffset = endOffset(pos);

        // Check data length equal to Integer.BYTES
        Preconditions.checkArgument(beginOffset + Integer.BYTES == endOffset);

        return data.getInt(beginOffset);
    }

    public int sizeInBytes() {
        return data.length();
    }

    public boolean isNullAt(int pos) {
        return nulls[pos] == 1;
    }

    @Override
    public Object getObject(int index) {
        return null;
    }

    @Override
    public void setObject(int index, Object value) {

    }

    @Override
    public List<Object> getValues() {
        return null;
    }

    @Override
    public List<byte[]> getBytes() {
        return null;
    }

    @Override
    public String getString(int index) {
        return null;
    }

    @Override
    public Boolean getBoolean(int index) {
        return null;
    }

    @Override
    public Short getShort(int index) {
        return null;
    }

    @Override
    public Float getFloat(int index) {
        return null;
    }

    @Override
    public Double getDouble(int index) {
        return null;
    }

    @Override
    public Byte getByte(int index) {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return null;
    }

    @Override
    public Time getTime(int index) {
        return null;
    }

    @Override
    public Date getDate(int index) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int index) {
        return null;
    }

    @Override
    public Blob getBlob(int index) {
        return null;
    }

    @Override
    public Clob getClob(int index) {
        return null;
    }

    @Override
    public byte[] getBytes(int index, String encoding) {
        return new byte[0];
    }

    @Override
    public byte[] getBytes(DataType dataType, int index, String encoding) {
        return new byte[0];
    }

    @Override
    public CursorMeta getParentCursorMeta() {
        return null;
    }

    @Override
    public void setCursorMeta(CursorMeta cursorMeta) {

    }

    @Override
    public int getColNum() {
        return 0;
    }

    @Override
    public long estimateSize() {
        return 0;
    }

    public byte[] serialize() throws IOException {
        int size = Integer.BYTES + Byte.BYTES * fieldNum + Integer.BYTES * fieldNum + data.length();
        Slice sliceOutput = Slices.allocate(size);

        int pos = 0;
        //write filedNum
        sliceOutput.setLong(0, fieldNum);
        pos += Integer.BYTES;

        //write nulls
        sliceOutput.setBytes(pos, nulls);
        pos += Byte.BYTES * fieldNum;

        //write offsets
        for (int offset : offsets) {
            sliceOutput.setInt(pos, offset);
            pos += Integer.BYTES;
        }

        //write data
        sliceOutput.setBytes(pos, data.getBytes());
        return sliceOutput.getBytes();
    }

    public static CSVRow deserialize(byte[] values) throws IOException {
        Slice data = Slices.wrappedBuffer(values);
        int pos = 0;

        int filedNum = data.getInt(pos);
        pos += Integer.BYTES;

        byte[] nulls = data.getBytes(pos, filedNum);
        pos += (Byte.BYTES * filedNum);

        int[] offsets = new int[filedNum];
        for (int i = 0; i < filedNum; i++) {
            offsets[i] = data.getInt(pos);
            pos += Integer.BYTES;
        }

        Slice realData = Slices.wrappedBuffer(data.getBytes(pos, data.length() - pos));

        return new CSVRow(realData, offsets, nulls, filedNum);
    }
}

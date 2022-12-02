/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.row;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import io.airlift.slice.Slice;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author mengshi.sunmengshi 2013-12-3 上午11:06:04
 * @since 5.0.0
 */
public abstract class AbstractRow implements Row {

    private CursorMeta cursorMeta;
    private List<DataType> dataTypes;
    protected int colNum;

    public AbstractRow(CursorMeta cursorMeta) {
        super();
        this.cursorMeta = cursorMeta;
    }

    private void initDataTypes() {
        this.dataTypes = Optional.ofNullable(cursorMeta)
            .map(CursorMeta::getColumns)
            .orElseGet(() -> ImmutableList.of())
            .stream()
            .map(ColumnMeta::getDataType)
            .collect(Collectors.toList());
    }

    private DataType typeOf(int index) {
        if (dataTypes == null) {
            initDataTypes();
        }
        if (index >= dataTypes.size()) {
            return null;
        }
        return dataTypes.get(index);
    }

    @Override
    public CursorMeta getParentCursorMeta() {
        return cursorMeta;
    }

    @Override
    public Integer getInteger(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.IntegerType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public Long getLong(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.LongType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public String getString(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.StringType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public Boolean getBoolean(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        Integer obj = DataTypes.BooleanType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
        if (FunctionUtils.isNull(obj)) {
            return null;
        }
        return BooleanType.isTrue(obj);
    }

    @Override
    public Short getShort(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.ShortType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public Float getFloat(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.FloatType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public Double getDouble(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.DoubleType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public byte[] getBytes(int index) {
        return getBytes(index, null);
    }

    @Override
    public Date getDate(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.DateType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public Timestamp getTimestamp(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);

        Object o = cm.getDataType().getResultGetter().get(this, index);
        if (o instanceof Timestamp) {
            return (Timestamp) o;
        } else {
            // Only if meta data has error.
            // This operation will lose the precision.
            return DataTypes.TimestampType.convertFrom(o);
        }
    }

    @Override
    public Time getTime(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);

        Object o = cm.getDataType().getResultGetter().get(this, index);
        if (o instanceof Time) {
            return (Time) o;
        } else {
            // Only if meta data has error.
            // This operation will lose the precision.
            return DataTypes.TimeType.convertFrom(o);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return (BigDecimal) DataTypes.DecimalType.convertJavaFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public List<byte[]> getBytes() {
        List<byte[]> res = new ArrayList<byte[]>();
        for (int i = 0; i < colNum; i++) {
            res.add(this.getBytes(i));
        }
        return res;
    }

    @Override
    public Byte getByte(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.ByteType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public Blob getBlob(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.BlobType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public Clob getClob(int index) {
        ColumnMeta cm = cursorMeta.getColumnMeta(index);
        return DataTypes.ClobType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public List<Object> getValues() {
        ArrayList<Object> values = new ArrayList<>(getColNum());
        for (int i = 0; i < getColNum(); i++) {
            values.add(getObject(i));
        }
        return values;
    }

    @Override
    public byte[] getBytes(DataType fromType, int index, String encoding) {
        Object o = this.getObject(index);

        if (FunctionUtils.isNull(o)) {
            return null;
        }

        if (o instanceof byte[]) {
            return (byte[]) o;
        }

        // handle slice type
        if (o instanceof Slice && CharsetName.isUTF8(encoding)) {
            Slice slice = (Slice) o;
            Object base = slice.getBase();
            if (base instanceof byte[]) {
                return (byte[]) base;
            } else {
                return slice.getBytes(); // need to copy
            }
        }

        // handle decimal
        if (o instanceof Decimal) {
            return ((Decimal) o).toBytes();
        }

        // handle bytes
        if (o instanceof UInt64) {
            return o.toString().getBytes();
        }

        // just for big bit
        if (o instanceof BigInteger
            && fromType != null
            && fromType.fieldType() == MySQLStandardFieldType.MYSQL_TYPE_BIT) {
            final ColumnMeta columnMeta = this.cursorMeta.getColumns().get(index);
            int precision = 64;
            if (columnMeta != null) {
                if (columnMeta.getField() != null) {
                    precision = cursorMeta.getColumnMeta(index).getField().getRelType().getPrecision();
                    if (precision < 1 || precision > 64) {
                        precision = 64;
                    }
                }
            }
            long l = ((BigInteger) o).longValue();
            final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).putLong(l);
            final int bytesLen = precision / 8 + (precision % 8 != 0 ? 1 : 0);
            final byte[] bytes = new byte[bytesLen];
            buf.flip();
            buf.position(Long.BYTES - bytesLen);
            buf.get(bytes);
            return bytes;
        }

        if (o instanceof Boolean) {
            o = this.getInteger(index);
            final ColumnMeta columnMeta = this.cursorMeta.getColumns().get(index);
            int ro = (Integer) o;
            if (columnMeta != null) {
                if (columnMeta.getField() != null) {
                    if (DataTypeUtil.equalsSemantically(columnMeta.getField().getDataType(), DataTypes.BitType)) {
                        return new byte[] {(byte) ro};
                    }
                }
            }
        }

        if (o instanceof Integer) {
            o = this.getInteger(index);
            final ColumnMeta columnMeta = this.cursorMeta.getColumns().get(index);
            if (o != null) {
                if (columnMeta != null) {
                    if (columnMeta.getField() != null) {
                        if (DataTypeUtil.equalsSemantically(columnMeta.getField().getDataType(), DataTypes.BitType)) {
                            int ro = (Integer) o;
                            if (ro == 0 || ro == 1) {
                                return new byte[] {(byte) ro};
                            }
                        }
                    }
                }
            }
        }

        if (o instanceof Blob) {
            try {
                if (((Blob) o).length() == 0) {
                    return new byte[0];
                }
                return ((Blob) o).getBytes(1, (int) ((Blob) o).length());
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        // fix scale.
        String str = DataTypeUtil.convert(fromType, DataTypes.StringType, o);
        try {
            if (encoding == null) {
                return str.getBytes();
            } else {
                return str.getBytes(TStringUtil.javaEncoding(encoding));
            }
        } catch (UnsupportedEncodingException e) {
            return str.getBytes();
        }
    }

    @Override
    public byte[] getBytes(int index, String encoding) {
        DataType fromType = typeOf(index);
        return getBytes(fromType, index, encoding);
    }

    @Override
    public void setCursorMeta(CursorMeta cursorMeta) {
        this.cursorMeta = cursorMeta;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<Object> values = this.getValues();
        for (int i = 0; i < colNum; i++) {
            sb.append(i + ":" + values.get(i) + " ");
        }
        return sb.toString();
    }

    @Override
    public int getColNum() {
        return colNum;
    }

    @Override
    public long estimateSize() {
        return 0L;
    }
}

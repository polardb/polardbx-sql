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

package com.alibaba.polardbx.optimizer.biv;

import com.alibaba.druid.mock.MockBlob;
import com.alibaba.druid.mock.MockClob;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.UndecidedStringType;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import io.airlift.slice.Slice;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.biv.MockUtils.SCHEDUAL_STACK;

public class MockResultSet implements ResultSet {
    private int rowNum = 0;
    private CursorMeta cursorMeta;
    private MockCacheData mockCacheData;

    public MockResultSet(MockStatement mockStatement, CursorMeta cursorMeta, boolean hasData) {
        this.cursorMeta = cursorMeta;
        if (!hasData || MockUtils.checkStack(SCHEDUAL_STACK)) {
            rowNum = 0;
        } else {
            rowNum = 1;
        }
    }

    public MockResultSet(MockPreparedStatement mockPreparedStatement, CursorMeta cursorMeta, boolean hasData) {
        this.cursorMeta = cursorMeta;
        if (!hasData || MockUtils.checkStack(SCHEDUAL_STACK)) {
            rowNum = 0;
        } else {
            rowNum = 1;
        }
    }

    public MockResultSet(MockCacheData mockCacheData) {
        this.mockCacheData = mockCacheData;
        rowNum = mockCacheData.getDatas().size();
    }

    @Override
    public boolean next() throws SQLException {
        return --rowNum >= 0;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1));
        }
        return "1";
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Integer.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1))) != 0;
        }
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Byte.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Short.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Integer.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Long.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Float.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Double.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return new BigDecimal(0);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)).getBytes();
        }
        return new byte[] {'0'};
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Date.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return new Date(System.currentTimeMillis());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Time.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return new Time(System.currentTimeMillis());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return Timestamp.valueOf(String.valueOf(mockCacheData.getDatas().get(rowNum).get(columnIndex - 1)));
        }
        return new Timestamp(System.currentTimeMillis());
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getString(index);
        }
        return "1";
    }

    private int findIndex(String columnLabel, ResultSetMetaData resultSetMetaData) throws SQLException {
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            if (columnLabel.equals(resultSetMetaData.getColumnName(i))) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getBoolean(index);
        }
        return false;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getByte(index);
        }
        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getShort(index);
        }
        return 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getInt(index);
        }
        return 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getLong(index);
        }
        return 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getFloat(index);
        }
        return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getDouble(index);
        }
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return new BigDecimal(0);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getBytes(index);
        }
        if (cursorMeta != null) {
            int sqlType = Types.VARCHAR;
            for (ColumnMeta columnMeta : cursorMeta.getColumns()) {
                if (columnLabel.equals(columnMeta.getName())) {
                    sqlType = columnMeta.getDataType().getSqlType();
                }
            }
            if (MockUtils.isSqlTypeNum(sqlType)) {
                return new byte[] {0};
            } else if (MockUtils.isSqlTypeChar(sqlType)) {
                return new byte[] {'a'};
            }
        }

        return new byte[] {'0'};
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getDate(index);
        }
        return new Date(System.currentTimeMillis());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getTime(index);
        }
        return new Time(System.currentTimeMillis());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        if (mockCacheData != null) {
            int index = findIndex(columnLabel, mockCacheData.getResultSetMetaData());
            return getTimestamp(index);
        }
        return new Timestamp(System.currentTimeMillis());
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (mockCacheData != null) {
            return mockCacheData.getResultSetMetaData();
        }
        return new MockResultSetMetaData(cursorMeta.getColumns(), Maps.newHashMap());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (mockCacheData != null) {
            return mockCacheData.getDatas().get(rowNum).get(columnIndex - 1);
        }

        if (cursorMeta == null && columnIndex == 1) {
            return 1;
        }
        return mockData(cursorMeta.getColumnMeta(columnIndex - 1).getDataType());
    }

    private Object mockData(DataType type) {
        if (type instanceof UndecidedStringType) {
            return null;
        }

        final Class clazz = type.getDataClass();
        if (clazz == Integer.class) {
            return 1;
        } else if (clazz == Long.class) {
            return 1;
        } else if (clazz == Short.class) {
            return 1;
        } else if (clazz == Byte.class) {
            return '1';
        } else if (clazz == Float.class) {
            return 1.0f;
        } else if (clazz == Double.class) {
            return 1.0d;
        } else if (clazz == String.class) {
            return "1";
        } else if (clazz == BigInteger.class) {
            return 1;
        } else if (clazz == Decimal.class) {
            return 1;
        } else if (clazz == Timestamp.class) {
            return new Timestamp(System.currentTimeMillis());
        } else if (clazz == Date.class) {
            return new Date(System.currentTimeMillis());
        } else if (clazz == Time.class) {
            return new Time(System.currentTimeMillis());
        } else if (clazz == byte[].class) {
            return new byte[] {'1'};
        } else if (clazz == Blob.class) {
            return new MockBlob();
        } else if (clazz == Clob.class) {
            return new MockClob();
        } else if (clazz == Enum.class) {
            return null;
        } else if (clazz == Slice.class) {
            return type.convertFrom("1");
        } else {
            throw new AssertionError("Data type " + clazz.getName() + " not supported");
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int index = findColumn(columnLabel);
        return getObject(index);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < cursorMeta.getColumns().size(); i++) {
            if (cursorMeta.getColumns().get(i).getName().equals(columnLabel)) {
                return i;
            }
        }
        return 0;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return new BigDecimal(0);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return new BigDecimal(0);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {

    }

    @Override
    public void afterLast() throws SQLException {

    }

    @Override
    public boolean first() throws SQLException {
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return (Blob) getObject(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return (Clob) getObject(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return (Blob) getObject(columnLabel);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return (Clob) getObject(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return (T) getObject(columnIndex);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return (T) getObject(columnLabel);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}

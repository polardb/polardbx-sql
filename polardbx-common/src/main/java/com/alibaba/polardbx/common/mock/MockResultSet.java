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

package com.alibaba.polardbx.common.mock;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.mock.MockDataSource.ExecuteInfo;
import com.alibaba.polardbx.common.mock.MockDataSource.QueryResult;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockResultSet implements ResultSet {

    private final MockDataSource mds;
    private ResultSetMetaData resultSetMetaData;
    public final Map<String, Integer> columns;
    public final List<Object[]> rows;
    private int cursor = -1;
    private boolean closed;
    private int closeInvocatingTimes = 0;
    private int nextInvokingTimes = 0;
    private long nextSleepTime = 0;

    public MockResultSet(MockDataSource mockDataSource, Map<String, Integer> columns, List<Object[]> values) {
        this.mds = mockDataSource;
        this.columns = columns;
        this.rows = values;
        resultSetMetaData = new MockResultSetMetaData(columns);
    }

    public MockResultSet(MockDataSource mockDataSource, QueryResult res) {
        this.mds = mockDataSource;
        if (res != null) {
            this.columns = res.columns;
            this.rows = res.rows;
        } else {
            this.columns = new HashMap<String, Integer>(0);
            this.rows = new ArrayList<Object[]>(0);
        }
        resultSetMetaData = new MockResultSetMetaData(columns);
    }

    public int getCloseInvocatingTimes() {
        return closeInvocatingTimes;
    }

    public int getNextInvokingTimes() {
        return nextInvokingTimes;
    }

    public boolean absolute(int row) throws SQLException {
        throw new NotSupportException("absolute");
    }

    public void afterLast() throws SQLException {
        throw new NotSupportException("afterLast");
    }

    public void beforeFirst() throws SQLException {
        throw new NotSupportException("beforeFirst");
    }

    public void cancelRowUpdates() throws SQLException {
        throw new NotSupportException("cancelRowUpdates");
    }

    public void clearWarnings() throws SQLException {
        throw new NotSupportException("clearWarnings");
    }

    public void close() throws SQLException {
        closed = true;
        closeInvocatingTimes++;
    }

    protected void checkClose() throws SQLException {
        if (closed) {
            throw new SQLException("closed");
        }
    }

    public void deleteRow() throws SQLException {
        throw new NotSupportException("deleteRow");
    }

    public void closeInternal(boolean removeThis) throws SQLException {
        throw new NotSupportException("closeInternal");
    }

    public int findColumn(String columnName) throws SQLException {
        throw new NotSupportException("findColumn");
    }

    public boolean first() throws SQLException {
        throw new NotSupportException("first");
    }

    public Array getArray(int i) throws SQLException {
        return (Array) getObject(i);
    }

    public Array getArray(String colName) throws SQLException {
        throw new NotSupportException("getArray(String colName)");
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new NotSupportException("getAsciiStream(int columnIndex)");
    }

    public InputStream getAsciiStream(String columnName) throws SQLException {
        throw new NotSupportException("getAsciiStream(String columnName)");
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return (BigDecimal) getObject(columnIndex);
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        throw new NotSupportException("getBigDecimal(String columnName)");
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new NotSupportException("getBigDecimal(int columnIndex, int scale)");
    }

    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        throw new NotSupportException("getBigDecimal(String columnName, int scale)");
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return (InputStream) getObject(columnIndex);
    }

    public InputStream getBinaryStream(String columnName) throws SQLException {
        throw new NotSupportException("getBinaryStream(String columnName)");
    }

    public Blob getBlob(int i) throws SQLException {
        return (Blob) getObject(i);
    }

    public Blob getBlob(String colName) throws SQLException {
        throw new NotSupportException("getBlob(String colName)");
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return (Boolean) getObject(columnIndex);
    }

    public boolean getBoolean(String columnName) throws SQLException {
        throw new NotSupportException("getBoolean(String columnName)");
    }

    public byte getByte(int columnIndex) throws SQLException {
        return (Byte) getObject(columnIndex);
    }

    public byte getByte(String columnName) throws SQLException {
        throw new NotSupportException("getByte(String columnName)");
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return (byte[]) getObject(columnIndex);
    }

    public byte[] getBytes(String columnName) throws SQLException {
        throw new NotSupportException("getBytes(String columnName)");
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return (Reader) getObject(columnIndex);
    }

    public Reader getCharacterStream(String columnName) throws SQLException {
        throw new NotSupportException("getCharacterStream(String columnName)");
    }

    public Clob getClob(int i) throws SQLException {
        return (Clob) getObject(i);
    }

    public Clob getClob(String colName) throws SQLException {
        throw new NotSupportException("getClob(String colName)");
    }

    public int getConcurrency() throws SQLException {
        throw new NotSupportException("getConcurrency");
    }

    public String getCursorName() throws SQLException {
        throw new NotSupportException("getCursorName");
    }

    public Date getDate(int columnIndex) throws SQLException {
        return (Date) getObject(columnIndex);
    }

    public Date getDate(String columnName) throws SQLException {
        throw new NotSupportException("getDate(String columnName)");
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new NotSupportException("getDate(int columnIndex, Calendar cal)");
    }

    public Date getDate(String columnName, Calendar cal) throws SQLException {
        throw new NotSupportException("getDate(String columnName, Calendar cal)");
    }

    public double getDouble(int columnIndex) throws SQLException {
        throw new NotSupportException("getDouble(int columnIndex)");
    }

    public double getDouble(String columnName) throws SQLException {
        throw new NotSupportException("getDouble(String columnName)");
    }

    public int getFetchDirection() throws SQLException {
        throw new NotSupportException("getFetchDirection");
    }

    public int getFetchSize() throws SQLException {
        throw new NotSupportException("getFetchSize");
    }

    public float getFloat(int columnIndex) throws SQLException {
        throw new NotSupportException("getFloat(int columnIndex)");
    }

    public float getFloat(String columnName) throws SQLException {
        throw new NotSupportException("getFloat(String columnName)");
    }

    public int getInt(int columnIndex) throws SQLException {
        Long value = (Long) this.rows.get(this.cursor)[columnIndex - 1];
        return value.intValue();
    }

    public int getInt(String columnName) throws SQLException {
        throw new NotSupportException("getInt(String columnName)");
    }

    public long getLong(int columnIndex) throws SQLException {
        Long value = (Long) this.rows.get(this.cursor)[columnIndex - 1];
        return value.longValue();
    }

    public long getLong(String columnName) throws SQLException {
        throw new NotSupportException("getLong(String columnName)");
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        MockDataSource.record(new ExecuteInfo(this.mds, "ResultSet.getMetaData", null, null));
        return this.resultSetMetaData;
    }

    public Object getObject(int columnIndex) throws SQLException {
        return this.rows.get(cursor)[columnIndex - 1];
    }

    public Object getObject(String columnName) throws SQLException {
        return this.rows.get(cursor)[this.columns.get(columnName) - 1];
    }

    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        throw new NotSupportException("getObject(int i, Map<String, Class<?>> map)");
    }

    public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
        throw new NotSupportException("getObject(String colName, Map<String, Class<?>> map)");
    }

    public Ref getRef(int i) throws SQLException {
        throw new NotSupportException("getRef(int i)");
    }

    public Ref getRef(String colName) throws SQLException {
        throw new NotSupportException("getRef(String colName)");
    }

    public int getRow() throws SQLException {
        throw new NotSupportException("getRow");
    }

    public short getShort(int columnIndex) throws SQLException {
        return (Short) this.getObject(columnIndex);
    }

    public short getShort(String columnName) throws SQLException {
        return (Short) this.getObject(columnName);
    }

    public Statement getStatement() throws SQLException {
        throw new NotSupportException("getStatement");
    }

    public String getString(int columnIndex) throws SQLException {
        return (String) this.getObject(columnIndex);
    }

    public String getString(String columnName) throws SQLException {
        return (String) this.getObject(columnName);
    }

    public Time getTime(int columnIndex) throws SQLException {
        return (Time) this.getObject(columnIndex);
    }

    public Time getTime(String columnName) throws SQLException {
        return (Time) this.getObject(columnName);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new NotSupportException("getTime(int columnIndex, Calendar cal)");
    }

    public Time getTime(String columnName, Calendar cal) throws SQLException {
        throw new NotSupportException("getTime(String columnName, Calendar cal)");
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) this.getObject(columnIndex);
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        return (Timestamp) this.getObject(columnName);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new NotSupportException("getTimestamp(int columnIndex, Calendar cal)");
    }

    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        throw new NotSupportException("getTimestamp(String columnName, Calendar cal)");
    }

    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new NotSupportException("getURL(int columnIndex)");
    }

    public URL getURL(String columnName) throws SQLException {
        throw new NotSupportException("getURL(String columnName)");
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new NotSupportException("getUnicodeStream(int columnIndex)");
    }

    public InputStream getUnicodeStream(String columnName) throws SQLException {
        throw new NotSupportException("getUnicodeStream(String columnName)");
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new NotSupportException("getWarnings");
    }

    public void insertRow() throws SQLException {
        throw new NotSupportException("insertRow");
    }

    public boolean isAfterLast() throws SQLException {
        throw new NotSupportException("isAfterLast");
    }

    public boolean isBeforeFirst() throws SQLException {
        throw new NotSupportException("isBeforeFirst");
    }

    public boolean isFirst() throws SQLException {
        throw new NotSupportException("isFirst");
    }

    public boolean isLast() throws SQLException {
        throw new NotSupportException("isLast");
    }

    public boolean last() throws SQLException {
        throw new NotSupportException("last");
    }

    public void moveToCurrentRow() throws SQLException {
        throw new NotSupportException("moveToCurrentRow");
    }

    public void moveToInsertRow() throws SQLException {
        throw new NotSupportException("moveToInsertRow");
    }

    protected boolean hasNext() {
        cursor++;
        return cursor < this.rows.size();
    }

    public boolean next() throws SQLException {
        try {
            Thread.sleep(nextSleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        nextInvokingTimes++;
        return hasNext();
    }

    public boolean previous() throws SQLException {
        throw new NotSupportException("previous");
    }

    public void refreshRow() throws SQLException {
        throw new NotSupportException("refreshRow");
    }

    public boolean relative(int rows) throws SQLException {
        throw new NotSupportException("relative");
    }

    public boolean rowDeleted() throws SQLException {
        throw new NotSupportException("rowDeleted");
    }

    public boolean rowInserted() throws SQLException {
        throw new NotSupportException("rowInserted");
    }

    public boolean rowUpdated() throws SQLException {
        throw new NotSupportException("rowUpdated");
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new NotSupportException("setFetchDirection");
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new NotSupportException("setFetchSize");
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new NotSupportException("updateArray(int columnIndex, Array x)");
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        throw new NotSupportException("updateArray(String columnName, Array x)");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new NotSupportException("updateAsciiStream(int columnIndex, InputStream x, int length)");
    }

    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        throw new NotSupportException("updateAsciiStream(String columnName, InputStream x, int length)");
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new NotSupportException("updateBigDecimal(int columnIndex, BigDecimal x)");
    }

    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        throw new NotSupportException("updateBigDecimal(String columnName, BigDecimal x)");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new NotSupportException("updateBinaryStream(int columnIndex, InputStream x, int length)");
    }

    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        throw new NotSupportException("updateBinaryStream(String columnName, InputStream x, int length)");
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new NotSupportException("updateBlob(int columnIndex, Blob x)");
    }

    public void updateBlob(String columnName, Blob x) throws SQLException {
        throw new NotSupportException("updateBlob(String columnName, Blob x)");
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new NotSupportException("updateBoolean(int columnIndex, boolean x)");
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        throw new NotSupportException("updateBoolean(String columnName, boolean x)");
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new NotSupportException("updateByte(int columnIndex, byte x)");
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        throw new NotSupportException("updateByte(String columnName, byte x)");
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new NotSupportException("updateBytes(int columnIndex, byte[] x)");
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw new NotSupportException("updateBytes(String columnName, byte[] x)");
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new NotSupportException("updateCharacterStream(int columnIndex, Reader x, int length)");
    }

    public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
        throw new NotSupportException("updateCharacterStream(String columnName, Reader reader, int length)");
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new NotSupportException("updateClob(int columnIndex, Clob x)");
    }

    public void updateClob(String columnName, Clob x) throws SQLException {
        throw new NotSupportException("updateClob(String columnName, Clob x)");
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new NotSupportException("updateDate(int columnIndex, Date x)");
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        throw new NotSupportException("updateDate(String columnName, Date x)");
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new NotSupportException("updateDouble(int columnIndex, double x)");
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        throw new NotSupportException("updateDouble(String columnName, double x)");
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new NotSupportException("updateFloat(int columnIndex, float x)");
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        throw new NotSupportException("updateFloat(String columnName, float x)");
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new NotSupportException("updateInt(int columnIndex, int x)");
    }

    public void updateInt(String columnName, int x) throws SQLException {
        throw new NotSupportException("updateInt(String columnName, int x)");
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new NotSupportException("updateLong(int columnIndex, long x)");
    }

    public void updateLong(String columnName, long x) throws SQLException {
        throw new NotSupportException("updateLong(String columnName, long x)");
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new NotSupportException("updateNull(int columnIndex)");
    }

    public void updateNull(String columnName) throws SQLException {
        throw new NotSupportException("updateNull(String columnName)");
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new NotSupportException("updateObject(int columnIndex, Object x)");
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        throw new NotSupportException("updateObject(String columnName, Object x)");
    }

    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        throw new NotSupportException("updateObject(int columnIndex, Object x, int scale)");
    }

    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        throw new NotSupportException("updateObject(String columnName, Object x, int scale)");
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new NotSupportException("updateRef(int columnIndex, Ref x)");
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        throw new NotSupportException("updateRef(String columnName, Ref x)");
    }

    public void updateRow() throws SQLException {
        throw new NotSupportException("updateRow");
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new NotSupportException("updateShort(int columnIndex, short x)");
    }

    public void updateShort(String columnName, short x) throws SQLException {
        throw new NotSupportException("updateShort(String columnName, short x)");
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new NotSupportException("updateString(int columnIndex, String x)");
    }

    public void updateString(String columnName, String x) throws SQLException {
        throw new NotSupportException("updateString(String columnName, String x)");
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new NotSupportException("updateTime(int columnIndex, Time x)");
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        throw new NotSupportException("updateTime(String columnName, Time x)");
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new NotSupportException("updateTimestamp(int columnIndex, Timestamp x)");
    }

    public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
        throw new NotSupportException("updateTimestamp(String columnName, Timestamp x)");
    }

    public boolean wasNull() throws SQLException {
        Object[] objects = this.rows.get(this.cursor);
        return null == objects[objects.length - 1];
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {

        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {

        return false;
    }

    public RowId getRowId(int columnIndex) throws SQLException {

        return null;
    }

    public RowId getRowId(String columnLabel) throws SQLException {

        return null;
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    public int getHoldability() throws SQLException {

        return 0;
    }

    public boolean isClosed() throws SQLException {

        return false;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    public NClob getNClob(int columnIndex) throws SQLException {

        return null;
    }

    public NClob getNClob(String columnLabel) throws SQLException {

        return null;
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {

        return null;
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {

        return null;
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    public String getNString(int columnIndex) throws SQLException {

        return null;
    }

    public String getNString(String columnLabel) throws SQLException {

        return null;
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {

        return null;
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {

        return null;
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

}

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

package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.jdbc.BufferResultSet;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.MultiResultCursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.repo.mysql.cursor.ResultSetCursor;

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
import java.util.List;
import java.util.Map;

/**
 * 多语句的返回对象
 *
 * @author agapple 2014年10月31日 下午11:03:55
 * @since 5.1.14
 */
public class TMultiResultSet implements BufferResultSet {

    private ResultSet currentResutSet;
    private int updateCount = -1;
    private List<ResultSet> resultSets = new ArrayList<ResultSet>();
    private int currentIndex = -1;
    private boolean isClosed;
    private MultiResultCursor cursor;

    public TMultiResultSet(MultiResultCursor cursor, Map<String, Object> extraCmd) {
        try {
            this.cursor = cursor;

            // 针对tddl实现的多语句
            for (ResultCursor resultCursor : cursor.getResultCursors()) {
                if (resultCursor instanceof ResultSetCursor) {
                    resultSets.add(((ResultSetCursor) resultCursor).getResultSet());
                } else if (resultCursor instanceof MultiResultCursor) {
                    resultSets.add(new TMultiResultSet((MultiResultCursor) resultCursor, extraCmd));

                } else {
                    resultSets.add(new TResultSet(resultCursor, extraCmd));
                }
            }

            currentResutSet = resultSets.get(++currentIndex);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int getAffectRows() throws SQLException {
        if (currentResutSet != null) {
            // 如果是tddl内部result，affect是混合在一起的
            if (currentResutSet instanceof TResultSet) {
                return ((TResultSet) currentResutSet).getAffectRows();
            }

            if (currentResutSet instanceof TMultiResultSet) {
                return ((TMultiResultSet) currentResutSet).getAffectRows();
            }
        }

        return updateCount;
    }

    public boolean getMoreResults() throws SQLException {
        return getMoreResults(1);
    }

    public boolean getMoreResults(int current) throws SQLException {
        if (current != java.sql.Statement.CLOSE_CURRENT_RESULT) {
            throw new NotSupportException();
        }

        if (currentResutSet instanceof TMultiResultSet) {

            if (((TMultiResultSet) currentResutSet).getMoreResults(current)) {
                return true;
            } else {
                updateCount = ((TMultiResultSet) currentResutSet).getAffectRows();
                if (updateCount != -1) {
                    return false;
                }
            }

        }
        currentIndex++;
        currentResutSet.close();// 关闭上一个
        if (currentIndex < resultSets.size()) {
            currentResutSet = resultSets.get(currentIndex);

            if (currentResutSet instanceof TResultSet) {
                updateCount = ((TResultSet) currentResutSet).getAffectRows();
            } else if (currentResutSet instanceof TMultiResultSet) {
                updateCount = ((TMultiResultSet) currentResutSet).getAffectRows();
            }
            return (updateCount == -1);
        } else {
            updateCount = -1;
            currentResutSet = null;
            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        if (isClosed) {
            return;
        }

        try {
            List<Throwable> exs = new ArrayList<>();
            try {
                if (this.currentResutSet != null && !this.currentResutSet.isClosed()) {
                    this.currentResutSet.close();
                }
            } catch (Throwable e) {
                exs.add(new TddlException(e));
            }
            // 关闭其他的resultSet
            for (int i = currentIndex + 1; i < resultSets.size(); i++) {
                try {
                    resultSets.get(i).close();
                } catch (Throwable e) {
                    exs.add(new TddlException(e));
                }
            }

            if (!exs.isEmpty()) {
                throw GeneralUtil.mergeException(exs);
            }
            isClosed = true;
        } catch (Exception e) {
            throw new SQLException(e);

        }
    }

    // 代理方法
    // ------------------

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return currentResutSet.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return currentResutSet.isWrapperFor(iface);
    }

    @Override
    public boolean next() throws SQLException {
        return currentResutSet.next();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return currentResutSet.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return currentResutSet.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return currentResutSet.getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return currentResutSet.getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return currentResutSet.getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return currentResutSet.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return currentResutSet.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return currentResutSet.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return currentResutSet.getDouble(columnIndex);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return currentResutSet.getBigDecimal(columnIndex, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return currentResutSet.getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return currentResutSet.getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return currentResutSet.getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return currentResutSet.getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return currentResutSet.getAsciiStream(columnIndex);
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return currentResutSet.getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return currentResutSet.getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return currentResutSet.getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return currentResutSet.getBoolean(columnLabel);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return currentResutSet.getByte(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return currentResutSet.getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return currentResutSet.getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return currentResutSet.getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return currentResutSet.getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return currentResutSet.getDouble(columnLabel);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return currentResutSet.getBigDecimal(columnLabel, scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return currentResutSet.getBytes(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return currentResutSet.getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return currentResutSet.getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return currentResutSet.getTimestamp(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return currentResutSet.getAsciiStream(columnLabel);
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return currentResutSet.getUnicodeStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return currentResutSet.getBinaryStream(columnLabel);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return currentResutSet.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        currentResutSet.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return currentResutSet.getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return currentResutSet.getMetaData();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return currentResutSet.getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return currentResutSet.getObject(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return currentResutSet.findColumn(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return currentResutSet.getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return currentResutSet.getCharacterStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return currentResutSet.getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return currentResutSet.getBigDecimal(columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentResutSet.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return currentResutSet.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return currentResutSet.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return currentResutSet.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        currentResutSet.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        currentResutSet.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        return currentResutSet.first();
    }

    @Override
    public boolean last() throws SQLException {
        return currentResutSet.last();
    }

    @Override
    public int getRow() throws SQLException {
        return currentResutSet.getRow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return currentResutSet.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return currentResutSet.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return currentResutSet.previous();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        currentResutSet.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return currentResutSet.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        currentResutSet.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return currentResutSet.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return currentResutSet.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return currentResutSet.getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return currentResutSet.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return currentResutSet.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return currentResutSet.rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        currentResutSet.updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        currentResutSet.updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        currentResutSet.updateByte(columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        currentResutSet.updateShort(columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        currentResutSet.updateInt(columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        currentResutSet.updateLong(columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        currentResutSet.updateFloat(columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        currentResutSet.updateDouble(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        currentResutSet.updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        currentResutSet.updateString(columnIndex, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        currentResutSet.updateBytes(columnIndex, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        currentResutSet.updateDate(columnIndex, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        currentResutSet.updateTime(columnIndex, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        currentResutSet.updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        currentResutSet.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        currentResutSet.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        currentResutSet.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        currentResutSet.updateObject(columnIndex, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        currentResutSet.updateObject(columnIndex, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        currentResutSet.updateNull(columnLabel);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        currentResutSet.updateBoolean(columnLabel, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        currentResutSet.updateByte(columnLabel, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        currentResutSet.updateShort(columnLabel, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        currentResutSet.updateInt(columnLabel, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        currentResutSet.updateLong(columnLabel, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        currentResutSet.updateFloat(columnLabel, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        currentResutSet.updateDouble(columnLabel, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        currentResutSet.updateBigDecimal(columnLabel, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        currentResutSet.updateString(columnLabel, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        currentResutSet.updateBytes(columnLabel, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        currentResutSet.updateDate(columnLabel, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        currentResutSet.updateTime(columnLabel, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        currentResutSet.updateTimestamp(columnLabel, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        currentResutSet.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        currentResutSet.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        currentResutSet.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        currentResutSet.updateObject(columnLabel, x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        currentResutSet.updateObject(columnLabel, x);
    }

    @Override
    public void insertRow() throws SQLException {
        currentResutSet.insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        currentResutSet.updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        currentResutSet.deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        currentResutSet.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        currentResutSet.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        currentResutSet.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        currentResutSet.moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return currentResutSet.getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return currentResutSet.getObject(columnIndex, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return currentResutSet.getRef(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return currentResutSet.getBlob(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return currentResutSet.getClob(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return currentResutSet.getArray(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return currentResutSet.getObject(columnLabel, map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return currentResutSet.getRef(columnLabel);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return currentResutSet.getBlob(columnLabel);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return currentResutSet.getClob(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return currentResutSet.getArray(columnLabel);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return currentResutSet.getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return currentResutSet.getDate(columnLabel, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return currentResutSet.getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return currentResutSet.getTime(columnLabel, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return currentResutSet.getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return currentResutSet.getTimestamp(columnLabel, cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return currentResutSet.getURL(columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return currentResutSet.getURL(columnLabel);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        currentResutSet.updateRef(columnIndex, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        currentResutSet.updateRef(columnLabel, x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        currentResutSet.updateBlob(columnIndex, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        currentResutSet.updateBlob(columnLabel, x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        currentResutSet.updateClob(columnIndex, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        currentResutSet.updateClob(columnLabel, x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        currentResutSet.updateArray(columnIndex, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        currentResutSet.updateArray(columnLabel, x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return currentResutSet.getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return currentResutSet.getRowId(columnLabel);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        currentResutSet.updateRowId(columnIndex, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        currentResutSet.updateRowId(columnLabel, x);
    }

    @Override
    public int getHoldability() throws SQLException {
        return currentResutSet.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        currentResutSet.updateNString(columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        currentResutSet.updateNString(columnLabel, nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        currentResutSet.updateNClob(columnIndex, nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        currentResutSet.updateNClob(columnLabel, nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return currentResutSet.getNClob(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return currentResutSet.getNClob(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return currentResutSet.getSQLXML(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return currentResutSet.getSQLXML(columnLabel);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        currentResutSet.updateSQLXML(columnIndex, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        currentResutSet.updateSQLXML(columnLabel, xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return currentResutSet.getNString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return currentResutSet.getNString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return currentResutSet.getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return currentResutSet.getNCharacterStream(columnLabel);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        currentResutSet.updateNCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        currentResutSet.updateNCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        currentResutSet.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        currentResutSet.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        currentResutSet.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        currentResutSet.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        currentResutSet.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        currentResutSet.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        currentResutSet.updateBlob(columnIndex, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        currentResutSet.updateBlob(columnLabel, inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        currentResutSet.updateClob(columnIndex, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        currentResutSet.updateClob(columnLabel, reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        currentResutSet.updateNClob(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        currentResutSet.updateNClob(columnLabel, reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        currentResutSet.updateNCharacterStream(columnIndex, x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        currentResutSet.updateNCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        currentResutSet.updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        currentResutSet.updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        currentResutSet.updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        currentResutSet.updateAsciiStream(columnLabel, x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        currentResutSet.updateBinaryStream(columnLabel, x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        currentResutSet.updateCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        currentResutSet.updateBlob(columnIndex, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        currentResutSet.updateBlob(columnLabel, inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        currentResutSet.updateClob(columnIndex, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        currentResutSet.updateClob(columnLabel, reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        currentResutSet.updateNClob(columnIndex, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        currentResutSet.updateNClob(columnLabel, reader);
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        // return this.targetResultSet.getObject(columnIndex, type);
        throw new NotSupportException("getObject");
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        // return this.targetResultSet.getObject(columnLabel, type);
        throw new NotSupportException("getObject");
    }

    public MultiResultCursor getMultiResultCursor() {
        return cursor;
    }

    @Override
    public long estimateCurrentSetSize() {
        if (currentResutSet != null && currentResutSet instanceof BufferResultSet) {
            return ((BufferResultSet) currentResutSet).estimateCurrentSetSize();
        } else {
            return 0;
        }
    }
}

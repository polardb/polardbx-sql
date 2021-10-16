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

package com.alibaba.polardbx.transaction.jdbc;

import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class DeferredPreparedStatement extends DeferredStatement implements PreparedStatement {

    protected final PreparedStatement pstmt;

    public DeferredPreparedStatement(DeferredConnection conn, PreparedStatement pstmt) {
        super(conn, pstmt);
        this.pstmt = pstmt;
    }

    @Override
    public void clearParameters() throws SQLException {
        pstmt.clearParameters();
    }

    @Override
    public void addBatch() throws SQLException {
        if (conn.discardResults > 0) {
            GeneralUtil.nestedException("A deferred PreparedStatement doesn't support batch");
        }
        pstmt.addBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (conn.discardResults > 0) {
            GeneralUtil.nestedException("A deferred PreparedStatement doesn't support batch");
        }
        return pstmt.executeBatch();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        if (conn.discardResults > 0) {
            GeneralUtil.nestedException("A deferred PreparedStatement doesn't support batch");
        }
        return pstmt.executeLargeBatch();
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            return discardResults(pstmt.execute());
        } finally {
            conn.afterExecuted();
        }
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        try {
            return discardResults(pstmt.executeLargeUpdate());
        } finally {
            conn.afterExecuted();
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            return discardResults(pstmt.executeQuery());
        } finally {
            conn.afterExecuted();
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        try {
            return discardResults(pstmt.executeUpdate());
        } finally {
            conn.afterExecuted();
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return pstmt.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return pstmt.getParameterMetaData();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        pstmt.setArray(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        pstmt.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        pstmt.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        pstmt.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        pstmt.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        pstmt.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        pstmt.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        pstmt.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        pstmt.setBlob(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        pstmt.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        pstmt.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        pstmt.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        pstmt.setByte(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        pstmt.setBytes(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        pstmt.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        pstmt.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        pstmt.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        pstmt.setClob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        pstmt.setClob(parameterIndex, reader);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        pstmt.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        pstmt.setDate(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        pstmt.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        pstmt.setDouble(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        pstmt.setFloat(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        pstmt.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        pstmt.setLong(parameterIndex, x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        pstmt.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        pstmt.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        pstmt.setNClob(parameterIndex, value);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        pstmt.setNClob(parameterIndex, reader);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        pstmt.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        pstmt.setNString(parameterIndex, value);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        pstmt.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        pstmt.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        pstmt.setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        pstmt.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        pstmt.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        pstmt.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        pstmt.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        pstmt.setRef(parameterIndex, x);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        pstmt.setRowId(parameterIndex, x);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        pstmt.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        pstmt.setShort(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        pstmt.setString(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        pstmt.setTime(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        pstmt.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        pstmt.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        pstmt.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        pstmt.setURL(parameterIndex, x);
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        pstmt.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void close() throws SQLException {
        conn.discardResults = 0;
        super.close();
    }
}

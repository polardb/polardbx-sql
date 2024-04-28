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

package com.alibaba.polardbx.transaction.rawsql;

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
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RawSqlPreparedStatement extends RawSqlStatement implements PreparedStatement {

    private Map<Integer, Object>       params  = new TreeMap<>();

    private List<Map<Integer, Object>> batches = new ArrayList<>();

    public RawSqlPreparedStatement(String sql){
        super(sql);
    }

    public ResultSet executeQuery() throws SQLException {
        return null;
    }

    public int executeUpdate() throws SQLException {
        return 0;
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        params.put(parameterIndex, null);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void clearParameters() throws SQLException {
        params.clear();
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public boolean execute() throws SQLException {
        return true;
    }

    public void addBatch() throws SQLException {
        batches.add(params);
        params = new TreeMap<>();
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        params.put(parameterIndex, null);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        params.put(parameterIndex, value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        params.put(parameterIndex, xmlObject);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        params.put(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (batches.isEmpty()) {
            replaceParams(sql, params, sb);
        } else {
            for (Map<Integer, Object> params : batches) {
                replaceParams(sql, params, sb);
                sb.append(";\n");
            }
        }
        return sb.toString();
    }

    private void replaceParams(String sql, Map<Integer, Object> params, StringBuilder sb) {
        String[] parts = sql.split("\\?");
        boolean endsWithPlaceholder = sql.endsWith("?");
        if (!endsWithPlaceholder && parts.length != params.size() + 1
                || endsWithPlaceholder && parts.length != params.size()) {
            throw new IllegalArgumentException("number of params not matches number of slots");
        }
        for (int i = 0; i < params.size(); i++) {
            sb.append(parts[i]);
            RawSqlUtils.formatParameter(params.get(i + 1), sb);
        }
        if (parts.length == params.size() + 1) {
            sb.append(parts[parts.length - 1]);
        }
    }
}

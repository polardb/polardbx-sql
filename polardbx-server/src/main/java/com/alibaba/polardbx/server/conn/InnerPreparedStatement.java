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

package com.alibaba.polardbx.server.conn;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;

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

/**
 * 参见InnerConnection
 *
 * @author ziyang.lb 2020-12-05
 */
public class InnerPreparedStatement extends InnerStatement implements PreparedStatement {

    private final String sql;
    private final List<Pair<Integer, ParameterContext>> params;

    public InnerPreparedStatement(InnerConnection innerConnection, String sql) {
        super(innerConnection);
        this.sql = sql;
        this.params = new ArrayList<>();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return (ResultSet) connection.executeSql(sql, params);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int) connection.executeSql(sql, params);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
            new ParameterContext(ParameterMethod.setLong, new Object[] {parameterIndex, x}));
        params.add(pair);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
            new ParameterContext(ParameterMethod.setString, new Object[] {parameterIndex, x}));
        params.add(pair);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {parameterIndex, x}));
        params.add(pair);
    }

    // ----------------------------------------------------------------------------------------------
    // ---------------------------------------根据需求，逐步实现----------------------------------------
    // ----------------------------------------------------------------------------------------------
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {

    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {

    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {

    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {

    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
            new ParameterContext(ParameterMethod.setInt, new Object[] {parameterIndex, x}));
        params.add(pair);

    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
//        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
//            new ParameterContext(ParameterMethod.setFloat, new Object[] {parameterIndex, x}));
//        params.add(pair);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
            new ParameterContext(ParameterMethod.setDouble, new Object[] {parameterIndex, x}));
        params.add(pair);

    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
//        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
//            new ParameterContext(ParameterMethod.setBigDecimal, new Object[] {parameterIndex, x}));
//        params.add(pair);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
            new ParameterContext(ParameterMethod.setBytes, new Object[] {parameterIndex, x}));
        params.add(pair);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
            new ParameterContext(ParameterMethod.setDate1, new Object[] {parameterIndex, x}));
        params.add(pair);

    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
//        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
//            new ParameterContext(ParameterMethod.setTime1, new Object[] {parameterIndex, x}));
//        params.add(pair);

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
//        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
//            new ParameterContext(ParameterMethod.setTimestamp1, new Object[] {parameterIndex, x}));
//        params.add(pair);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void clearParameters() throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

    }

    @Override
    public boolean execute() throws SQLException {
        return false;
    }

    @Override
    public void addBatch() throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
//        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
//            new ParameterContext(ParameterMethod.setDate1, new Object[] {parameterIndex, x}));
//        params.add(pair);

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
//        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
//            new ParameterContext(ParameterMethod.setTime2, new Object[] {parameterIndex, x, cal}));
//        params.add(pair);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
//        Pair<Integer, ParameterContext> pair = new Pair<>(parameterIndex,
//            new ParameterContext(ParameterMethod.setTimestamp2, new Object[] {parameterIndex, x, cal}));
//        params.add(pair);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }
}

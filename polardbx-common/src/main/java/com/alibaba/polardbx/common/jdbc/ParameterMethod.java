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

package com.alibaba.polardbx.common.jdbc;

import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;


public enum ParameterMethod {

    setArray, setAsciiStream, setBigDecimal, setBinaryStream, setBlob, setBoolean, setByte, setBytes, //
    setCharacterStream, setClob, setDate1, setDate2, setDouble, setFloat, setInt, setLong, setBit, //
    setNull1, setNull2, setObject1, setObject2, setObject3, setRef, setShort, setString, //
    setTime1, setTime2, setTimestamp1, setTimestamp2, setURL, setUnicodeStream, setTableName, //
    setBloomFilterData, setBloomFilterDataLength, setBloomFilterFuncNum, setDelegate; //

    public boolean isBloomFilterParameterMethod() {
        return (this == setBloomFilterData)
            || (this == setBloomFilterDataLength)
            || (this == setBloomFilterFuncNum);
    }

    public static void setParameters(PreparedStatement stmt, List<Object[]> methodAndArgsList) throws SQLException {
        for (int i = 0; i < methodAndArgsList.size(); i++) {
            Object[] methodAndArgs = methodAndArgsList.get(i);
            ParameterMethod pm = (ParameterMethod) methodAndArgs[0];
            Object[] args = (Object[]) methodAndArgs[1];
            pm.setParameter(stmt, args);
        }
    }

    public static void setParameters(Statement stmt, Map<Integer, ParameterContext> parameterSettings)
        throws SQLException {
        if (!(stmt instanceof PreparedStatement)) {
            return;
        }

        if (null != parameterSettings) {
            for (ParameterContext context : parameterSettings.values()) {
                context.getParameterMethod().setParameter((PreparedStatement) stmt, context.getArgs());
            }
        }
    }

    public static void setParameters(Statement stmt, List<ParameterContext> parameterSettings) throws SQLException {
        if (!(stmt instanceof PreparedStatement)) {
            return;
        }

        if (null != parameterSettings) {
            int i = 0;
            for (ParameterContext parameterContext : parameterSettings) {
                parameterContext.getParameterMethod()
                    .setParameter((PreparedStatement) stmt, ++i, parameterContext.getArgs());
            }
        }
    }

    @SuppressWarnings("deprecation")
    public void setParameter(PreparedStatement stmt, Object... args) throws SQLException {
        setParameter(stmt, (Integer) args[0], args);
    }

    public void setParameter(PreparedStatement stmt, int index, Object... args) throws SQLException {
        switch (this) {
        case setArray:
            stmt.setArray(index, (Array) args[1]);
            break;
        case setAsciiStream:
            stmt.setAsciiStream(index, (InputStream) args[1], (Integer) args[2]);
            break;
        case setBigDecimal:
            stmt.setBigDecimal(index, (BigDecimal) args[1]);
            break;
        case setBinaryStream:
            stmt.setBinaryStream(index, (InputStream) args[1], (Integer) args[2]);
            break;
        case setBlob:
            stmt.setBlob(index, (Blob) args[1]);
            break;
        case setBoolean:
            stmt.setBoolean(index, (Boolean) args[1]);
            break;
        case setByte:
            stmt.setByte(index, (Byte) args[1]);
            break;
        case setBytes:
            stmt.setBytes(index, (byte[]) args[1]);
            break;
        case setCharacterStream:
            stmt.setCharacterStream(index, (Reader) args[1], (Integer) args[2]);
            break;
        case setClob:
            stmt.setClob(index, (Clob) args[1]);
            break;
        case setDate1:
            stmt.setDate(index, (Date) args[1]);
            break;
        case setDate2:
            stmt.setDate(index, (Date) args[1], (Calendar) args[2]);
            break;
        case setDouble:
            stmt.setDouble(index, (Double) args[1]);
            break;
        case setFloat:
            stmt.setFloat(index, (Float) args[1]);
            break;
        case setInt:
            stmt.setInt(index, (Integer) args[1]);
            break;
        case setLong:
            stmt.setLong(index, (Long) args[1]);
            break;
        case setBit:

            assert args[1] instanceof BigInteger;

            stmt.setObject(index, args[1]);
            break;
        case setNull1:

            if (args[1] == null) {
                stmt.setNull(index, Types.NULL);
            } else {
                stmt.setNull(index, (Integer) args[1]);
            }
            break;
        case setNull2:
            if (args[1] == null) {
                stmt.setNull(index, Types.NULL, (String) args[2]);
            } else {
                stmt.setNull(index, (Integer) args[1], (String) args[2]);
            }
            break;
        case setObject1:
            stmt.setObject(index, args[1]);
            break;
        case setObject2:
            stmt.setObject(index, args[1], (Integer) args[2]);
            break;
        case setObject3:
            stmt.setObject(index, args[1], (Integer) args[2], (Integer) args[3]);
            break;
        case setRef:
            stmt.setRef(index, (Ref) args[1]);
            break;
        case setShort:
            stmt.setShort(index, (Short) args[1]);
            break;
        case setString:
            stmt.setString(index, (String) args[1]);
            break;
        case setTime1:
            stmt.setTime(index, (Time) args[1]);
            break;
        case setTime2:
            stmt.setTime(index, (Time) args[1], (Calendar) args[2]);
            break;
        case setTimestamp1:
            stmt.setTimestamp(index, (Timestamp) args[1]);
            break;
        case setTimestamp2:
            stmt.setTimestamp(index, (Timestamp) args[1], (Calendar) args[2]);
            break;
        case setURL:
            stmt.setURL(index, (URL) args[1]);
            break;
        case setUnicodeStream:
            stmt.setUnicodeStream(index, (InputStream) args[1], (Integer) args[2]);
            break;

        case setTableName:
            stmt.setObject(index, new TableName((String) args[1]));
            break;
        case setBloomFilterData:
            stmt.setBytes(index, ((BloomFilterInfo) args[2]).getBytesData());
            break;
        case setBloomFilterDataLength:
            stmt.setInt(index, ((BloomFilterInfo) args[2]).getDataLenInBits());
            break;
        case setBloomFilterFuncNum:
            stmt.setInt(index, ((BloomFilterInfo) args[2]).getHashFuncNum());
            break;
        default:
            throw new IllegalArgumentException("Unhandled ParameterMethod:" + this.name());
        }
    }
}

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

package com.alibaba.polardbx.repo.mysql.common;

import com.alibaba.polardbx.common.jdbc.InvalidDate;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.repo.mysql.spi.MyJdbcHandler;
import com.mysql.jdbc.ResultSetImpl;
import org.openjdk.jol.info.ClassLayout;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

// TODO(moyi) remove it, which is not used in x-protocol
public class ResultSetWrapper {
    public static long INSTANCE_MEM_SIZE = ClassLayout.parseClass(ResultSetWrapper.class).instanceSize()
        + ClassLayout.parseClass(ResultSetImpl.class).instanceSize();

    private ResultSet rs;
    private boolean isClosed = false;
    private MyJdbcHandler jdbcHandler;

    public ResultSetWrapper(ResultSet rs, MyJdbcHandler MyJdbcHandler) {
        this.rs = rs;

        if (rs == null) {
            rs = null;
        }
        this.jdbcHandler = MyJdbcHandler;
    }

    public Object getObject(int columnIndex) throws SQLException {
        return getObject(rs, columnIndex);
    }

    public static Object getObject(ResultSet rs, int columnIndex) throws SQLException {
        try {
            Object obj = rs.getObject(columnIndex);
            try {
                if (obj instanceof Timestamp || obj instanceof Date || obj instanceof Time) {
                    // Handle zero month or zero day, which is allowed in DATE/DATETIME:
                    // https://dev.mysql.com/doc/refman/5.7/en/date-and-time-types.html
                    // However if it's returned by JDBC as Timestamp or Date, it will be converted to another date.
                    // Also java.util.Time does not keep microsecond part.
                    byte[] rawBytes = new byte[rs.getAsciiStream(columnIndex).available()];
                    rs.getAsciiStream(columnIndex).read(rawBytes);
                    MysqlDateTime mysqlDateTime = StringTimeParser.parseString(rawBytes,
                        obj instanceof Timestamp ? Types.TIMESTAMP : obj instanceof Date ? Types.DATE : Types.TIME);
                    if (obj instanceof Timestamp) {
                        OriginalTimestamp t = new OriginalTimestamp(mysqlDateTime);
                        return t;
                    } else if (obj instanceof Date) {
                        OriginalDate t = new OriginalDate(mysqlDateTime);
                        return t;
                    } else {
                        OriginalTime t = new OriginalTime(mysqlDateTime);
                        return t;
                    }
                }
            } catch (Throwable e) {
                // Do nothing, just return the object before conversion.
            }
            return obj;
        } catch (SQLException ex) {
            if (TStringUtil.containsIgnoreCase(ex.getMessage(), "0000-00-00")
                && TStringUtil.containsIgnoreCase(ex.getMessage(), "can not be represented as java.sql.Timestamp")) {
                Timestamp ts = ZeroTimestamp.instance;
                return ts;
            } else if (TStringUtil.containsIgnoreCase(ex.getMessage(), "0000-00-00")
                && TStringUtil.containsIgnoreCase(ex.getMessage(), "can not be represented as java.sql.Date")) {
                ZeroDate ts = ZeroDate.instance;
                return ts;
            } else if (TStringUtil.containsIgnoreCase(ex.getMessage(), "can not be represented as java.sql.Date")) {
                return InvalidDate.instance; // Mainly for year type
            } else if (rs.getMetaData().getColumnType(columnIndex) == 92 && TStringUtil
                .containsIgnoreCase(ex.getMessage(), "Bad format for Time")) {
                try {
                    byte[] rawBytes = new byte[rs.getAsciiStream(columnIndex).available()];
                    rs.getAsciiStream(columnIndex).read(rawBytes);
                    MysqlDateTime mysqlDateTime = StringTimeParser.parseString(rawBytes, Types.TIME);
                    OriginalTime t = new OriginalTime(mysqlDateTime);
                    return t;
                } catch (Exception e) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }
    }


    public byte[] getBytes(int columnIndex) throws SQLException {
        return getBytes(rs, columnIndex);
    }

    public static byte[] getBytes(ResultSet rs, int columnIndex) throws SQLException {
        try {
            return rs.getBytes(columnIndex);
        } catch (SQLException ex) {
            if (TStringUtil.containsIgnoreCase(ex.getMessage(), "0000-00-00")
                && TStringUtil.containsIgnoreCase(ex.getMessage(), "can not be represented as java.sql.Timestamp")) {
                return String.valueOf(getObject(rs, columnIndex)).getBytes();
            } else if (TStringUtil.containsIgnoreCase(ex.getMessage(), "0000-00-00")
                && TStringUtil.containsIgnoreCase(ex.getMessage(), "can not be represented as java.sql.Date")) {
                return String.valueOf(getObject(rs, columnIndex)).getBytes();
            } else if (TStringUtil.containsIgnoreCase(ex.getMessage(), "can not be represented as java.sql.Date")) {
                return String.valueOf(getObject(rs, columnIndex)).getBytes(); // Reading
                // Friendly
            } else if (rs.getMetaData().getColumnType(columnIndex) == 92 && TStringUtil
                .containsIgnoreCase(ex.getMessage(), "Bad format for Time")) {
                try {
                    byte[] rawBytes = new byte[rs.getAsciiStream(columnIndex).available()];
                    rs.getAsciiStream(columnIndex).read(rawBytes);
                    String str = new String(rawBytes);
                    Time t = Time.valueOf(str);
                    return String.valueOf(t).getBytes();
                } catch (Exception e) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }
    }
}

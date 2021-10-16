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

package com.alibaba.polardbx.server.executor.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.alibaba.polardbx.net.util.MySQLMessage;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

/**
 * MysqlDefs contains many values that are needed for communication with the
 * MySQL server.
 *
 * @author Mark Matthews
 * @version $Id$
 */
public final class MysqlDefs {

    private static final BigDecimal MAX_LONGLONG_VALUE = new BigDecimal(Long.MAX_VALUE);
    private static final BigDecimal MAX_UNSIGNED_LONGLONG_VALUE = new BigDecimal("18446744073709551616");

    // ~ Static fields/initializers
    // ---------------------------------------------

    public static final int COM_BINLOG_DUMP = 18;

    public static final int COM_CHANGE_USER = 17;

    public static final int COM_CLOSE_STATEMENT = 25;

    public static final int COM_CONNECT_OUT = 20;

    public static final int COM_END = 29;

    public static final int COM_EXECUTE = 23;

    public static final int COM_FETCH = 28;

    public static final int COM_LONG_DATA = 24;

    public static final int COM_PREPARE = 22;

    public static final int COM_REGISTER_SLAVE = 21;

    public static final int COM_RESET_STMT = 26;

    public static final int COM_SET_OPTION = 27;

    public static final int COM_TABLE_DUMP = 19;

    public static final int CONNECT = 11;

    public static final int CREATE_DB = 5;

    public static final int DEBUG = 13;

    public static final int DELAYED_INSERT = 16;

    public static final int DROP_DB = 6;

    public static final int FIELD_LIST = 4;

    public static final int FIELD_TYPE_BIT = 16;

    public static final int FIELD_TYPE_BLOB = 252;

    public static final int FIELD_TYPE_DATE = 10;

    public static final int FIELD_TYPE_DATETIME = 12;

    // Data Types
    public static final int FIELD_TYPE_DECIMAL = 0;

    public static final int FIELD_TYPE_DOUBLE = 5;

    public static final int FIELD_TYPE_ENUM = 247;

    public static final int FIELD_TYPE_FLOAT = 4;

    public static final int FIELD_TYPE_GEOMETRY = 255;

    public static final int FIELD_TYPE_INT24 = 9;

    public static final int FIELD_TYPE_LONG = 3;

    public static final int FIELD_TYPE_LONG_BLOB = 251;

    public static final int FIELD_TYPE_LONGLONG = 8;

    public static final int FIELD_TYPE_MEDIUM_BLOB = 250;

    public static final int FIELD_TYPE_NEW_DECIMAL = 246;

    public static final int FIELD_TYPE_NEWDATE = 14;

    public static final int FIELD_TYPE_NULL = 6;

    public static final int FIELD_TYPE_SET = 248;

    public static final int FIELD_TYPE_SHORT = 2;

    public static final int FIELD_TYPE_STRING = 254;

    public static final int FIELD_TYPE_TIME = 11;

    public static final int FIELD_TYPE_TIMESTAMP = 7;

    public static final int FIELD_TYPE_TINY = 1;

    // Older data types
    public static final int FIELD_TYPE_TINY_BLOB = 249;

    public static final int FIELD_TYPE_VAR_STRING = 253;

    public static final int FIELD_TYPE_VARCHAR = 15;

    public static final int FIELD_TYPE_JSON = 245;

    // Newer data types
    public static final int FIELD_TYPE_YEAR = 13;

    public static final int INIT_DB = 2;

    static final long LENGTH_BLOB = 65535;

    static final long LENGTH_LONGBLOB = 4294967295L;

    static final long LENGTH_MEDIUMBLOB = 16777215;

    static final long LENGTH_TINYBLOB = 255;

    // Limitations
    public static final int MAX_ROWS = 50000000;                              // From
    // the
    // MySQL
    // FAQ

    /**
     * Used to indicate that the server sent no field-level character set
     * information, so the driver should use the connection-level character
     * encoding instead.
     */
    public static final int NO_CHARSET_INFO = -1;

    static final byte OPEN_CURSOR_FLAG = 1;

    public static final int PING = 14;

    public static final int PROCESS_INFO = 10;

    public static final int PROCESS_KILL = 12;

    public static final int QUERY = 3;

    public static final int QUIT = 1;

    // ~ Methods
    // ----------------------------------------------------------------

    public static final int RELOAD = 7;

    public static final int SHUTDOWN = 8;

    //
    // Constants defined from mysql
    //
    // DB Operations
    public static final int SLEEP = 0;

    public static final int STATISTICS = 9;

    public static final int TIME = 15;

    public static int javaTypeDetect(int javaType, int scale) {
        switch (javaType) {
        case Types.NUMERIC: {
            if (scale > 0) {
                return Types.DECIMAL;
            } else {
                return javaType;
            }
        }
        default: {
            return javaType;
        }
        }

    }

    /**
     * http://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
     * refert to mysql connector's Buffer.readFieldLength
     */
    public static int readLengthEncodedInteger(MySQLMessage mm) {
        int result = mm.read(); // should transfer byte to int to make sure it's
        // not negative
        if (result == 251) {
            return -1;
        } else if (result == 0xfc) {
            byte b2 = mm.read();
            byte b3 = mm.read();
            result = (b3 << 8) | (b2 & 0xff);
        } else if (result == 0xfd) {
            byte b2 = mm.read();
            byte b3 = mm.read();
            byte b4 = mm.read();
            result = (b4 << 16) | (b3 << 8) | (b2 & 0xff);
        } else if (result == 0xfe) {
            byte b2 = mm.read();
            byte b3 = mm.read();
            byte b4 = mm.read();
            byte b5 = mm.read();
            byte b6 = mm.read();
            byte b7 = mm.read();
            byte b8 = mm.read();
            byte b9 = mm.read();
            result = (b9 << 56) | (b8 << 48) | (b7 << 40) | (b6 << 32) | (b5 << 24) | (b4 << 16) | (b3 << 8)
                | (b2 & 0xff);
        }
        return result;
    }

    /**
     * http://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
     */
    public static byte[] LengthEncodedInteger(int len) {
        byte[] result = new byte[0];
        if (len < 251) {
            result = new byte[1];
            result[0] = (byte) len;
        } else if (len >= 251 && len < (1 << 16)) {
            result = new byte[3];
            result[0] = (byte) 0xfc;
            result[1] = (byte) (len & 0xff);
            result[2] = (byte) ((len >> 8) & 0xff);
        } else if (len >= (1 << 16) && len <= (1 << 24)) {
            result = new byte[4];
            result[0] = (byte) 0xfd;
            result[1] = (byte) ((len) & 0xff);
            result[2] = (byte) ((len >> 8) & 0xff);
            result[3] = (byte) ((len >> 16) & 0xff);
        } else if (len >= (1 << 24)) {
            result = new byte[9];
            result[0] = (byte) 0xfe;
            result[1] = (byte) ((len) & 0xff);
            result[2] = (byte) ((len >> 8) & 0xff);
            result[3] = (byte) ((len >> 16) & 0xff);
            result[4] = (byte) ((len >> 24) & 0xff);
            result[5] = (byte) ((len >> 32) & 0xff);
            result[6] = (byte) ((len >> 40) & 0xff);
            result[7] = (byte) ((len >> 48) & 0xff);
            result[8] = (byte) ((len >> 56) & 0xff);
        }

        return result;
    }

    public static int MySQLTypeUInt(int type) {
        if (type < 0) {
            return type + 256;
        } else {
            return type;
        }
    }

    /**
     * http://dev.mysql.com/doc/internals/en/binary-protocol-value.html
     */
    public static byte[] resultSetToByte(ResultSet rs, int column, int mysqlType, boolean unsigned, String charset)
        throws SQLException {
        byte[] result = null;
        if (mysqlType == FIELD_TYPE_NEW_DECIMAL || mysqlType == FIELD_TYPE_DECIMAL) {
            BigDecimal v = rs.getBigDecimal(column);
            // v == null if the column cell is null value
            if (v != null) {
                result = convertBigDecimal(v, charset);
            }
        } else if (mysqlType == FIELD_TYPE_TINY) {
            byte v = 0;
            if (unsigned) {
                short sv = rs.getShort(column);
                if (!rs.wasNull()) {
                    // convert to signed value
                    if (v > 127) {
                        v = (byte) (sv - 256);
                    } else {
                        v = (byte) sv;
                    }
                }
            } else {
                v = rs.getByte(column);
            }

            if (!rs.wasNull()) {
                result = new byte[1];
                result[0] = v;
            }
        } else if (mysqlType == FIELD_TYPE_SHORT) {
            short v = 0;
            if (unsigned) {
                int iv = rs.getInt(column);
                if (!rs.wasNull()) {
                    // convert to signed value
                    if (v > 32767) {
                        v = (short) (iv - 65536);
                    } else {
                        v = (short) iv;
                    }
                }
            } else {
                v = rs.getShort(column);
            }

            if (!rs.wasNull()) {
                result = new byte[2];
                result[0] = (byte) (v & 0xff);
                result[1] = (byte) (v >> 8);
            }
        } else if (mysqlType == FIELD_TYPE_LONG || mysqlType == FIELD_TYPE_INT24) {
            long v = rs.getLong(column);
            if (!rs.wasNull()) {
                // convert to signed value
                if (unsigned) {
                    if (mysqlType == FIELD_TYPE_INT24) {
                        // v = (int) (v - 16777216L);
                        // if (v > 8388607) {
                        // throw new
                        // IllegalArgumentException("not support unsigned mediumint");
                        // }
                    } else {
                        if (v > 2147483647) {
                            v = (int) (v - 4294967296L);
                        }
                    }
                }
                // java is small endian always
                // ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN
                result = new byte[4];
                result[0] = (byte) ((v) & 0xff);
                result[1] = (byte) ((v >> 8) & 0xff);
                result[2] = (byte) ((v >> 16) & 0xff);
                result[3] = (byte) ((v >> 24) & 0xff);
            }
        } else if (mysqlType == FIELD_TYPE_FLOAT) {
            float v = rs.getFloat(column);
            if (!rs.wasNull()) {
                result = new byte[4];
                int bits = Float.floatToIntBits(v); // see javadoc of
                // floatToIntBits
                result[0] = (byte) ((bits) & 0xff);
                result[1] = (byte) ((bits >> 8) & 0xff);
                result[2] = (byte) ((bits >> 16) & 0xff);
                result[3] = (byte) ((bits >> 24) & 0xff);
            }
        } else if (mysqlType == FIELD_TYPE_DOUBLE) {
            double v = rs.getDouble(column);
            if (!rs.wasNull()) {
                result = new byte[8];
                long bits = Double.doubleToLongBits(v); // see java doc of
                // doubleToLongBits
                result[7] = (byte) ((bits >> 56) & 0xff);
                result[6] = (byte) ((bits >> 48) & 0xff);
                result[5] = (byte) ((bits >> 40) & 0xff);
                result[4] = (byte) ((bits >> 32) & 0xff);
                result[3] = (byte) ((bits >> 24) & 0xff);
                result[2] = (byte) ((bits >> 16) & 0xff);
                result[1] = (byte) ((bits >> 8) & 0xff);
                result[0] = (byte) ((bits) & 0xff);
            }
        } else if (mysqlType == FIELD_TYPE_NULL) {
            // just return null
        } else if (mysqlType == FIELD_TYPE_LONGLONG) {
            long v = 0;
            if (unsigned) {
                BigDecimal bv = rs.getBigDecimal(column);
                if (!rs.wasNull()) {
                    // convert to signed value
                    if (bv.compareTo(MAX_LONGLONG_VALUE) > 0) {
                        long unsignedValue = bv.longValue() & Long.MAX_VALUE;
                        unsignedValue |= Long.MIN_VALUE;
                        v = unsignedValue;
                    } else {
                        v = bv.longValue();
                    }
                }
            } else {
                v = rs.getLong(column);
            }

            if (!rs.wasNull()) {
                result = new byte[8];
                result[7] = (byte) ((v >> 56) & 0xff);
                result[6] = (byte) ((v >> 48) & 0xff);
                result[5] = (byte) ((v >> 40) & 0xff);
                result[4] = (byte) ((v >> 32) & 0xff);
                result[3] = (byte) ((v >> 24) & 0xff);
                result[2] = (byte) ((v >> 16) & 0xff);
                result[1] = (byte) ((v >> 8) & 0xff);
                result[0] = (byte) ((v) & 0xff);
            }
        } else if (mysqlType == FIELD_TYPE_DATE) {
            try {
                Date v = rs.getDate(column);
                if (v != null) {
                    // always set to 4+1
                    result = convertDate4(v);
                }
            } catch (Exception e) {
                result = new byte[] {0};
            }

        } else if (mysqlType == FIELD_TYPE_TIME) {
            /**
             * compare with 1970.1.1 on mysql connector
             * ResultSetRaw.getColumnValue always use timestamp since we need to
             * calculate day, Time not valid for getting day parameters
             */
            try {
                Time v = rs.getTime(column);
                if (v != null) {
                    // always set to max 12+1 with milliseconds
                    result = convertTime12(v);
                }
            } catch (Exception e) {
                result = new byte[] {0};
            }
        } else if (mysqlType == FIELD_TYPE_DATETIME || mysqlType == FIELD_TYPE_TIMESTAMP) {
            try {
                Timestamp v = rs.getTimestamp(column);
                if (v != null) {
                    // always set to max 12+1 with milliseconds
                    result = convertDate11(v);
                }
            } catch (Exception e) {
                result = new byte[] {0};

            }
        } else if (mysqlType == FIELD_TYPE_YEAR) {
            try {
                String s = rs.getString(column);
                if (StringUtils.isNotEmpty(s)) {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy");
                    java.util.Date parse = format.parse(s);
                    java.util.Date date = (java.util.Date) parse;
                    result = convertDate2(new Date(date.getTime()));
                } else {
                    result = null;
                }
            } catch (Exception e) {
                result = new byte[] {0};

            }
        } else if (mysqlType == FIELD_TYPE_NEWDATE) {
            try {
                // same to date mysql connector MysqlDefs.mysqlToJavaType
                Date v = rs.getDate(column);
                if (v != null) {
                    result = convertDate4(v);
                }
            } catch (Exception e) {
                result = new byte[] {0};

            }
        } else if (mysqlType == FIELD_TYPE_ENUM) {
            // enum same to char & varchar by mysql connecotor.
            // MysqlDefs.mysqlToJavaType
            String v = rs.getString(column);
            if (v != null) {
                result = convertVarStringLikeValue(v, charset);
            }
        } else if (mysqlType == FIELD_TYPE_SET) {
            // enum same to char & varchar by mysql connecotor.
            // MysqlDefs.mysqlToJavaType
            String v = rs.getString(column);
            if (v != null) {
                result = convertVarStringLikeValue(v, charset);
            }
        } else if (mysqlType == FIELD_TYPE_TINY_BLOB) {
            // refer to mysql connector ResultSetImpl.getObject
            byte[] v = rs.getBytes(column);
            if (v != null) {
                result = convertVarBytes(v);
            }
        } else if (mysqlType == FIELD_TYPE_MEDIUM_BLOB) {
            // refer to mysql connector ResultSetImpl.getObject
            byte[] v = rs.getBytes(column);
            if (v != null) {
                result = convertVarBytes(v);
            }
        } else if (mysqlType == FIELD_TYPE_LONG_BLOB) {
            // refer to mysql connector ResultSetImpl.getObject
            byte[] v = rs.getBytes(column);
            if (v != null) {
                result = convertVarBytes(v);
            }
        } else if (mysqlType == FIELD_TYPE_BLOB) {
            // refer to mysql connector ResultSetImpl.getObject
            byte[] v = rs.getBytes(column);
            if (v != null) {
                result = convertVarBytes(v);
            }
        } else if (mysqlType == FIELD_TYPE_VAR_STRING || mysqlType == FIELD_TYPE_VARCHAR
            || mysqlType == FIELD_TYPE_STRING) {
            String v = rs.getString(column);
            if (v != null) {
                result = convertVarStringLikeValue(v, charset);
            }
        } else if (mysqlType == FIELD_TYPE_GEOMETRY) {
            // refers to mysql connector ResultSetImpl.getObject
            // InputStream v = rs.getBinaryStream(column);
            byte[] v = rs.getBytes(column);
            if (v != null) {
                result = convertVarBytes(v);
                // result = convertInputBinaryStream(v);
            }
        } else if (mysqlType == FIELD_TYPE_BIT) {
            // same as string
            final byte[] bytes = rs.getBytes(column);
            if (bytes != null) {
                result = convertVarBytes(bytes);
            }
        } else {
            // default set to string
            String v = rs.getString(column);
            if (v != null) {
                result = convertVarStringLikeValue(v, charset);
            }
        }

        return result;
    }

    public static int javaTypeMysql(int javaType) {
        switch (javaType) {
        case Types.NUMERIC:
        case Types.DECIMAL:
            return MysqlDefs.FIELD_TYPE_NEW_DECIMAL;

        case Types.TINYINT:
            return MysqlDefs.FIELD_TYPE_TINY;

        case Types.SMALLINT:
            return MysqlDefs.FIELD_TYPE_SHORT;

        case Types.INTEGER:
            return MysqlDefs.FIELD_TYPE_LONG;

        case Types.REAL:
        case Types.FLOAT:
            return MysqlDefs.FIELD_TYPE_FLOAT;

        case Types.DOUBLE:
            return MysqlDefs.FIELD_TYPE_DOUBLE;

        case Types.NULL:
            return MysqlDefs.FIELD_TYPE_NULL;

        case Types.TIMESTAMP:
            return MysqlDefs.FIELD_TYPE_TIMESTAMP;

        case Types.BIGINT:
            return MysqlDefs.FIELD_TYPE_LONGLONG;

        case Types.DATE:
            return MysqlDefs.FIELD_TYPE_DATE;

        case Types.TIME:
            return MysqlDefs.FIELD_TYPE_TIME;

        case Types.VARBINARY:
            return MysqlDefs.FIELD_TYPE_TINY_BLOB;

        case Types.LONGVARBINARY:
            return MysqlDefs.FIELD_TYPE_BLOB;

        case Types.VARCHAR:
            return MysqlDefs.FIELD_TYPE_VAR_STRING;

        case Types.CHAR:
            return MysqlDefs.FIELD_TYPE_STRING;

        case Types.BINARY:
            return MysqlDefs.FIELD_TYPE_LONG_BLOB;

        case Types.BIT:
            return MysqlDefs.FIELD_TYPE_BIT;

        case Types.CLOB:
            return MysqlDefs.FIELD_TYPE_VAR_STRING;

        case Types.BLOB:
            return MysqlDefs.FIELD_TYPE_BLOB;

        case Types.BOOLEAN:
            return MysqlDefs.FIELD_TYPE_TINY;

        case DataType.YEAR_SQL_TYPE:
            return MysqlDefs.FIELD_TYPE_YEAR;

        case DataType.MEDIUMINT_SQL_TYPE:
            return MysqlDefs.FIELD_TYPE_INT24;

        case DataType.DATETIME_SQL_TYPE:
            return MysqlDefs.FIELD_TYPE_DATETIME;

        case DataType.JSON_SQL_TYPE:
            return MysqlDefs.FIELD_TYPE_JSON;

        default:
            return MysqlDefs.FIELD_TYPE_STRING;
        }

    }

    private static byte[] convertVarStringLikeValue(String v, String charset) {
        byte[] vbytes = StringUtil.encode(v, charset);
        byte[] lens = LengthEncodedInteger(vbytes.length); // getlength
        ByteArrayOutputStream bb = new ByteArrayOutputStream(lens.length + vbytes.length);
        /**
         * IMPORTANT!!! mysql connector only use little endian at
         * ResultSetRow.getNativeDate for binary result
         */
        LittleEndianDataOutputStream dout = new LittleEndianDataOutputStream(bb);

        try {
            dout.write(lens); // write length
            dout.write(vbytes); // write string
        } catch (IOException e) {
            return null;
        } finally {
            IOUtils.closeQuietly(dout);
        }

        return bb.toByteArray();
    }

    private static byte[] convertVarBytes(byte[] v) {
        byte[] lens = LengthEncodedInteger(v.length); // getlength
        ByteArrayOutputStream bb = new ByteArrayOutputStream(lens.length + v.length);
        /**
         * IMPORTANT!!! mysql connector only use little endian at
         * ResultSetRow.getNativeDate for binary result
         */
        LittleEndianDataOutputStream dout = new LittleEndianDataOutputStream(bb);

        try {
            dout.write(lens);
            dout.write(v);
        } catch (IOException e) {
            return null;
        } finally {
            IOUtils.closeQuietly(dout);
        }

        return bb.toByteArray();
    }

    private static byte[] convertDate4(Date v) {
        return TimeStorage.convertDate4(v);
    }

    private static byte[] convertTime12(Time v) {
        ByteArrayOutputStream bb = new ByteArrayOutputStream(12 + 1);
        /**
         * IMPORTANT!!! mysql connector only use little endian at
         * ResultSetRow.getNativeDate for binary result
         */
        LittleEndianDataOutputStream dout = new LittleEndianDataOutputStream(bb);
        try {
            dout.writeByte(12); // length

            Calendar cal = Calendar.getInstance();
            cal.clear(); // 1970.1.1

            long timeInMillis = v.getTime() - cal.getTimeInMillis();
            final boolean minus = (timeInMillis < 0);
            if (minus) {
                timeInMillis = -timeInMillis;
            }
            final int millisecond = (int) (timeInMillis % 1000);
            timeInMillis = timeInMillis / 1000;
            final int second = (int) (timeInMillis % 60);
            timeInMillis = timeInMillis / 60;
            final int minute = (int) (timeInMillis % 60);
            timeInMillis = timeInMillis / 60;
            final int hour = (int) (timeInMillis % 24);
            final int days = (int) (timeInMillis / 24);

            if (!minus) {
                dout.writeByte(0);
                dout.writeInt(days);
            } else {
                dout.writeByte(1); // negative
                dout.writeInt(days);
            }
            dout.writeByte(hour);
            dout.writeByte(minute);
            dout.writeByte(second);

            dout.writeInt(millisecond);

        } catch (IOException e) {
            return null;
        } finally {
            IOUtils.closeQuietly(dout);
        }

        return bb.toByteArray();
    }

    private static byte[] convertDate11(Timestamp v) {
        return TimeStorage.convertDate11(v);
    }

    private static byte[] convertDate2(Date v) {
        return TimeStorage.convertDate2(v);
    }

    private static byte[] convertBigDecimal(BigDecimal v, String charset) {
        return convertVarStringLikeValue(v.toString(), charset);
    }
}

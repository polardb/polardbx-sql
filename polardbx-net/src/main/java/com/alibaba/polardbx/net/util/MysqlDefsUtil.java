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

package com.alibaba.polardbx.net.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

/**
 * MysqlDefs contains many values that are needed for communication with the
 * MySQL server.
 *
 * @author simiao.zw
 * @version $Id$
 */
public final class MysqlDefsUtil {

    private static final Logger logger = LoggerFactory.getLogger(MysqlDefsUtil.class);

    static final int FIELD_TYPE_BIT = 16;

    static final int FIELD_TYPE_BLOB = 252;

    static final int FIELD_TYPE_DATE = 10;

    static final int FIELD_TYPE_DATETIME = 12;

    // Data Types
    static final int FIELD_TYPE_DECIMAL = 0;

    static final int FIELD_TYPE_DOUBLE = 5;

    static final int FIELD_TYPE_ENUM = 247;

    static final int FIELD_TYPE_FLOAT = 4;

    static final int FIELD_TYPE_GEOMETRY = 255;

    static final int FIELD_TYPE_INT24 = 9;

    static final int FIELD_TYPE_LONG = 3;

    static final int FIELD_TYPE_LONG_BLOB = 251;

    static final int FIELD_TYPE_LONGLONG = 8;

    static final int FIELD_TYPE_MEDIUM_BLOB = 250;

    static final int FIELD_TYPE_NEW_DECIMAL = 246;

    static final int FIELD_TYPE_NEWDATE = 14;

    static final int FIELD_TYPE_NULL = 6;

    static final int FIELD_TYPE_SET = 248;

    static final int FIELD_TYPE_SHORT = 2;

    static final int FIELD_TYPE_STRING = 254;

    static final int FIELD_TYPE_TIME = 11;

    static final int FIELD_TYPE_TIMESTAMP = 7;

    static final int FIELD_TYPE_TINY = 1;

    // Older data types
    static final int FIELD_TYPE_TINY_BLOB = 249;

    static final int FIELD_TYPE_VAR_STRING = 253;

    static final int FIELD_TYPE_VARCHAR = 15;

    // Newer data types
    static final int FIELD_TYPE_YEAR = 13;

    /**
     * http://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
     * Here we should adopt byte & 0xff, otherwise out length might be -1.
     */
    public static int readLengthEncodedInteger(MySQLMessage mm) {
        int result = 0;
        byte b = mm.read();
        int b1 = b & 0xff;
        if (b1 < 0xfb) {
            result = b1 & 0xff;
        } else if (b1 == 0xfc) {
            byte b2 = mm.read();
            byte b3 = mm.read();
            result = ((b3 & 0xff) << 8) | (b2 & 0xff);
        } else if (b1 == 0xfd) {
            byte b2 = mm.read();
            byte b3 = mm.read();
            byte b4 = mm.read();
            result = ((b4 & 0xff) << 16) | ((b3 & 0xff) << 8) | (b2 & 0xff);
        } else if (b1 == 0xfe) {
            byte b2 = mm.read();
            byte b3 = mm.read();
            byte b4 = mm.read();
            byte b5 = mm.read();
            byte b6 = mm.read();
            byte b7 = mm.read();
            byte b8 = mm.read();
            byte b9 = mm.read();
            result = ((b9 & 0xff) << 56) | ((b8 & 0xff) << 48) | ((b7 & 0xff) << 40) | ((b6 & 0xff) << 32)
                | ((b5 & 0xff) << 24) | ((b4 & 0xff) << 16) | ((b3 & 0xff) << 8) | (b2 & 0xff);
        }
        return result;
    }

    public static Object readObject(MySQLMessage mm, short mysqlType, boolean fullblob, String encode) {

        boolean unsigned = ((mysqlType >>> 8) & 0xff) == 0x80;
        mysqlType = (short) (mysqlType & 0x00ff);

        if (mysqlType == FIELD_TYPE_STRING || mysqlType == FIELD_TYPE_VARCHAR || mysqlType == FIELD_TYPE_VAR_STRING ||
            /* from parser, the column set will be set to StringType */
            mysqlType == FIELD_TYPE_SET ||
            /* From parser, the column enum will be set to StringType */
            mysqlType == FIELD_TYPE_ENUM) {
            // refers to ResultSet --> byte[] on MysqlDefs
            int len = readLengthEncodedInteger(mm);

            if (len == 0) {
                /**
                 * All column in prepare will be STRING, EXECUTE will send
                 * actual params types along with value in packet's
                 * "type of each parameter" section, so here whatever Java or C
                 * connector can determine exact value types without
                 * misunderstanding. Even if client use wrong prams type within
                 * mysql_stmt_bind_param, for example, MYSQL_TYPE_LONG was set
                 * to MYSQL_TYPE_STRING, if actual value is is_null == 1, then
                 * nullmap will deal with it. else if actual value is '', both
                 * mysql and java jdbc driver will report error
                 * "Incorrect Integer value"
                 */
                if (logger.isDebugEnabled()) {
                    StringBuffer info = new StringBuffer();
                    info.append("[readObject]");
                    info.append("[sqlType:").append(mysqlType).append(", ");
                    info.append("length:").append(len).append(", ");
                    info.append("value force to null]");
                    logger.debug(info.toString());
                }
                return "";
            }

            Object var = readEncodeBytes(mm, encode, len);
            if (logger.isDebugEnabled()) {
                StringBuffer info = new StringBuffer();
                info.append("[readObject]");
                info.append("[sqlType:").append(mysqlType).append(", ");
                info.append("length:").append(len).append(", ");
                info.append("value:").append(var.toString()).append("]");
                logger.debug(info.toString());
            }
            return var;
        } else if (mysqlType == FIELD_TYPE_BIT) {
            /* from parser, the column bit will be set to BitType */
            int len = readLengthEncodedInteger(mm);
            return readEncodeBytes(mm, encode, len);
        } else if (mysqlType == FIELD_TYPE_DECIMAL || mysqlType == FIELD_TYPE_NEW_DECIMAL) {
            int len = readLengthEncodedInteger(mm);
            return readEncodeBytes(mm, encode, len);
        } else if (mysqlType == FIELD_TYPE_LONG_BLOB || mysqlType == FIELD_TYPE_MEDIUM_BLOB
            || mysqlType == FIELD_TYPE_BLOB || mysqlType == FIELD_TYPE_TINY_BLOB
            || mysqlType == FIELD_TYPE_GEOMETRY) {
            /**
             * Since in test, found send_long_date blob value has no length, so
             * we just guard them by a ByteArrayInputStream buffer, and use eof
             * to check if it reach end IMPORTANT!!! because the blob may be
             * transferred by send_long_data packet, so here should determine if
             * current paramIndex for blob is ready being filled, if so, just
             * skip it should not use readBlobTillEnd here, it will only be used
             * at send_data_long here should always be a size before bytes
             */
            if (fullblob) {
                return readBlobTillEnd(mm);
            } else {
                return readBlob(mm);
            }
        } else if (mysqlType == FIELD_TYPE_LONGLONG) {
            if (unsigned) {
                return toUnsignedBigInteger(mm.readLong());
            } else {
                return mm.readLong();
            }
        } else if (mysqlType == FIELD_TYPE_LONG || mysqlType == FIELD_TYPE_INT24) {
            if (unsigned) {
                return Integer.toUnsignedLong(mm.readInt());
            } else {
                return mm.readInt();
            }
        } else if (mysqlType == FIELD_TYPE_SHORT || mysqlType == FIELD_TYPE_YEAR) {
            if (unsigned) {
                return Short.toUnsignedInt((short) mm.readUB2());
            } else {
                short v = (short) mm.readUB2();
                return v;
            }
        } else if (mysqlType == FIELD_TYPE_TINY) {
            if (unsigned) {
                return Byte.toUnsignedInt(mm.read());
            } else {
                byte v = mm.read();
                return v;
            }
        } else if (mysqlType == FIELD_TYPE_DOUBLE) {
            /**
             * MYSQL_TYPE_DOUBLE stores a floating point in IEEE 754 double
             * precision format first byte is the last byte of the significant
             * as stored in C.
             */
            // byte[] v = mm.readBytes(8);
            // String hex = new String(v);
            // Double.longBitsToDouble(Long.valueOf(hex, 16));
            // return Double.longBitsToDouble(byte8(v));
            return mm.readDouble();
        } else if (mysqlType == FIELD_TYPE_FLOAT) {
            /**
             * MYSQL_TYPE_FLOAT stores a floating point in IEEE 754 single
             * precision format
             */
            return mm.readFloat();
            // String hex = new String(mm.readBytes(4));
            // return Float.intBitsToFloat(Integer.valueOf(hex, 16));
        } else if (mysqlType == FIELD_TYPE_DATE || mysqlType == FIELD_TYPE_DATETIME
            || mysqlType == FIELD_TYPE_TIMESTAMP) {
            /**
             * length (1) -- number of bytes following (valid values: 0, 4, 7,
             * 11) year (2) -- year month (1) -- month day (1) -- day hour (1)
             * -- hour minute (1) -- minutes second (1) -- seconds micro_second
             * (4) -- micro-seconds
             */
            // byte[] to date
            /**
             * http://dev.mysql.com/doc/refman/5.0/en/datetime.html DATE type
             * range from '1000-01-01' to '9999-12-31' DATETIME type range from
             * '1000-01-01 00:00:00' to '9999-12-31 23:59:59' TIMESTAMP type
             * range from '1970-01-01 00:00:01' to '2038-01-19 03:14:07'
             */
            int len = mm.read(); // 0,4,7,11
            if (len == 0) {
                return "0000-00-00 00:00:00.000";
            }
            if (len == 4) {
                return readDate3(mm);
            } else if (len == 7) {
                return readDate6(mm);
            } else if (len == 11) {
                return readTimeStamp(mm);
            }
        } else if (mysqlType == FIELD_TYPE_TIME) {
            int len = mm.read(); // 0, 8, 12
            if (len == 0) {
                return "00:00:00";
            }

            if (len == 8) {
                return readTime3(mm);
            } else if (len == 12) {
                return readTime4(mm);
            }
        } else if (mysqlType == FIELD_TYPE_NULL) {
            /**
             * http://dev.mysql.com/doc/internals/en/null-bitmap.html do nothing
             * since nullmap process it.
             */
        }

        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "parameter of mysqltype:" + mysqlType + " fullblob:"
            + fullblob + " encode:" + encode);
    }

    private static BigInteger toUnsignedBigInteger(long i) {
        if (i >= 0L) {
            return BigInteger.valueOf(i);
        } else {
            int upper = (int) (i >>> 32);
            int lower = (int) i;
            // return (upper << 32) + lower
            return BigInteger.valueOf(Integer.toUnsignedLong(upper))
                .shiftLeft(32)
                .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
        }
    }

    private static Object readEncodeBytes(MySQLMessage mm, String encode, int len) {
        Charset charset;
        try {
            charset = Charset.forName(CharsetUtil.getJavaCharset(encode));
        } catch (Exception e) {
            throw new IllegalArgumentException("Unknown charset '" + encode + "'");
        }
        CharsetDecoder decoder = charset.newDecoder();
        // Let decoder throws error if sees invalid data
        decoder.onMalformedInput(CodingErrorAction.REPORT);

        byte[] bytes = mm.readBytes(len);
        try {
            CharBuffer decoded = decoder.decode(ByteBuffer.wrap(bytes));
            return decoded.toString();
        } catch (CharacterCodingException e) {
            // Cannot decode, return raw bytes
            return bytes;
        }
    }

    private static String readDate6(MySQLMessage mm) {
        int year = mm.readUB2();
        int month = mm.read();
        int day = mm.read();
        int hour = mm.read();
        int minute = mm.read();
        int second = mm.read();

        StringBuilder sb = new StringBuilder();
        sb.append(year).append('-').append(month).append('-').append(day).append(' ');
        sb.append(hour).append(':').append(minute).append(':').append(second);

        return sb.toString();
    }

    private static String readDate3(MySQLMessage mm) {
        int year = mm.readUB2();
        int month = mm.read();
        int day = mm.read();

        StringBuilder sb = new StringBuilder();
        sb.append(year).append('-').append(month).append('-').append(day);

        return sb.toString();
    }

    private static String readTimeStamp(MySQLMessage mm) {
        int year = mm.readUB2();
        int month = mm.read();
        int day = mm.read();
        int hour = mm.read();
        int minute = mm.read();
        int second = mm.read();
        int millisecond = mm.readInt();

        StringBuilder sb = new StringBuilder();
        sb.append(year).append('-').append(month).append('-').append(day).append(' ');
        sb.append(hour).append(':').append(minute).append(':').append(second).append('.');
        sb.append(millisecond);

        return sb.toString();
    }

    private static String readTime3(MySQLMessage mm) {
        int sign = mm.read();
        int day = mm.readInt();
        int hour = mm.read();
        int minute = mm.read();
        int second = mm.read();

        StringBuilder sb = new StringBuilder();
        if (sign == 1) {
            sb.append('-');
        }
        sb.append(day).append(' ');
        sb.append(hour).append(':').append(minute).append(':').append(second);

        return sb.toString();
    }

    private static String readTime4(MySQLMessage mm) {
        int sign = mm.read();
        int day = mm.readInt();
        int hour = mm.read();
        int minute = mm.read();
        int second = mm.read();
        int milliseconds = mm.readInt();

        StringBuilder sb = new StringBuilder();
        if (sign == 1) {
            sb.append('-');
        }
        sb.append(day).append(' ');
        sb.append(hour).append(':').append(minute).append(':').append(second).append('.');
        sb.append(milliseconds);

        return sb.toString();
    }

    // private static long byte8(byte[] v) {
    // // little endian
    // return (v[0] << 56) | (v[1] << 48) | (v[2] << 40) | (v[3] << 32) | (v[4]
    // << 24) | (v[5] << 16) | (v[6] << 8)
    // | (v[7] & 0xff);
    // }

    /**
     * Since incoming blob always being send by as separate Send_Data_long with
     * no array size so we just read as much as possible
     * http://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html this
     * should be only used inside send_long_data, and read only once, so no need
     * to adjust mm position
     */
    public static byte[] readBlobTillEnd(MySQLMessage mm) {
        ByteArrayInputStream bi =
            new ByteArrayInputStream(mm.bytes(), mm.position(), mm.bytes().length - mm.position());
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bo);
        int len = 0;
        try {
            while ((len = bi.available()) > 0) {
                while (len-- > 0) {
                    dos.writeByte(bi.read());
                }
            }
            return bo.toByteArray(); // for blog just consider it as byte[]
        } catch (IOException e) {
            return null;
        }
    }

    public static byte[] readBlob(MySQLMessage mm) {
        if (!mm.hasRemaining()) {
            return null;
        }
        int len = readLengthEncodedInteger(mm);
        if (len == 0) {
            return null;
        }
        int remain = mm.bytes().length - mm.position();

        len = len > remain ? remain : len;
        return mm.readBytes(len);
    }

}

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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTime;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.text.SimpleDateFormat;

public class RawSqlUtils {

    private static final Logger logger = LoggerFactory.getLogger(RawSqlUtils.class);

    private static final char[] HEX_CHARS =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static String formatParameter(Object value) {
        StringBuilder sb = new StringBuilder();
        formatParameter(value, sb);
        return sb.toString();
    }

    public static void formatParameter(Object object, StringBuilder sb) {
        if (object == null) {
            sb.append("NULL");
        } else if (object instanceof Number) {
            // Byte, Short, Integer, Long, Float, Double, BigDecimal
            sb.append(object.toString());
        } else if (object instanceof ZeroDate || object instanceof ZeroTime || object instanceof ZeroTimestamp) {
            sb.append("'").append(object.toString()).append("'");
        } else if (object instanceof java.sql.Time) {
            sb.append("'").append(new SimpleDateFormat("HH:mm:ss").format(object)).append("'");
        } else if (object instanceof java.sql.Date) {
            sb.append("'").append(new SimpleDateFormat("yyyy-MM-dd").format(object)).append("'");
        } else if (object instanceof java.util.Date) { // (includes java.sql.Timestamp)
            sb.append("'").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(object)).append("'");
        } else if (object instanceof String) {
            sb.append("'");
            escapeString((String) object, sb);
            sb.append("'");
        } else if (object instanceof Boolean) {
            sb.append(((Boolean) object) ? "1" : "0");
        } else if (object instanceof byte[]) {
            sb.append("X'");
            toHexString((byte[]) object, sb);
            sb.append("'");
        } else if (object instanceof TableName) {
            sb.append(((TableName) object).getTableName());
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_UNSUPPORTED,
                "Parameter type " + object.getClass().getSimpleName() + " is not supported");
        }
    }

    private static void escapeString(String in, StringBuilder out) {
        for (int i = 0, j = in.length(); i < j; i++) {
            char c = in.charAt(i);
            if (c == '\'') {
                out.append(c);
            }
            out.append(c);
        }
    }

    private static void toHexString(byte[] bytes, StringBuilder out) {
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            out.append(HEX_CHARS[v / 16]);
            out.append(HEX_CHARS[v % 16]);
        }
    }
}

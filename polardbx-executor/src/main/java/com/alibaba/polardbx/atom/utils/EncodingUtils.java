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

package com.alibaba.polardbx.atom.utils;

import com.mysql.jdbc.JDBC4Connection;

import java.sql.Connection;
import java.sql.SQLException;

public class EncodingUtils {

    public static String mysqlEncoding(String encoding) {
        if (encoding.equalsIgnoreCase("iso_8859_1")) {
            return "latin1";
        }

        if (encoding.equalsIgnoreCase("utf-8") || encoding.equalsIgnoreCase("utf8")) {
            return "utf8mb4";
        }

        return encoding;
    }

    public static String javaEncoding(String encoding) {
        if (encoding.equalsIgnoreCase("utf8mb4")) {
            return "utf8";
        } else if (encoding.equalsIgnoreCase("binary")) {
            return "iso_8859_1";
        }

        return encoding;
    }

    public static void setEncoding(Connection conn, String encoding) throws SQLException {
        JDBC4Connection connInner = conn.unwrap(JDBC4Connection.class);
        connInner.setEncoding(encoding);
    }

    public static String getEncoding(Connection conn) throws SQLException {
        JDBC4Connection connInner = conn.unwrap(JDBC4Connection.class);
        return connInner.getEncoding();
    }

}

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

import java.sql.SQLException;
import java.sql.Statement;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.utils.TStringUtil;

public final class SavePoint {

    public static void set(IConnection conn, String name) throws SQLException {
        StringBuilder savePointQuery = new StringBuilder("SAVEPOINT ");
        savePointQuery.append('`');
        savePointQuery.append(TStringUtil.escape(name, '`', '`'));
        savePointQuery.append('`');

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(savePointQuery.toString());
        }
    }

    public static void setLater(IConnection conn, String name) throws SQLException {
        StringBuilder savePointQuery = new StringBuilder("SAVEPOINT ");
        savePointQuery.append('`');
        savePointQuery.append(TStringUtil.escape(name, '`', '`'));
        savePointQuery.append('`');

        conn.executeLater(savePointQuery.toString());
    }

    public static void rollback(IConnection conn, String name) throws SQLException {
        StringBuilder savePointQuery = new StringBuilder("ROLLBACK TO SAVEPOINT ");
        savePointQuery.append('`');
        savePointQuery.append(TStringUtil.escape(name, '`', '`'));
        savePointQuery.append('`');

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(savePointQuery.toString());
        }
    }

    public static void release(IConnection conn, String name) throws SQLException {
        StringBuilder savePointQuery = new StringBuilder("RELEASE SAVEPOINT ");
        savePointQuery.append('`');
        savePointQuery.append(TStringUtil.escape(name, '`', '`'));
        savePointQuery.append('`');

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(savePointQuery.toString());
        }
    }
}

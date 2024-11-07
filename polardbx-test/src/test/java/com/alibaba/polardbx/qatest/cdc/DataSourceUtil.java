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

package com.alibaba.polardbx.qatest.cdc;

import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by ziyang.lb
 **/
public class DataSourceUtil {
    private final static int QUERY_TIMEOUT = 7200;

    public static void closeQuery(ResultSet rs, Statement stmt, Connection conn) {
        JdbcUtil.close(rs);
        JdbcUtil.close(stmt);
        JdbcUtil.closeConnection(conn);
    }

    public static ResultSet query(Connection conn, String sql, int fetchSize)
        throws SQLException {
        return query(conn, sql, fetchSize, QUERY_TIMEOUT);
    }

    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
        throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        return query(stmt, sql);
    }

    public static ResultSet query(Statement stmt, String sql) throws SQLException {
        return stmt.executeQuery(sql);
    }
}


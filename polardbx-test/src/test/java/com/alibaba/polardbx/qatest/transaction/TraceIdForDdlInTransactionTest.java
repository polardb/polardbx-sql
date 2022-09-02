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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TraceIdForDdlInTransactionTest extends ReadBaseTestCase {

    final String tableName1 = "testTraceIdOfDmlAfterDdl1";
    final String tableName2 = "testTraceIdOfDmlAfterDdl2";

    @Test
    public void testTraceIdOfDmlAfterDdl() throws SQLException {

        // before
        try (Statement stmt = tddlConnection.createStatement()) {
            createTable(stmt, tableName1);
            dropTable(stmt, tableName2);
        }

        tddlConnection.setAutoCommit(false);
        try (Statement stmt = tddlConnection.createStatement()) {
            // 1. execute DML 1
            executeDml(stmt);
            final String traceIdDml1 = getTraceId(stmt);

            // 2. execute DDL
            createTable(stmt, tableName2);

            // 3. execute DML 2
            executeDml(stmt);
            final String traceIdDml2 = getTraceId(stmt);

            // DML 1 should be committed and its traceId is different from DML 2
            Assert.assertFalse("dml 1 has the same trace id with dml 2",
                traceIdDml1.equalsIgnoreCase(traceIdDml2));

            // 4. execute DML 3
            executeDml(stmt);
            final String traceIdDml3 = getTraceId(stmt);
            // DML 2 should has the same trace id with DML 3 since they are in the same transaction
            Assert.assertTrue("dml 2 has different trace id from dml 3",
                traceIdDml2.equalsIgnoreCase(traceIdDml3));

            // 5. execute several DDL
            dropTable(stmt, tableName2);
            createTable(stmt, tableName2);
            dropTable(stmt, tableName2);

            // 6. execute DML 4 and DML 5, they should have the same trace Id
            executeDml(stmt);
            final String traceIdDml4 = getTraceId(stmt);
            executeDml(stmt);
            final String traceIdDml5 = getTraceId(stmt);
            Assert.assertTrue("dml 4 has different trace id from dml 5",
                traceIdDml4.equalsIgnoreCase(traceIdDml5));

        } finally {
            tddlConnection.commit();
            tddlConnection.setAutoCommit(true);

            // after
            try (Statement stmt = tddlConnection.createStatement()) {
                dropTable(stmt, tableName1);
                dropTable(stmt, tableName2);
            }
        }

    }

    private void executeDml(Statement stmt) throws SQLException {
        stmt.execute("select * from " + tableName1 + " for update");
    }

    private void createTable(Statement stmt, String tableName) throws SQLException {
        stmt.execute("create table if not exists " + tableName + "(id int primary key)");
    }

    private void dropTable(Statement stmt, String tableName) throws SQLException {
        stmt.execute("drop table if exists " + tableName);
    }

    private String getTraceId(Statement stmt) throws SQLException {
        final String SHOW_PROCESSLIST = "show processlist";
        try (ResultSet rs = stmt.executeQuery(SHOW_PROCESSLIST)) {
            while (rs.next()) {
                final String info = rs.getString("INFO");
                if (StringUtils.equalsIgnoreCase(info, SHOW_PROCESSLIST)) {
                    final String traceId = rs.getString("TRACEID");
                    if (StringUtils.isNotBlank(traceId)) {
                        return traceId.split("-")[0];
                    }
                }
            }
        }
        return "";
    }
}

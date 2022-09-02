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

package com.alibaba.polardbx.qatest.ddl.auto.autoNewPartition;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

/**
 * @version 1.0
 */
public abstract class BaseAutoPartitionNewPartition extends AsyncDDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    public void dropTableWithGsi(String primary, List<String> indexNames) {
        final String finalPrimary = quoteSpecialName(primary);
        try (final Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + finalPrimary);
            for (String gsi : Optional.ofNullable(indexNames).orElse(ImmutableList.of())) {
                stmt.execute("DROP TABLE IF EXISTS " + quoteSpecialName(gsi));
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public String showFullCreateTable(Connection conn, String tbName) {
        String sql = "show full create table " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getString("Create Table");
        } catch (SQLException ignore) {
        } finally {
            JdbcUtil.close(rs);
        }
        return null;
    }

    public String getExplainResult(String sql) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain " + sql);
        try {
            return JdbcUtil.resultsStr(rs);
        } finally {
            JdbcUtil.close(rs);
        }

    }

    public void dropTableIfExists(String tableName) {
        dropTableIfExists(tddlConnection, tableName);
    }

}

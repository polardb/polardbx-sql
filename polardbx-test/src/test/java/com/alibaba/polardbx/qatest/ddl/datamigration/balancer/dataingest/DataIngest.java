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

package com.alibaba.polardbx.qatest.ddl.datamigration.balancer.dataingest;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author moyi
 * @since 2021/04
 */
public abstract class DataIngest {

    public static int MOD_DIVISOR = 1000_000_007;
    protected String table;
    protected Connection connection;

    /**
     * For consistency check
     */
    protected int idSum = 0;
    protected int kSum = 0;
    protected int rowCount = 0;

    public void setTable(String table) {
        this.table = table;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    protected String insertSql(String table) {
        return "insert into " + table + " (id, k) values (?, ?)";
    }

    protected void prepareK(PreparedStatement ps, int k) throws SQLException {
        kSum += k;
        kSum = kSum % MOD_DIVISOR;
        ps.setInt(2, k);
    }

    public void ingest(int dataRows) {
        final String sql = insertSql(table);
        try (PreparedStatement ps = JdbcUtil.preparedStatement(sql, connection)) {
            for (int i = 0; i < dataRows; i++) {
                int k = ThreadLocalRandom.current().nextInt(dataRows);
                prepareK(ps, k);
                generatePk(dataRows, ps, i);

                this.rowCount++;
                ps.addBatch();
                ps.clearParameters();
                if (i % 1000 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        } catch (SQLException e) {
            Assert.fail("ingest data into table " + table + " failed due to: " + e.getMessage());
        }
    }

    /**
     * Check consistency of data, after split/merge operations
     */
    public void checkConsistency() {
        checkRowCount();
        checkId();
        checkK();
    }

    private void checkRowCount() {
        String checkCount = "select count(*) from " + this.table;
        String result = JdbcUtil.executeQueryAndGetFirstStringResult(checkCount, connection);
        Assert.assertEquals(String.valueOf(this.rowCount), result);
    }

    protected void checkId() {
        String checkIdSum = String.format("select mod(sum(id), %d)    from %s", MOD_DIVISOR, this.table);
        String result = JdbcUtil.executeQueryAndGetFirstStringResult(checkIdSum, connection);
        Assert.assertEquals(String.valueOf(this.idSum), result);
    }

    protected void checkK() {
        String checkKSum = String.format("select mod(sum(k), %d)    from %s", MOD_DIVISOR, this.table);
        String result = JdbcUtil.executeQueryAndGetFirstStringResult(checkKSum, connection);
        Assert.assertEquals(String.valueOf(this.kSum), result);
    }

    public abstract void generatePk(int dataRows, PreparedStatement ps, int i) throws SQLException;
}

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

/**
 * @author moyi
 * @since 2021/04
 */
public class DataIngestForThreeColumns extends DataIngest {

    protected String insertSql(String table) {
        return "insert into " + table + " (id1, id2, k) values (?, ?, ?)";
    }

    public DataIngestForThreeColumns() {
    }

    public DataIngestForThreeColumns(String tableName, Connection conn) {
        this.table = tableName;
        this.connection = conn;
    }

    @Override
    protected void prepareK(PreparedStatement ps, int k) throws SQLException {
        this.kSum += k;
        this.kSum = this.kSum % MOD_DIVISOR;
        ps.setInt(3, k);
    }

    @Override
    public void generatePk(int dataRows, PreparedStatement ps, int i) throws SQLException {
        this.idSum += i;
        this.idSum = this.idSum % MOD_DIVISOR;
        ps.setLong(1, i);
        ps.setLong(2, i);
    }

    @Override
    public void checkId() {
        String checkIdSum = String.format("select mod(sum(id1), %d)    from %s", MOD_DIVISOR, this.table);
        String result = JdbcUtil.executeQueryAndGetFirstStringResult(checkIdSum, connection);
        Assert.assertEquals(String.valueOf(this.idSum), result);
    }

}

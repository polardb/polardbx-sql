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

package com.alibaba.polardbx.qatest.ddl.balancer.dataingest;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author moyi
 * @since 2021/04
 */
public class DataIngestForTimestamp extends DataIngest {

    private long baseTimestamp = 0;
    private long sumDeltaTimestamp = 0;

    @Override
    public void generatePk(int dataRows, PreparedStatement ps, int i) throws SQLException {
        if (baseTimestamp == 0) {
            baseTimestamp = System.currentTimeMillis();
        }
        long current = System.currentTimeMillis();
        Timestamp ts = new Timestamp(current);
        ps.setTimestamp(1, ts);

        this.sumDeltaTimestamp += current - baseTimestamp;
    }

    @Override
    protected void checkK() {
        final String sql = String.format("select sum(k-%d) from %s", this.baseTimestamp, this.table);

        String result = JdbcUtil.executeQueryAndGetFirstStringResult(sql, this.connection);
        Assert.assertEquals(String.valueOf(this.sumDeltaTimestamp), result);
    }
}

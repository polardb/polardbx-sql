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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author moyi
 * @since 2021/04
 */
public class DataIngestForDateTime extends DataIngest {

    @Override
    public void generatePk(int dataRows, PreparedStatement ps, int i) throws SQLException {
        int year = ThreadLocalRandom.current().nextInt(100) + 1900;
        int month = ThreadLocalRandom.current().nextInt(12) + 1;
        int day = ThreadLocalRandom.current().nextInt(28) + 1;
        LocalDate date = LocalDate.of(year, month, day);
        ps.setDate(1, Date.valueOf(date));
        this.idSum += year + month + day;
    }

    @Override
    protected void checkId() {
        final String sql = "select sum(year(id)) + sum(month(id)) + sum(day(id)) from " + this.table;

        String result = JdbcUtil.executeQueryAndGetFirstStringResult(sql, this.connection);
        Assert.assertEquals(String.valueOf(idSum), result);
    }
}

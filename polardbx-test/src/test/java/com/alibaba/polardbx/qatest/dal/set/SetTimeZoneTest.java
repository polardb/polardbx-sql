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

package com.alibaba.polardbx.qatest.dal.set;

import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.validator.DataOperator.setOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Created by chuanqin on 2019/9/17. Set session time zone to each one of
 * supported time zones at a time.
 */
public class SetTimeZoneTest extends DirectConnectionBaseTestCase {

    @Test
    public void setTimeZoneTest() {
        String originTimeZone = "SYSTEM";
        String getTzSql = "show variables like 'time_zone'";
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuery(getTzSql, tddlConnection);
            if (rs.next()) {
                originTimeZone = rs.getString(2);
            }

        } catch (SQLException e) {
            //ignore
        } finally {
            JdbcUtil.close(rs);
        }
        for (int i = -12; i < 14; i++) {
            String setTzSql = "set time_zone=\"" + i + ":00\"";
            if (i >= 0) {
                setTzSql = "set time_zone=\"+" + i + ":00\"";
            }
            setOnMysqlAndTddl(mysqlConnection, tddlConnection, setTzSql);
            String sql = "SELECT time_format(now(), '%H:%i')";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            setTzSql = "SET TIME_ZONE=\"" + originTimeZone + "\"";
            setOnMysqlAndTddl(mysqlConnection, tddlConnection, setTzSql);
        }
    }
}

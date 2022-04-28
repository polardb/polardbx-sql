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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;

import java.util.List;

/**
 * @version 1.0
 * @ClassName PolardbxSchedulerTest
 * @description
 * @Author
 */
public class PolardbxSchedulerTest extends MysqlTest {

    public void test_0() {
        String sql = "CREATE SCHEDULE FOR LOCAL_PARTITION ON `local_partition`.`t_order33` CRON '0 0 12 1/5 * ?' TIMEZONE '+00:00'";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(
            "CREATE SCHEDULE FOR LOCAL_PARTITION ON `local_partition`.`t_order33` CRON '0 0 12 1/5 * ?' TIMEZONE '+00:00'",
            SQLUtils.toMySqlString(result));
        Assert.assertEquals(
            "create schedule for local_partition on `local_partition`.`t_order33` cron '0 0 12 1/5 * ?' timezone '+00:00'",
            SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_1() {
        String sql = "CREATE SCHEDULE FOR LOCAL_PARTITION ON `local_partition`.`t_order33` CRON '0 0 12 1/5 * ?'";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(
            "CREATE SCHEDULE FOR LOCAL_PARTITION ON `local_partition`.`t_order33` CRON '0 0 12 1/5 * ?'",
            SQLUtils.toMySqlString(result));
        Assert.assertEquals(
            "create schedule for local_partition on `local_partition`.`t_order33` cron '0 0 12 1/5 * ?'",
            SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_2() {
        String sql = "DROP SCHEDULE 11111111111";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(
            "DROP SCHEDULE 11111111111",
            SQLUtils.toMySqlString(result));
        Assert.assertEquals(
            "drop schedule 11111111111",
            SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_3() {
        String sql = "CHECK TABLE t1 WITH LOCAL PARTITION";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(
            "CHECK TABLE t1 WITH LOCAL PARTITION",
            SQLUtils.toMySqlString(result));
        Assert.assertEquals(
            "check table t1 with local partition",
            SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

}

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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.util.Pair;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PartitionInfoEqualsTest extends BasePlannerTest {

    static final String dbName = "optest";

    public PartitionInfoEqualsTest() {
        super(dbName);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Test
    public void TestPartitionInfoEquals() {
        for (Pair<Boolean, Pair<String, String>> createTablePair : createTablePairs) {
            ec.setParams(new Parameters());
            ec.setSchemaName(appName);
            ec.setServerVariables(new HashMap<>());
            final MySqlCreateTableStatement stat1 =
                (MySqlCreateTableStatement) FastsqlUtils.parseSql(createTablePair.getValue().left).get(0);
            final MySqlCreateTableStatement stat2 =
                (MySqlCreateTableStatement) FastsqlUtils.parseSql(createTablePair.getValue().right).get(0);
            final TableMeta tm1 = new TableMetaParser().parse(stat1, ec);
            final SqlCreateTable sqlCreateTable1 = (SqlCreateTable) FastsqlParser
                .convertStatementToSqlNode(stat1, null, ec);
            buildLogicalCreateTable(dbName, tm1, sqlCreateTable1, stat1.getTableName(),
                PartitionTableType.PARTITION_TABLE, PlannerContext.fromExecutionContext(ec));
            final TableMeta tm2 = new TableMetaParser().parse(stat2, ec);
            final SqlCreateTable sqlCreateTable2 = (SqlCreateTable) FastsqlParser
                .convertStatementToSqlNode(stat2, null, ec);
            buildLogicalCreateTable(dbName, tm2, sqlCreateTable2, stat2.getTableName(),
                PartitionTableType.PARTITION_TABLE, PlannerContext.fromExecutionContext(ec));
            Assert.assertTrue(createTablePair.getKey() == (tm1.getPartitionInfo().equals(tm2.getPartitionInfo())),
                "unexpected tableGroup for " + tm1.getTableName() + " <->" + tm2.getTableName());
        }
    }

    @Test
    @Ignore
    public void testSql() {

    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }

    static List<Pair<Boolean, Pair<String, String>>> createTablePairs = Arrays.asList(
        new Pair<>(Boolean.TRUE, new Pair<>("create table t1(a int, b int) partition by hash(a) partitions 3",
            "create table t2(a int, c int) partition by hash(c) partitions 3")),
        new Pair<>(Boolean.FALSE, new Pair<>("create table t3(a int, b int) partition by hash(a) partitions 3",
            "create table t4(a int, c bigint) partition by hash(c) partitions 3")),
        new Pair<>(Boolean.TRUE, new Pair<>("create table t5(a int, b int) partition by key(a) partitions 3",
            "create table t6(a int, c int) partition by key(c) partitions 3")),
        new Pair<>(Boolean.FALSE, new Pair<>("create table t7(a int, b int) partition by key(a) partitions 3",
            "create table t8(a int, c varchar(2)) partition by key(c) partitions 3")),
        new Pair<>(Boolean.TRUE, new Pair<>(
            "create table t9(a datetime, b int) partition by range(year(a))(partition p1 values less than (10))",
            "create table t10(c datetime, b int) partition by range(year(c))(partition p1 values less than (10))")),
        new Pair<>(Boolean.FALSE, new Pair<>(
            "create table t11(a datetime, b int) partition by range(year(a))(partition p1 values less than (10))",
            "create table t12(c datetime, b int) partition by range(month(c))(partition p1 values less than (10))")),
        new Pair<>(Boolean.FALSE, new Pair<>(
            "create table t13(a datetime, b int) partition by range(year(a))(partition p1 values less than (10))",
            "create table t14(a int, b int) partition by range(a)(partition p1 values less than (10))")),
        new Pair<>(Boolean.FALSE, new Pair<>(
            "create table t15(a datetime, b varchar(20)) charset=utf8 partition by key(b) partitions 3",
            "create table t16(c datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3")),
        new Pair<>(Boolean.FALSE, new Pair<>(
            "create table t17(a datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3",
            "create table t18(c datetime, b varchar(20)) charset=gbk collate=gbk_chinese_ci partition by key(b) partitions 3")),
        new Pair<>(Boolean.TRUE, new Pair<>(
            "create table t19(a datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3",
            "create table t20(c datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3")),
        new Pair<>(Boolean.TRUE, new Pair<>(
            "create table t21(a datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3",
            "create table t22(c datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3")));
}


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

package com.alibaba.polardbx.qatest.ddl.sharding.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTruncateWithRecycleMarkTask.CDC_RECYCLE_HINTS;

/**
 * Created by ziyang.lb
 **/

public class CdcDdlRecordTest extends CdcBaseTest {

    private static final Logger logger = LoggerFactory.getLogger(CdcDdlRecordTest.class);
    private final static SQLParserFeature[] FEATURES = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
    };
    private final static String DB_NAME_PREFIX = "cdc_ddl_test_";
    private final static AtomicInteger DB_NAME_SUFFIX = new AtomicInteger(0);
    private final String dbName;
    private final String serverId;

    public CdcDdlRecordTest(String serverId) {
        this.dbName = DB_NAME_PREFIX + DB_NAME_SUFFIX.incrementAndGet();
        if (StringUtils.equals(serverId, "8989")) {
            this.serverId = serverId;
        } else {
            this.serverId = null;
        }
    }

    @Parameterized.Parameters
    public static List<String[]> getTestParameters() {
        return Arrays.asList(new String[][] {{"9999"}, {"8989"},});
    }

    @Test
    public void testCdcDdlRecord() throws SQLException, InterruptedException {
        String sql;
        String tokenHints;
        AtomicLong jobIdSeed = new AtomicLong(0);
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select database()");

            if (StringUtils.isNotBlank(serverId)) {
                sql = "set polardbx_server_id=" + serverId;
                executeSql(stmt, sql);
            }

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database if exists " + dbName;
            executeSql(stmt, sql);
            Thread.sleep(2000);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
            Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());

            tokenHints = buildTokenHints();
            sql = tokenHints + "create database " + dbName;
            executeSql(stmt, sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
            Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());

            sql = "use " + dbName;
            executeSql(stmt, sql);

            doDDl(stmt, jobIdSeed, "t_ddl_test_normal", 0);
            doDDl(stmt, jobIdSeed, "t_ddl_test_gsi", 1);
            doDDl(stmt, jobIdSeed, "t_ddl_test_broadcast", 2);
            doDDl(stmt, jobIdSeed, "t_ddl_test_without_primary", 3);
            doDDl(stmt, jobIdSeed, "t_ddl_test_single", 4);
            testRecycleBin(stmt);
            testDropManyTable(stmt);

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database " + dbName;
            executeSql(stmt, sql);
            stmt.execute("use __cdc__");
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
            Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        }
    }

    private void doDDl(Statement stmt, AtomicLong jobIdSeed,
                       String tableName, int testType) throws SQLException {
        String sql;
        String tokenHints;

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop table if exists %s ", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());

        // Test Step
        // 连续执行两次drop ... if exists ...，验证第二次也需要打标(和mysql行为保持一致)
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop table if exists %s ", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());

        // Test Step
        if (testType == 0) {
            sql = String.format(CREATE_T_DDL_TEST_TABLE, tableName);
        } else if (testType == 1) {
            sql = String.format(CREATE_T_DDL_TEST_TABLE_GSI, tableName, "g_i_tv", "g_i_ext");
        } else if (testType == 2) {
            sql = String.format(CREATE_T_DDL_TEST_TABLE_BROADCAST, tableName);
        } else if (testType == 3) {
            sql = String.format(CREATE_T_DDL_TEST_TABLE_WITHOUT_PRIMARY, tableName);
        } else if (testType == 4) {
            sql = String.format(CREATE_T_DDL_TEST_TABLE_SINGLE, tableName);
        } else if (testType == 5) {
            sql = String.format(CREATE_T_DDL_TEST_TABLE_CLUSTER_GSI, tableName, "g_i_tv", "g_i_ext");
        }
        tokenHints = buildTokenHints();
        sql = tokenHints + sql;
        executeSql(stmt, sql);
        //打标的建表语句和传入的建表语句并不完全一样，此处只演示是否是create语句
        Assert.assertTrue(StringUtils.startsWith(getDdlRecordSql(tokenHints), tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());

        // Test Step
        doDml(jobIdSeed, tableName, 10);

        //--------------------------------------------------------------------------------
        //-----------------------------------Test Columns---------------------------------
        //--------------------------------------------------------------------------------
        // Test Step
        tokenHints = buildTokenHints();
        sql =
            tokenHints + String.format("alter table %s add column add1 varchar(20) not null default '111'", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        // Test Step
        tokenHints = buildTokenHints();
        sql =
            tokenHints + String
                .format("alter table %s add column add2 varchar(20) not null default '222' after job_id", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        // Test Step
        // optimize error by Do not support mix ADD COLUMN with other ALTER statements when table contains CLUSTERED INDEX

        if (testType != 5) {
            tokenHints = buildTokenHints();
            sql = tokenHints + String
                .format("alter table %s add column add3 bigint default 0,drop column add2", tableName);
            executeSql(stmt, sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
            Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
            doDml(jobIdSeed, tableName, 10);
        }

        // Test Step
        if (testType != 5) {
            tokenHints = buildTokenHints();
            sql =
                tokenHints + String.format("alter table %s modify add1 varchar(50) not null default '111'", tableName);
            executeSql(stmt, sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
            Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
            doDml(jobIdSeed, tableName, 10);
        }

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String
            .format("alter table %s change column add1 add111 varchar(50) not null default '111'", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop column add111", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop column add3", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        //--------------------------------------------------------------------------------
        //-------------------------------Test Local Indexes-------------------------------
        //--------------------------------------------------------------------------------
        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add index idx_test(`table_name`)", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add unique idx_job(`job_id`)", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("create index idx_gmt on %s(`gmt_created`)", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        // Test Step
        // 对于含有聚簇索引的表，引擎不支持一个语句里drop两个index，所以一个语句包含两个drop的sql就不用测试了
        // 否则会报错：optimize error by Do not support multi ALTER statements on table with clustered index
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop index idx_test", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop index idx_gmt on %s", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
        doDml(jobIdSeed, tableName, 10);

        //--------------------------------------------------------------------------------
        //--------------------------------------Test Gsi----------------------------------
        //--------------------------------------------------------------------------------
        // single表和broadcast表，不支持gsi，无需测试
        if (testType != 2 && testType != 4) {
            // Test Step
            tokenHints = buildTokenHints();
            sql =
                tokenHints + String
                    .format("CREATE GLOBAL INDEX g_i_test ON %s (`EXT_ID`) DBPARTITION BY HASH(`EXT_ID`)", tableName);
            executeSql(stmt, sql);
            Assert.assertEquals("", getDdlRecordSql(tokenHints));//GSI类型，不进行打标
            Assert.assertEquals(0, getDdlRecordSqlCount(tokenHints));
            doDml(jobIdSeed, tableName, 10);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + "CHECK GLOBAL INDEX g_i_test";
            executeSql(stmt, sql);
            Assert.assertEquals("", getDdlRecordSql(tokenHints));//GSI类型，不进行打标
            Assert.assertEquals(0, getDdlRecordSqlCount(tokenHints));
            doDml(jobIdSeed, tableName, 10);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("alter table %s add GLOBAL INDEX g_i_test11 ON %s (`JOB_ID`) COVERING "
                + "(`GMT_CREATED`) DBPARTITION BY HASH(`JOB_ID`)", tableName, tableName);
            //+ "add column add1 varchar(20) not null default '111'";//gsi不支持混合模式，省事儿了，不用测了
            executeSql(stmt, sql);
            Assert.assertEquals("", getDdlRecordSql(tokenHints));
            Assert.assertEquals(0, getDdlRecordSqlCount(tokenHints));
            doDml(jobIdSeed, tableName, 10);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("drop index g_i_test on %s", tableName);
            executeSql(stmt, sql);
            Assert.assertEquals("", getDdlRecordSql(tokenHints));//GSI类型，不进行打标
            Assert.assertEquals(0, getDdlRecordSqlCount(tokenHints));
            doDml(jobIdSeed, tableName, 10);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("alter table %s drop index g_i_test11", tableName);
            executeSql(stmt, sql);
            Assert.assertEquals("", getDdlRecordSql(tokenHints));//GSI类型，不进行打标
            Assert.assertEquals(0, getDdlRecordSqlCount(tokenHints));
            doDml(jobIdSeed, tableName, 10);
        }

        //--------------------------------------------------------------------------------
        //------------------------------------Test 拆分键变更-------------------------------
        //--------------------------------------------------------------------------------
        // 含有GSI的表、single表、broadcast表，不支持拆分键变更，ScaleOut/In，Rename，Truncate
        if (testType != 1 && testType != 2 && testType != 4 && testType != 5) {
            // Test Step
            // 如果是带有GSI的表，会报错：Table 't_ddl_test' is global secondary index table, which is forbidden to be modified.
            tokenHints = buildTokenHints();
            sql = tokenHints + String
                .format("alter table %s dbpartition by hash(ID) tbpartition by hash(ID) tbpartitions 8", tableName);
            executeSql(stmt, sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
            Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
            doDml(jobIdSeed, tableName, 10);
        }

        //--------------------------------------------------------------------------------
        //------------------------------------Test Truncate ------------------------------
        //--------------------------------------------------------------------------------
        // Test Step
        // 如果是带有GSI的表，会报错: Does not support truncate table with global secondary index，so use t_ddl_test_zzz for test
        if (testType != 1) {
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("truncate table %s", tableName);
            executeSql(stmt, sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
            Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
            doDml(jobIdSeed, tableName, 10);
        }

        //--------------------------------------------------------------------------------
        //------------------------------------Test rename --------------------------------
        //--------------------------------------------------------------------------------
        // Test Step
        // 如果是带有GSI的表，会报错: Does not support modify primary table 't_ddl_test' cause global secondary index exists,so use t_ddl_test_yyy for test
        if (testType != 1) {
            tokenHints = buildTokenHints();
            String newTableName = tableName + "_new";
            sql = tokenHints + String.format("rename table %s to %s", tableName, newTableName);
            executeSql(stmt, sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
            Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
            Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
            doDml(jobIdSeed, newTableName, 10);
            tableName = newTableName;
        }

        // Test Step
        String tmpTableName = tableName + "_tmp_test_" + System.currentTimeMillis();
        sql = String.format("create table %s like %s", tmpTableName, tableName);
        executeSql(stmt, sql);
        String markSql = getDdlRecordSqlByTableName(tmpTableName);
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(markSql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement sqlStatement = statementList.get(0);
        MySqlCreateTableStatement createTableStatement = (MySqlCreateTableStatement) sqlStatement;
        Assert.assertEquals(tmpTableName, SQLUtils.normalize(createTableStatement.getTableName()));

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop table %s", tableName);
        executeSql(stmt, sql);
        Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));
        Assert.assertEquals(serverId, getDdlExtInfo(tokenHints).getServerId());
    }

    private void testRecycleBin(Statement stmt) throws SQLException, InterruptedException {
        String tokenHints = buildTokenHints();
        String sql = tokenHints + "CREATE TABLE `t_recycle_test_1` (\n"
            + " `id` int(11) NOT NULL,\n"
            + " `balance` int(11) NOT NULL,\n"
            + " `name` varchar(100) not null,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ")  ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`name`) tbpartition by hash(`name`) tbpartitions 1";
        executeSql(stmt, sql);
        Assert.assertTrue(StringUtils.startsWith(getDdlRecordSql(tokenHints), tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));

        Timestamp lastTs = getLastDdlRecordTimestamp(dbName);
        sql = "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table t_recycle_test_1";
        executeSql(stmt, sql);
        List<Map<String, String>> recordSqls = getDdlRecordListNewerThan(dbName, lastTs);
        Assert.assertTrue(StringUtils.startsWith(recordSqls.get(0).get("ddl_sql"), CDC_RECYCLE_HINTS));
        Assert.assertTrue(StringUtils.containsIgnoreCase(recordSqls.get(0).get("ddl_sql"), "rename"));
        Assert.assertEquals(1, recordSqls.size());

        tokenHints = buildTokenHints();
        sql = tokenHints + "CREATE TABLE `t_recycle_test_2` (\n"
            + " `id` int(11) NOT NULL,\n"
            + " `balance` int(11) NOT NULL,\n"
            + " `name` varchar(100) not null,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ")  ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`name`) tbpartition by hash(`name`) tbpartitions 1";
        executeSql(stmt, sql);
        Assert.assertTrue(StringUtils.startsWith(getDdlRecordSql(tokenHints), tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));

        lastTs = getLastDdlRecordTimestamp(dbName);
        // sleep一段时间,避免truncate产生的ddl record时间戳与建表ddl record相同
        TimeUnit.SECONDS.sleep(5);
        sql = "/!TDDL:ENABLE_RECYCLEBIN=true*/truncate t_recycle_test_2";
        executeSql(stmt, sql);
        recordSqls = getDdlRecordListNewerThan(dbName, lastTs);
        Assert.assertEquals(3, recordSqls.size());
        Assert.assertTrue(StringUtils.startsWith(recordSqls.get(0).get("ddl_sql"), CDC_RECYCLE_HINTS));
        Assert.assertTrue(StringUtils.startsWith(recordSqls.get(1).get("ddl_sql"), CDC_RECYCLE_HINTS));
        Assert.assertTrue(StringUtils.startsWith(recordSqls.get(2).get("ddl_sql"), CDC_RECYCLE_HINTS));
        Assert.assertTrue(StringUtils.containsIgnoreCase(recordSqls.get(0).get("ddl_sql"), "create table"));
        Assert.assertTrue(StringUtils.containsIgnoreCase(recordSqls.get(1).get("ddl_sql"), "rename"));
        Assert.assertTrue(StringUtils.containsIgnoreCase(recordSqls.get(2).get("ddl_sql"), "rename"));
        Assert.assertEquals(JSONObject.parseObject(recordSqls.get(0).get("ext"), DDLExtInfo.class).getServerId(),
            serverId);
        Assert.assertEquals(JSONObject.parseObject(recordSqls.get(1).get("ext"), DDLExtInfo.class).getServerId(),
            serverId);
        Assert.assertEquals(JSONObject.parseObject(recordSqls.get(2).get("ext"), DDLExtInfo.class).getServerId(),
            serverId);
    }

    private void testDropManyTable(Statement stmt) throws SQLException {
        try {
            stmt.execute("create table test_drop_1(id int)");
            stmt.execute("create table test_drop_2(id int)");
            stmt.execute("create table test_drop_3(id int)");
            Timestamp lastTs = getLastDdlRecordTimestamp(dbName);
            stmt.execute("drop table test_drop_1,test_drop_2,test_drop_3");
            throw new TddlNestableRuntimeException("dro many table should received error,but not");
        } catch (Throwable e) {
            // igonre
        }
    }

    private void executeSql(Statement stmt, String sql) throws SQLException {
        logger.info("execute sql : " + sql);
        stmt.execute(sql);
    }

}

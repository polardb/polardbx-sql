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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group1;

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.Litmus;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author chenmo.cm
 */
public class AlterTableWithGsiTest extends AsyncDDLBaseNewDBTestCase {

    private static final String createOption = " if not exists ";
    private static final String baseGsiTableName = "gsi_base_table";
    private String tableName = "chenhui";
    private String indexTableName = "g_i_chenhui";
    private boolean supportXA = false;
    private static final String ALLOW_ALTER_GSI_INDIRECTLY_HINT =
        "/*+TDDL:cmd_extra(ALLOW_ALTER_GSI_INDIRECTLY=true)*/";

    // private static final String createOption = " ";

    @Before
    public void init() throws SQLException {

        /**
         * * diamond上完全一致(中间结果可能被覆盖)，所以这里不适合检查DiamondNotExist了 所以这里还是加上了if not
         * exists;
         */
        // initConnections();
        // diamond规则中不存在该表
        // assertDiamondNotExist(tableName);

        dropTableWithGsi(baseGsiTableName, ImmutableList.of("g_i_test_base"));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            HINT_CREATE_GSI + "CREATE TABLE IF NOT EXISTS`" + baseGsiTableName + "` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT, \n" + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n" + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`), \n"
                + "    GLOBAL UNIQUE g_i_test_base(`buyer_id`) COVERING (`order_snapshot`) DBPARTITION BY HASH "
                + "(`buyer_id`) \n" + ") ENGINE = InnoDB CHARSET = utf8mb4 dbpartition by hash(`order_id`);\n");

        supportXA = JdbcUtil.supportXA(tddlConnection);
    }

    @After
    public void clean() {

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + baseGsiTableName);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableOneGroupOneAtomWithoutRule() {
        String mytable = tableName;
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + " %s(a int,b char)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column ccc varchar(30)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(ccc) values('chenhui test')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableOneGroupOneAtomWithRule() {

        String mytable = tableName + "_2";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s(a int,b char)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column ccc varchar(30)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(ccc) values('chenhui test')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtom() {
        String mytable = tableName + "_3";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption
            + "%s(a int,b char) dbpartition by hash(a) dbpartitions 2", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column ccc varchar(30)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(a, ccc) values(100, 'chenhui test')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupMultiAtom() {

        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s (id int,name varchar(30),primary key(id)) "
                + "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column ccc varchar(30)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(id,ccc) values(100,'chenhui test')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableOneGroupOneAtomWithoutRuleDropColumn() {

        String mytable = tableName;
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + " %s(a int,b char)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column a", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(a) values(100)", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableOneGroupOneAtomWithRuleDropColumn() {
        String mytable = tableName + "_2";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s(a int,b char)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column a", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(a) values(100)", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomDropColumn() {

        String mytable = tableName + "_3";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption
            + "%s(a int,b char) dbpartition by hash(a)  dbpartitions 2", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column b", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(a,b) values(100,'x')", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupMultiAtomDropColumn() {
        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s (id int,name varchar(30),primary key(id)) "
                + "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column name", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(id,name) values(100,'chenhui test')", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomDropShardColumn() {

        String mytable = tableName + "_3";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption
            + "%s(a int,b char) dbpartition by hash(a)  dbpartitions 2", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column a", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupMultiAtomDropShardColumn() {

        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s (id int,name varchar(30),primary key(id)) "
                + "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column id", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableOneGroupOneAtomWithoutRuleRenameColumn() {

        String mytable = tableName;
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + " %s(a int,b char)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column b c char", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(c) values('x')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableOneGroupOneAtomWithRuleRenameColumn() {
        String mytable = tableName + "_2";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s(a int,b char)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column b c char", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(c) values('x')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomRenameColumn() {
        String mytable = tableName + "_3";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption
            + "%s(a int,b char) dbpartition by hash(a)  dbpartitions 2", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column b c char", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(a,c) values(100,'x')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupMultiAtomRenameColumn() {
        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s (id int,name varchar(30),primary key(id)) "
                + "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column name user varchar(30)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(id,user) values(100,'chenhui')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomRenameShardColumn() {

        String mytable = tableName + "_3";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption
            + "%s(a int,b char) dbpartition by hash(a)  dbpartitions 2", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column a c int", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupMultiAtomRenameShardColumn() {
        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format("create table "
                + createOption
                + "%s (id int,name varchar(30),primary key(id)) "
                + "dbpartition by hash(name) tbpartition by hash(name) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column id user_id int", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(user_id,name) values(100,'chenhui')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testRenameTableName() {

        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        dropTableIfExists(mytable + "_bak");
        String sql = String.format("create table " + createOption + "%s (id int,name varchar(30),primary key(id)) "
                + "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        //
        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s rename to %s_bak", mytable, mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 老的表名应该报错
        sql = String.format("insert into %s(id,name) values(100,'chenhui')", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        // 新表名应该成功
        sql = String.format("insert into %s_bak(id,name) values(100,'chenhui')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        dropTableIfExists(mytable + "_bak");
        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableOneGroupOneAtomWithRuleModifyColumnType() {

        String mytable = tableName + "_2";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s(a int,b char)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b varchar(100)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(b) values('chenhui test modify column type')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomModifyColumnType() {

        String mytable = tableName + "_3";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption
            + "%s(a int,b char) dbpartition by hash(a)  dbpartitions 2", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b varchar(100)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(a,b) values(100,'chenhui test modify column type')", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupMultiAtomModifyColumnType() {
        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s (id int,name varchar(30),primary key(id)) "
                + "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column name int", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(id,name) values(100,100)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomModifyShardColumnType() {
        String mytable = tableName + "_3";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption
            + "%s(a int,b char) dbpartition by hash(a)  dbpartitions 2", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column a char", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupMultiAtomAlterShardColumnType() {

        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s (id int,name varchar(30),primary key(id)) "
                + "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column id  varchar(100)", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testAlterBroadCastTable() {

        String mytable = tableName + "_6";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption
            + "%s (id int,name varchar(30),primary key(id)) broadcast", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column partment  varchar(100)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

//        assertThat(getExplainNum(sql)).isEqualTo(getNodeNum(tddlConnection));
        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableAddDefaultNow() {

        String mytable = tableName;
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + " %s(a int,b char)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "ALTER TABLE %s ADD UPDATE_TIME TIMESTAMP not null DEFAULT now() ON UPDATE CURRENT_TIMESTAMP COMMENT '最近更新时间';",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSequence() {
        String mytable = "sequence";
        String sql = String.format(
            "ALTER TABLE %s ADD UPDATE_TIME TIMESTAMP not null DEFAULT now() ON UPDATE CURRENT_TIMESTAMP COMMENT '最近更新时间';",
            mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSequenceOpt() {
        String mytable = "sequence_opt";
        String sql = String.format(
            "ALTER TABLE %s ADD UPDATE_TIME TIMESTAMP not null DEFAULT now() ON UPDATE CURRENT_TIMESTAMP COMMENT '最近更新时间';",
            mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testDropSequence() {
        String mytable = "sequence";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testDropSequenceOpt() {
        String mytable = "sequence_opt";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testDropSequenceTemp() {
        String mytable = "sequence_temp";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testDropDbLock() {
        String mytable = "__DRDS__SYSTEM__LOCK__";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testDropTxcUndoLog() {
        String mytable = "txc_undo_log";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    /**
     * @since 5.1.24
     */
    @Ignore("涉及到增删改AutoIncrement列,暂时不支持不测试")
    @Test
    public void testAlterSeqAutoIncrement() throws Exception {
        String tableName = "alter_table_test_1";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (auto_id int not null, id int , name varchar(20)) dbpartition by hash(id)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String insert_sql = String.format("insert into %s (id, name) values (1, 'abc')", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, insert_sql, "");

        sql = String.format("alter table %s modify column auto_id int not null auto_increment primary key", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
            "auto_id",
            tddlConnection).get(0)).isEqualTo(1L);

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqDeleteAutoIncrement() throws Exception {
        String tableName = "alter_table_test_2";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (auto_id int not null auto_increment primary key, id int , name varchar(20)) dbpartition by hash(id)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String insert_sql = String.format("insert into %s (id, name) values (1, 'abc')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);
        assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
            "auto_id",
            tddlConnection).get(0)).isEqualTo(100001L);

        sql = String.format("alter table %s modify column auto_id int not null", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateFailed(tddlConnection, insert_sql, "");

        assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
            "auto_id",
            tddlConnection).get(0)).isEqualTo(100001L);
        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.24
     */
    @Ignore("Mysql不支持")
    @Test
    public void testAlterSeqAutoIncrementType() throws Exception {
        String tableName = "alter_table_test_3";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (auto_id int not null auto_increment primary key, id int , name varchar(20)) dbpartition by hash(id)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String insert_sql = String.format("insert into %s (id, name) values (1, 'abc')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
            "auto_id",
            tddlConnection).get(0)).isEqualTo(1L);

        sql = String.format("alter table %s modify column auto_id bigint not null primary key auto_increment",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
            "auto_id",
            tddlConnection)).containsExactly(1L, 2L);
        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqAutoIncrementStartWith() throws Exception {
        String tableName = "alter_table_test_4";
        dropTableIfExists(tableName);

        String sql = String.format(
            "create table %s (auto_id int not null auto_increment primary key, id int , name varchar(20)) dbpartition by hash(id)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String insert_sql = String.format("insert into %s (id, name) values (1, 'abc')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
            "auto_id",
            tddlConnection).get(0)).isEqualTo(100001L);

        sql = String.format("alter table %s auto_increment = 40", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
            "auto_id",
            tddlConnection)).containsExactly(100001L, 200001L);

        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqAutoIncrementStartWith2() throws Exception {
        String tableName = "alter_table_test_4";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (auto_id int not null auto_increment primary key, id int , name varchar(20)) dbpartition by hash(id)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s auto_increment = 40", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String insert_sql = String.format("insert into %s (id, name) values (1, 'abc')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
            "auto_id",
            tddlConnection).get(0)).isEqualTo(200001L);

        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi() {

        String mytable = tableName + "_3";
        dropTableIfExists(mytable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b char, global index g_i_b(b) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column ccc varchar(30)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, ccc) values(100, 'b', 'chenhui test')", mytable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupMultiAtomWithGsi() {
        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s (id int,name varchar(30),primary key(id),"
                + " global index g_i_b(name) dbpartition by hash(name) tbpartition by hash(name) tbpartitions 3) "
                + "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 5 dbpartitions 2",
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column ccc varchar(30)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(id, name,ccc) values(100, 'mocheng test','chenhui test')", mytable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        dropTableIfExists(mytable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_error_drop_column() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30), f varchar(30)"
                + ", unique index u_i_c(c)"
                + ", unique index u_i_d_e(d, e)"
                + ", global unique %s(b, f) covering(c, d) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column a", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support drop column included in primary key of table which has global secondary index");

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column b", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support drop sharding key of global secondary index");

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column c", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column d", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support drop column included in unique key of table which has global secondary index");

        sql = String.format("alter table %s drop column e", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column f, drop column c",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support drop column included in unique key of global secondary index");

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_error_modify_column() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column a bigint(10) primary key", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support change sharding key column type on drds mode database ");

        sql = String.format("alter table %s modify column b varchar(40)", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support change sharding key column type on drds mode database");

        final String looseHint = "/*+TDDL:cmd_extra(ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI=true)*/";

        sql = String.format(looseHint + "alter table %s modify column c varchar(40)", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(looseHint + "alter table %s modify column d varchar(40), modify column c varchar(40)",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "do not support multi alter statements on table with global secondary index");

        dropTableIfExists(primaryTable);
    }

    @Ignore
    @BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
    public void testAlterTableMultiGroupOneAtomWithGsi_error_drop_column_with_uppercase() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(A int primary key,B varchar(30), C varchar(30), D varchar(30), E varchar(30), F varchar(30)"
                + ", unique index u_i_c(C)"
                + ", unique index u_i_d_e(D, E)"
                + ", global unique %s(B, F) covering(C, D) dbpartition by hash(B)) dbpartition by hash(A)",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column a", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support drop column included in primary key of table which has global secondary index");

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column b", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support drop sharding key of global secondary index");

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column c", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column d", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support drop column included in unique key of table which has global secondary index");

        sql = String.format("alter table %s drop column e", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column f, drop column c",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support drop column included in unique key of global secondary index");

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_modify_column_hint() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableWithGsi(primaryTable, ImmutableList.of(indexTable));
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30) NOT NULL, c varchar(30) NOT NULL, d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            String.format(HINT_ALLOW_ALTER_GSI_INDIRECTLY + "alter table %s modify column c varchar(40)", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            HINT_ALLOW_ALTER_GSI_INDIRECTLY + "alter table %s modify column d varchar(40), modify column c varchar(40)",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support multi ALTER statements on table with global secondary index");

        dropTableIfExists(primaryTable);
    }

    protected static TableChecker getTableChecker(Connection conn, String tableName) {
        final String sql = String.format("show create table %s", tableName);
        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(conn, sql)) {
            Assert.assertTrue(resultSet.next());
            String createTableStr = resultSet.getString(2);
            ;
            if (isMySQL80()) {
                createTableStr = createTableStr.replaceAll(" CHARACTER SET \\w+", "")
                    .replaceAll(" COLLATE \\w+", "")
                    .replaceAll(" DEFAULT COLLATE = \\w+", "")
                    .replaceAll(" int ", " int(11) ")
                    .replaceAll(" bigint ", " bigint(11) ")
                    .replaceAll(" int,", " int(11),")
                    .replaceAll(" bigint,", " bigint(11),")
                    .replaceAll(" int\n", " int(11)\n")
                    .replaceAll(" bigint\n", " bigint(11)\n");
            }
            return TableChecker.buildTableChecker(createTableStr);
        } catch (Exception e) {
            throw new RuntimeException("show create table failed!", e);
        }
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_error_change_column() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column a a1 bigint(10) primary key", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "optimize error by Do not support change the column name of sharding key");

        final String looseHint = "/*+TDDL:cmd_extra(ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI=true)*/";

        sql = String.format(looseHint + "alter table %s change column b b1 varchar(40)", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "optimize error by Do not support change the column name of sharding key");

        sql = String.format(looseHint + "alter table %s change column c c1 varchar(40)", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String
            .format(looseHint + "alter table %s change column d d1 varchar(40), change column c1 c2 varchar(40)",
                primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_change_column_hint() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableWithGsi(primaryTable, ImmutableList.of(indexTable));
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String
            .format(HINT_ALLOW_ALTER_GSI_INDIRECTLY + "alter table %s change column c c1 varchar(40)", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(HINT_ALLOW_ALTER_GSI_INDIRECTLY
                + "alter table %s change column d d1 varchar(40), change column c1 c varchar(40)",
            primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_error_alter_default() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s alter column a set default 123", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s alter column a drop default", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s alter column b set default 'bbb'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s alter column b drop default", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s alter column c set default 'ccc'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s alter column c drop default", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s alter column d set default 'ddd', alter column c set default 'ccc'",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "do not support multi alter statements on table with global secondary index");

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_alter_default_hint() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableWithGsi(primaryTable, ImmutableList.of(indexTable));
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String
            .format(HINT_ALLOW_ALTER_GSI_INDIRECTLY + "alter table %s alter column b set default 'bbb'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            String.format(HINT_ALLOW_ALTER_GSI_INDIRECTLY + "alter table %s alter column b drop default", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String
            .format(HINT_ALLOW_ALTER_GSI_INDIRECTLY + "alter table %s alter column c set default 'ccc'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(HINT_ALLOW_ALTER_GSI_INDIRECTLY
                + "alter table %s alter column d set default 'ddd', alter column c set default 'ccc'",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Do not support multi ALTER statements on table with global secondary index");

        final TableChecker tableChecker = getTableChecker(tddlConnection, indexTable);
        tableChecker.identicalTableDefinitionTo(
            "create table " + indexTable
                + "(a int(11) NOT NULL,b varchar(30), c varchar(30) default 'ccc', PRIMARY KEY(a))", true,
            Litmus.THROW);

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_alter_default() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s alter column d set default 'ddd'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, c) values(100, 'b', 'chenhui test')", primaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("select d from %s where a = 100 and b = 'b' and c = 'chenhui test'", primaryTable);

            try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("ddd", resultSet.getString(1));
            } catch (Exception e) {
                throw new RuntimeException("get insert result failed!", e);
            }
        }

        sql = String.format("alter table %s alter column d drop default", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_change_column() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column d d1 varchar(40) default 'ddd'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, c) values(100, 'b', 'chenhui test')", primaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("select d1 from %s where a = 100 and b = 'b' and c = 'chenhui test'", primaryTable);

            try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("ddd", resultSet.getString(1));
            } catch (Exception e) {
                throw new RuntimeException("get insert result failed!", e);
            }
        }

        JdbcUtil.executeQuery("analyze table " + primaryTable, tddlConnection);
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            String.format("SELECT COUNT(1) FROM %s where c = 'b' ", primaryTable), tddlConnection)) {
            MatcherAssert.assertThat(resultSet.next(), is(true));
        } catch (Exception e) {
            throw new RuntimeException("sharding advisor failed!", e);
        }
        sql = "/*+TDDL:cmd_extra(SHARDING_ADVISOR_BROADCAST_THRESHOLD=-1)*/shardingadvise";
        DataValidator.sqlMayErrorAssert(sql, tddlConnection, "ERR_TABLE_NOT_EXIST");

        sql = String.format("alter table %s change column d1 d varchar(30)", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_modify_column() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column d varchar(40) default 'ddd'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, c) values(100, 'b', 'chenhui test')", primaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("select d from %s where a = 100 and b = 'b' and c = 'chenhui test'", primaryTable);

            try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("ddd", resultSet.getString(1));
            } catch (Exception e) {
                throw new RuntimeException("get insert result failed!", e);
            }
        }

        sql = String.format("alter table %s modify column d varchar(30)", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_modify_column1() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30)"
                + ", unique index u_i_d(d)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column d varchar(40) default 'ddd'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column d varchar(30)", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_drop_column() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
                + ", unique index u_i_d(d)"
                + ", global index %s(b) covering(c) dbpartition by hash(b)) dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column e", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column e varchar(40) default 'eee'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, c, d) values(100, 'b', 'chenhui test', 'd')", primaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("select e from %s where a = 100 and b = 'b' and c = 'chenhui test' and d = 'd'",
                primaryTable);

            try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("eee", resultSet.getString(1));
            } catch (Exception e) {
                throw new RuntimeException("get insert result failed!", e);
            }
        }

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithoutGsi_drop_column() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI + "create table " + createOption
                + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
                + ", unique index u_i_d(d)" + ") dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column d", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column d varchar(40) default 'ddd'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, c, e) values(100, 'b', 'chenhui test', 'e')", primaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("select d from %s where a = 100 and b = 'b' and c = 'chenhui test' and e = 'e'",
                primaryTable);

            try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("ddd", resultSet.getString(1));
            } catch (Exception e) {
                throw new RuntimeException("get insert result failed!", e);
            }
        }

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithoutGsi_drop_column_1() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI + "create table " + createOption
            + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
            + ", unique index u_i_c(c)"
            + ", global index %s(b) covering(c) dbpartition by hash(b)"
            + ") dbpartition by hash(a) dbpartitions 2", primaryTable, indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column c", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column c varchar(40) default 'ccc'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, d, e) values(100, 'b', 'chenhui test', 'e')", primaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("select c from %s where a = 100 and b = 'b' and d = 'chenhui test' and e = 'e'",
                primaryTable);

            try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("ccc", resultSet.getString(1));
            } catch (Exception e) {
                throw new RuntimeException("get insert result failed!", e);
            }
        }

        final TableChecker indexTableChecker = getTableChecker(tddlConnection, indexTable);
        Assert.assertTrue(indexTableChecker.columnNotExists("c"));

        final TableChecker primaryTableChecker = getTableChecker(tddlConnection, primaryTable);
        Assert.assertTrue(primaryTableChecker.indexNotExists("u_i_c", false));
        Assert.assertTrue(primaryTableChecker.indexExists(indexTable, true));
        Assert.assertTrue(primaryTableChecker.columnNotExistsInGsi("c"));

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithoutGsi_drop_column_2() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI + "create table " + createOption
            + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
            + ", unique index u_i_c(c)"
            + ", global index %s(b, d) covering(c) dbpartition by hash(b)"
            + ") dbpartition by hash(a) dbpartitions 2", primaryTable, indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "alter table %s drop column d", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column d varchar(40) default 'ddd'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, c, e) values(100, 'b', 'chenhui test', 'e')", primaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("select d from %s where a = 100 and b = 'b' and c = 'chenhui test' and e = 'e'",
                primaryTable);

            try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("ddd", resultSet.getString(1));
            } catch (Exception e) {
                throw new RuntimeException("get insert result failed!", e);
            }
        }

        final TableChecker indexTableChecker = getTableChecker(tddlConnection, indexTable);
        Assert.assertTrue(indexTableChecker.columnNotExists("d"));

        final TableChecker primaryTableChecker = getTableChecker(tddlConnection, primaryTable);
        Assert.assertTrue(primaryTableChecker.indexExists("u_i_c", false));
        Assert.assertTrue(primaryTableChecker.indexExists(indexTable, true));
        Assert.assertTrue(primaryTableChecker.columnNotExistsInGsi("d"));
        Assert.assertTrue(primaryTableChecker.columnExists("d"));

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    @Ignore("does not support by fastsql")
    public void testAlterTableMultiGroupOneAtomWithGsi_order_by() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI + "create table " + createOption
            + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
            + ", unique index u_i_c(c)"
            + ", global index %s(b, d) covering(c) dbpartition by hash(b)"
            + ") dbpartition by hash(a) dbpartitions 2", primaryTable, indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s order by e, d, c, b, a", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = String.format("insert into %s(a, b, c, d, e) values(100, 'b', 'chenhui test', 'ddd', 'e')",
                primaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("select * from %s where a = 100 and b = 'b' and c = 'chenhui test' and e = 'e'",
                primaryTable);

            try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("e", resultSet.getString(1));
                Assert.assertEquals("ddd", resultSet.getString(2));
                Assert.assertEquals("chenhui test", resultSet.getString(3));
                Assert.assertEquals("b", resultSet.getString(4));
                Assert.assertEquals("100", resultSet.getString(5));
            } catch (Exception e) {
                throw new RuntimeException("get insert result failed!", e);
            }
        }

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_error_add_index() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI + "create table " + createOption
            + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
            + ", global index %s(b, d) covering(c) dbpartition by hash(b)"
            + ") dbpartition by hash(a) dbpartitions 2", primaryTable, indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add index(c) ", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        final TableChecker primaryTableChecker = getTableChecker(tddlConnection, primaryTable);
        Assert.assertTrue(primaryTableChecker.indexExists(ImmutableSet.of("c"), null, false));

        sql = String.format("alter table %s add index %s(c) ", primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicated index name " + indexTable);

        sql = String.format("alter table %s add unique index %s(c) ", primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicated index name " + indexTable);

        // FastSql does not support
        // sql = String.format("alter table %s add fulltext index %s(c) ",
        // primaryTable, indexTable);
        // JdbcUtil.executeUpdateFailed(polarDbXConnection, sql,
        // "Duplicated index name " + indexTable);

        sql = String.format("alter table %s add spatial index %s(c) ", primaryTable, indexTable);
        //JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicated index name " + indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicated index name " + indexTable);

        final String referenceTable = tableName + "reference";
        dropTableIfExists(referenceTable);
        sql = String.format(HINT_CREATE_GSI + "create table " + createOption
            + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30), key(d)"
            + ") dbpartition by hash(a) dbpartitions 2", referenceTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add foreign key %s(c) references %s(d)",
            primaryTable,
            indexTable,
            referenceTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicated index name " + indexTable);

//        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "do not support foreign key");
        dropTableIfExists(referenceTable);
        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_enable_keys() {
        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI + "create table " + createOption
            + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
            + ", unique index u_i_c(c)"
            + ", global index %s(b, d) covering(c) dbpartition by hash(b)"
            + ") dbpartition by hash(a) dbpartitions 2", primaryTable, indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s disable keys", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s enable keys", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_error_drop_primary_key() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI + "create table " + createOption
            + "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
            + ", unique index u_i_c(c)"
            + ", global index %s(b, d) covering(c) dbpartition by hash(b)"
            + ") dbpartition by hash(a) dbpartitions 2", primaryTable, indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop primary key", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            "Does not support drop primary key from table with global secondary index");

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithoutGsi_drop_primary_key() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI + "create table " + createOption
            + "%s(a int, b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
            + ", unique index u_i_c(c)" + ", primary key(a)"
            + ") dbpartition by hash(a) dbpartitions 2", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop primary key", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        final TableChecker tableChecker = getTableChecker(tddlConnection, primaryTable);
        Assert.assertTrue(tableChecker.indexNotExists(ImmutableSet.of("a"), null, false));

        dropTableIfExists(primaryTable);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testAlterTableMultiGroupOneAtomWithGsi_alter_table_options() {

        final String primaryTable = tableName + "_6";
        final String indexTable = indexTableName + "_6";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key auto_increment,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
                + ", unique index u_i_c(c)"
                + ", global index %s(b, d) covering(c) dbpartition by hash(b)"
                + ") dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s ENGINE = InnoDB", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s auto_increment = 100", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s comment 'comment test'", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s pack_keys = default", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // sql = String.format("alter table %s pack_keys = 0", primaryTable);
        // JdbcUtil.executeUpdateSuccess(polarDbXConnection, sql);

        sql = String.format("alter table %s row_format = DYNAMIC", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // final String referenceTable = tableName + "reference";
        // dropTableIfExists(referenceTable);
        // sql = String.format(HINT_CREATE_GSI + "create table " + createOption
        // +
        // "%s(a int primary key,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
        // + ") dbpartition by hash(a) dbpartitions 2", referenceTable);
        // JdbcUtil.executeUpdateSuccess(polarDbXConnection, sql);

        // sql = String.format("alter table %s union(%s)", primaryTable,
        // referenceTable);
        // JdbcUtil.executeUpdateSuccess(polarDbXConnection, sql);

        // dropTableIfExists(referenceTable);
        dropTableIfExists(primaryTable);
    }

    @Test
    public void testAlterTableConvertCharset() {

        final String primaryTable = tableName + "_7";
        final String indexTable = indexTableName + "_7";

        dropTableIfExists(primaryTable);
        String sql = String.format(HINT_CREATE_GSI
                + "create table "
                + createOption
                + "%s(a int primary key auto_increment,b varchar(30), c varchar(30), d varchar(30), e varchar(30)"
                + ", unique index u_i_c(c)"
                + ", global index %s(b, d) covering(c) dbpartition by hash(b)"
                + ") dbpartition by hash(a) dbpartitions 2",
            primaryTable,
            indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s convert to character set latin1", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(showCreateTable(tddlConnection, primaryTable).contains("latin1"));
        Assert.assertTrue(showCreateTable(tddlConnection, indexTable).contains("latin1"));

        sql = String.format("alter table %s convert to character set utf8 collate utf8_bin", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(showCreateTable(tddlConnection, primaryTable).contains("utf8"));
        String showIndexTable = showCreateTable(tddlConnection, indexTable);
        if (isMySQL80()) {
            showIndexTable = showIndexTable.replace("utf8mb3_bin", "utf8_bin");
        }
        System.out.println(showIndexTable);
        Assert.assertTrue(showIndexTable.contains("utf8_bin"));
        Assert.assertTrue(showCreateTable(tddlConnection, primaryTable).contains("utf8"));
        String showIndexTable1 = showCreateTable(tddlConnection, indexTable);
        if (isMySQL80()) {
            showIndexTable1 = showIndexTable1.replace("utf8mb3_bin", "utf8_bin");
        }
        System.out.println(showIndexTable1);
        Assert.assertTrue(showIndexTable1.contains("utf8_bin"));

        sql = String.format("alter table %s convert to character set utf8 collate utf8_general_cixx", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "unknown collate name 'utf8_general_cixx'");

        sql = String.format("alter table %s convert to character set utf8 collate LATIN1_BIN", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "collate name 'latin1_bin' not support for 'utf8'");

        sql = String.format("alter table %s convert to character set utf2", primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "unknown charset name 'utf2'");

        dropTableIfExists(primaryTable);
    }

    public String showCreateTable(Connection conn, String tbName) {
        String sql = "show create table " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getString("Create Table");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return null;
    }
}

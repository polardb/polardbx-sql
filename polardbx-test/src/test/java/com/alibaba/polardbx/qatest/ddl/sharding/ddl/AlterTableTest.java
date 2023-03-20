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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertThat;

@NotThreadSafe
public class AlterTableTest extends AsyncDDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(AlterTableTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";
    private static final String ALLOW_ALTER_GSI_INDIRECTLY_HINT =
        "/*+TDDL:cmd_extra(ALLOW_ALTER_GSI_INDIRECTLY=true)*/";

    public AlterTableTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}
        });
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + "chenhui";
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

    @Test
    public void testAlterShardKeyByHint() {
        String mytable = tableName + "_5";
        dropTableIfExists(mytable);
        String sql = String.format("create table " + createOption + "%s (id int,name varchar(30),primary key(id)) "
            + "dbpartition by hash(id) tbpartition by hash(id)", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column id int not null comment 'new comment'", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not supported", "can't modify shard column");

        sql = String.format(
            "/*TDDL:ENABLE_ALTER_SHARD_KEY=TRUE*/alter table %s modify column id int not null comment 'new comment'",
            mytable);
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
    @Ignore
    @Test
    public void testAlterSequence() {
        String mytable = schemaPrefix + "sequence";
        String sql = String.format(
            "ALTER TABLE %s ADD UPDATE_TIME TIMESTAMP not null DEFAULT now() ON UPDATE CURRENT_TIMESTAMP COMMENT '最近更新时间';",
            mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    /**
     * @since 5.1.24
     */
    @Ignore
    @Test
    public void testAlterSequenceOpt() {
        String mytable = schemaPrefix + "sequence_opt";
        String sql = String.format(
            "ALTER TABLE %s ADD UPDATE_TIME TIMESTAMP not null DEFAULT now() ON UPDATE CURRENT_TIMESTAMP COMMENT '最近更新时间';",
            mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Ignore
    @Test
    public void testDropSequence() {
        String mytable = schemaPrefix + "sequence";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Ignore
    @Test
    public void testDropSequenceOpt() {
        String mytable = schemaPrefix + "sequence_opt";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Ignore
    @Test
    public void testDropSequenceTemp() {
        String mytable = schemaPrefix + "sequence_temp";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testDropDbLock() {
        String mytable = schemaPrefix + "__DRDS__SYSTEM__LOCK__";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testDropTxcUndoLog() {
        String mytable = schemaPrefix + "txc_undo_log";
        String sql = String.format("DROP TABLE %s", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    /**
     * @since 5.1.24
     */
    @Ignore("涉及到增删改AutoIncrement列,暂时不支持不测试")
    @Test
    public void testAlterSeqAutoIncrement() throws Exception {
        String tableName = schemaPrefix + "alter_table_test_1";
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
        String tableName = schemaPrefix + "alter_table_test_2";
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
        String tableName = schemaPrefix + "alter_table_test_3";
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
        String tableName = schemaPrefix + "alter_table_test_4";
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
        String tableName = schemaPrefix + "alter_table_test_4";
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

    @Test
    public void testAlterSeqAutoIncrementStartWith3() {
        String tableName = schemaPrefix + "alter_table_test_no_seq";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (auto_id int not null primary key, id int , name varchar(20)) dbpartition by hash(id)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s auto_increment = 40", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(tableName);
    }

    @Test
    public void testAlterTableFollowedByInsertWhenAutoCommitFalse() throws Exception {
        String tableName = schemaPrefix + "ddl_followed_by_dml_when_autocommit_false";

        dropTableIfExists(tableName);

        tddlConnection.setAutoCommit(false);

        // Choose a transaction policy that supports both MySQL 5.6 and 5.7.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set drds_transaction_policy='2PC'");

        String sqlCreateTable = "create table " + tableName + "(c1 int primary key) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable);

        String sqlInsert = "insert into " + tableName + "(c1) values(1),(2),(3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlInsert);

        String sqlAlterTable = "alter table " + tableName + " add column c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlAlterTable);

        // The insert following Alter Table should succeed without failure
        // related to Async DDL and distribution transaction log.
        sqlInsert = "insert into " + tableName + "(c1) values(4),(5),(6)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlInsert);

        tddlConnection.setAutoCommit(true);

        dropTableIfExists(tableName);
    }

    @Test
    public void testAlterTableWithoutSeqChange() throws Exception {
        String tableName = schemaPrefix + "alter_table_without_seq_change";

        dropTableIfExists(tableName);

        String sqlCreateTable = "create table " + tableName
            + "(c1 int not null auto_increment, c2 int, primary key(c1)) dbpartition by hash(c2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable);

        String sqlAlterTable =
            "alter table " + tableName + " modify column c1 bigint unsigned not null auto_increment by group";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlAlterTable);

        dropTableIfExists(tableName);
    }

    @Test
    public void testAlterTableWithBothAddAndDropColumns() {
        String tableName = schemaPrefix + "alter_table_with_both_add_and_drop_columns";

        dropTableIfExists(tableName);

        String sql = String.format("create table " + createOption + "%s (c1 int, c2 int) "
            + "dbpartition by hash(c1) tbpartition by hash(c1) tbpartitions 2", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add column c3 varchar(10) add column c4 bigint", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop column c2 drop column c4", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String
            .format("alter table %s add column c2 char(10) drop column c3 add column c4 int add index idx(c1)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(tableName);
    }

    @Test
    @Ignore
    public void testAlterTableWithMysqlPartition() {
        String tableName = schemaPrefix + "alter_table_mysql_partition";
        dropTableIfExists(tableName);
        String sql = "CREATE TABLE if not exists " + tableName
            + "(col1 INT, col2 CHAR(5), col3 DATETIME)"
            + " dbpartition by hash(col1) dbpartitions 2 tbpartition by hash(col1) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "alter table " + tableName + " PARTITION BY HASH ( YEAR(col3) )";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support table with mysql partition");
    }

    @Test
    public void testAlterTableDropAddPrimaryKey() throws SQLException {
        if (TStringUtil.isNotEmpty(schemaPrefix)) {
            return;
        }

        String tableName = "biz_goods_child";

        dropTableIfExists(tableName);

        String sql = "create table if not exists %s (id bigint not null, child_order_id varchar(64),"
            + "goods_id varchar(64), order_id varchar(64), create_time datetime(3) DEFAULT CURRENT_TIMESTAMP(3),"
            + "key(order_id), key(create_time), primary key(child_order_id, goods_id))"
            + "dbpartition by hash(child_order_id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        String initialPrimaryKeyColumns = "child_order_id,goods_id";
        String currentPkColumns = fetchPrimaryKeyColumns(tableName);
        Assert.assertTrue(TStringUtil.equalsIgnoreCase(currentPkColumns, initialPrimaryKeyColumns));

        sql = "alter table %s change id id varchar(128) not null, drop primary key, add primary key (id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        String newPrimaryKeyColumn = "id";
        currentPkColumns = fetchPrimaryKeyColumns(tableName);
        Assert.assertTrue(TStringUtil.equalsIgnoreCase(currentPkColumns, newPrimaryKeyColumn));

        dropTableIfExists(tableName);
    }

    @Test
    public void testAlterTableDropAddCompPrimaryKey() {
        if (TStringUtil.isNotEmpty(schemaPrefix)) {
            return;
        }

        String tableName = "drop_add_comp_pk";

        dropTableIfExists(tableName);

        String sql = "CREATE TABLE " + tableName + " (\n"
            + "col1 varchar(255) DEFAULT NULL,\n"
            + "col2 varchar(255) NOT NULL,\n"
            + "id int(11) NOT NULL AUTO_INCREMENT,\n"
            + "PRIMARY KEY (id,col2)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "ALTER TABLE " + tableName + "\n"
            + "DROP PRIMARY KEY,\n"
            + "DROP COLUMN col2,\n"
            + "ADD PRIMARY KEY (id) USING BTREE";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testAlterTableDropKey() throws SQLException {
        if (TStringUtil.isNotEmpty(schemaPrefix)) {
            return;
        }

        String tableName = "test_drop_key";

        dropTableIfExists(tableName);

        String sql = "create table if not exists %s (c1 int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s add key (c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s drop key c1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s add key (c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    public void testAlterColumnWithInvalidDefaultValue1() throws SQLException {
        if (TStringUtil.isNotEmpty(schemaPrefix)) {
            return;
        }

        String tableName = "test_column_invalid_default_value1";

        dropTableIfExists(tableName);

        String sql = "create table if not exists %s (c1 datetime)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column c1 datetime NOT NULL DEFAULT now+1000*60*5";
        JdbcUtil.executeUpdateFailed(
            tddlConnection,
            String.format(sql, tableName),
            "Not all physical DDLs have been executed successfully"
        );

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
    }

    @Test
    public void testAlterColumnWithInvalidDefaultValue2() throws SQLException {
        if (TStringUtil.isNotEmpty(schemaPrefix)) {
            return;
        }

        String tableName = "test_column_invalid_default_value2";

        dropTableIfExists(tableName);

        String sql = "create table if not exists %s (c1 datetime)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column c1 text default '222222'";
        JdbcUtil.executeUpdateFailed(
            tddlConnection,
            String.format(sql, tableName),
            "Not all physical DDLs have been executed successfully"
        );

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
    }

    @Test
    public void testAlterColumnWithInvalidDefaultValue3() throws SQLException {
        if (TStringUtil.isNotEmpty(schemaPrefix)) {
            return;
        }

        String tableName = "test_column_invalid_default_value3";

        dropTableIfExists(tableName);

        String sql = "create table if not exists %s (c1 int primary key auto_increment)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s drop primary key";
        JdbcUtil.executeUpdateFailed(
            tddlConnection,
            String.format(sql, tableName),
            "Not all physical DDLs have been executed successfully"
        );

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
    }

    @Test
    public void testAlterColumnWithInvalidDefaultValue4() throws SQLException {
        if (TStringUtil.isNotEmpty(schemaPrefix)) {
            return;
        }

        String tableName = "test_column_invalid_default_value4";

        dropTableIfExists(tableName);

        String sql = "create table if not exists %s (c1 bigint)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column c1 bigint(32) NOT NULL DEFAULT CURRENT_TIMESTAMP";
        JdbcUtil.executeUpdateFailed(
            tddlConnection,
            String.format(sql, tableName),
            "Not all physical DDLs have been executed successfully"
        );

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
    }

    @Test
    public void testUpdateStatisticsAfterDdl() throws SQLException {
        String simpleTableName = "test_stats_after_ddl";
        String simpleTableNameRenamed = simpleTableName + "_renamed";
        String tableName = schemaPrefix + simpleTableName;
        String tableNameRenamed = tableName + "_renamed";

        dropTableIfExists(tableName);
        dropTableIfExists(tableNameRenamed);

        String sql = "create table %s(c1 int not null primary key, c2 int, c3 int) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "select * from %s where c1 > 10";
        JdbcUtil.executeSuccess(tddlConnection, String.format(sql, tableName));
        checkVirtualStatistics(simpleTableName, new String[] {"c1", "c2", "c3"});

        sql = "rename table %s to %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, tableNameRenamed));
        checkVirtualStatistics(simpleTableName, null);
        checkVirtualStatistics(simpleTableNameRenamed, new String[] {"c1", "c2", "c3"});

        sql = "alter table %s drop column c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableNameRenamed));
        checkVirtualStatistics(simpleTableNameRenamed, new String[] {"c1", "c2"});

        sql = "alter table %s change column c2 c4 bigint";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableNameRenamed));
        checkVirtualStatistics(simpleTableNameRenamed, new String[] {"c1", "c4"});

        dropTableIfExists(tableNameRenamed);
        checkVirtualStatistics(simpleTableNameRenamed, null);
    }

    @Test
    public void testAlterTableAddColumnWithKeys() throws SQLException {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String mysqlSchema = TStringUtil.isBlank(mysqlDatabase2) ? mysqlDatabase1 : mysqlDatabase2;
        String simpleTableName = "test_add_col_with_key";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(simpleTableName);

        String sql = "create table if not exists %s (c1 smallint not null unique key primary key)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        sql += " dbpartition by hash(`c1`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);
        Pair<String, String> phyDbTableName = fetchPhyDbAndTableNames(schemaName, simpleTableName, false);
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s "
            + "add c2 enum ('e1' , 'e2', 'e3', 'e4', 'e5') null, "
            + "add c3 boolean null unique, "
            + "add c4 timestamp unique key, "
            + "add c5 year(4) null unique";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s drop c2, drop c3, drop c4, drop c5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add column ("
            + "c2 enum ('e1' , 'e2', 'e3', 'e4', 'e5') null, "
            + "c3 boolean null unique, "
            + "c4 timestamp unique key, "
            + "c5 year(4) null unique)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "drop index c5 on %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(simpleTableName);
    }

    @Test
    public void testAlterTableColumnPosition() throws SQLException {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String mysqlSchema = TStringUtil.isBlank(mysqlDatabase2) ? mysqlDatabase1 : mysqlDatabase2;
        String simpleTableName = "test_col_pos";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(simpleTableName);

        String sql = "create table %s ("
            + "`id` bigint(20) unsigned not null auto_increment, "
            + "`buyer_id` bigint(20) not null, "
            + "`msg_id` varchar(60) not null, "
            + "`action_type` int(11) not null, "
            + "`coins` int(11) not null, "
            + "`msg` varchar(30) not null, "
            + "`create_at` timestamp not null default current_timestamp, "
            + "`update_at` timestamp not null default current_timestamp on update current_timestamp, "
            + "primary key (`id`))";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        sql += " dbpartition by hash(`buyer_id`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        sql = "alter table %s add column `order_id` bigint(20) not null default '0' after `msg`";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        sql = "alter table %s drop column `coins`, drop column `msg`";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(simpleTableName);
    }

    @Test
    public void testAlterTableColumnIndexWithEscapeChar() {
        String tableName = schemaPrefix + "test_escaping_col_name";

        dropTableIfExists(tableName);

        String sql = "create table %s (id int not null primary key, name varchar(10), age int, dept int) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "create index idx_name on %s(name, dept)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "create index idx_age on %s(age)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s rename index idx_age to `'`";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s change column `name` `'` varchar(10)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s drop index `'`";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s add index `'`(age)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "drop index `'` on %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "create index `'` on %s(age)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s drop column `'`";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s add column `'` varchar(10)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "create index idx_new on %s(`'`, dept)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "drop index idx_new on %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s add index idx_new(`'`, dept)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s drop index idx_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        dropTableIfExists(tableName);
    }

    @Test
    @Ignore("not supported")
    public void testSetGlobalInstantAddColumn() throws Exception {
        setGlobalSupportInstantAddColumn(true);
        Assert.assertTrue(checkInstantAddColumnVariables(true));

        setGlobalSupportInstantAddColumn(false);
        Assert.assertTrue(checkInstantAddColumnVariables(false));
    }

    @Test
    public void testInstantAddColumn() throws SQLException {
        testInstantAddColumn(true);
    }

    @Test
    public void testInstantAddColumnComplexUnsupported() throws Exception {
        testInstantAddColumnComplex(false);
    }

    private void testInstantAddColumn(boolean supported) throws SQLException {
        setGlobalSupportInstantAddColumn(supported);

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String mysqlSchema = TStringUtil.isBlank(mysqlDatabase2) ? mysqlDatabase1 : mysqlDatabase2;
        String simpleTableName = "test_instant_add_column";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(simpleTableName);

        String sql = "create table %s (c1 int not null primary key, c2 int)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        sql += " dbpartition by hash(c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        sql = "alter table %s add c3 int first, add c4 int, add c5 int after c1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        sql = "alter table %s add c6 int after c1, drop c5, add c7 int first, "
            + "modify c4 varchar(100), add c8 int after c2, change c3 c33 bigint";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        sql = "alter table %s add c9 int after c9";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Unknown column 'c9'");

        sql = "alter table %s add c9 int, compression='zlib'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        sql = "alter table %s drop primary key";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        sql = "alter table %s add c10 int not null primary key";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName));
        compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(simpleTableName);
    }

    @Test
    public void testInstantAddColumnComplex() throws Exception {
        testInstantAddColumnComplex(true);
    }

    public void testInstantAddColumnComplex(boolean supported) throws Exception {
        setGlobalSupportInstantAddColumn(supported);

        String msg = "Start testing %s table:";
        log.info(String.format(msg, "single"));
        testInstantAddColumnComplex("");

        log.info(String.format(msg, "broadcast"));
        testInstantAddColumnComplex("broadcast");

        log.info(String.format(msg, "sharding with db only"));
        testInstantAddColumnComplex("dbpartition by hash(id)");

        log.info(String.format(msg, "sharding with both db and tb"));
        testInstantAddColumnComplex("dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2");
    }

    private void testInstantAddColumnComplex(String extSyntax) throws Exception {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String mysqlSchema = TStringUtil.isBlank(mysqlDatabase2) ? mysqlDatabase1 : mysqlDatabase2;
        String simpleTableName = "test_instant_add_column_complex";
        String tableName = schemaPrefix + simpleTableName;
        String simpleBaseTableName = simpleTableName + "_base";
        String baseTableName = schemaPrefix + simpleBaseTableName;

        dropTableIfExists(tableName);
        dropTableIfExists(baseTableName);
        dropTableIfExistsInMySql(simpleTableName);

        String sql = "create table %s(id int not null, name varchar(32), primary key(id)) %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, baseTableName, extSyntax));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, extSyntax));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName, ""));

        testAlterThenInserts(schemaName, simpleTableName, tableName, mysqlSchema, baseTableName);

        dropTableIfExists(tableName);
        dropTableIfExists(baseTableName);
        dropTableIfExistsInMySql(simpleTableName);
    }

    private void testAlterThenInserts(String schemaName, String simpleTableName, String tableName, String mysqlSchema,
                                      String baseTableName) throws Exception {
        String sql;
        for (String[] param : TEST_PARAMS) {
            log.info("Start Change " + param[0]);

            sql = "alter table %s %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, baseTableName, param[1]));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, param[1]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, simpleTableName, param[1]));

            // The structures should be consistent between logical and physical tables.
            compareAllColumnPositions(schemaName, tableName, mysqlSchema, simpleTableName);

            // The insert statements should execute successfully.
            sql = "insert into %s() values %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, param[2]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName, param[2]));
            sql = "insert into %s() values %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, param[3]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName, param[3]));
            sql = "insert into %s() values %s on duplicate key update %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, param[4], param[5]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName, param[4], param[5]));

            sql = "select * from %s";
            selectContentSameAssert(String.format(sql, tableName), null, mysqlConnection, tddlConnection, false);

            sql = "delete from %s where id > 0";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName));

            // reverse column order
            sql = "insert into %s %s values %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format(sql, tableName, param[6], reverseValues(param[2])));
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format(sql, tableName, param[6], reverseValues(param[2])));
            sql = "insert into %s %s values %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format(sql, tableName, param[6], reverseValues(param[3])));
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format(sql, tableName, param[6], reverseValues(param[3])));
            sql = "insert into %s %s values %s on duplicate key update %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format(sql, tableName, param[6], reverseValues(param[4]), param[5]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format(sql, tableName, param[6], reverseValues(param[4]), param[5]));

            sql = "select * from %s";
            selectContentSameAssert(String.format(sql, tableName), null, mysqlConnection, tddlConnection, false);

            sql = "delete from %s where id > 0";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName));

            // Add some data to select source table.
            sql = "insert into %s values %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, baseTableName, param[3]));
            sql = "insert into %s select * from %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, baseTableName));

            // Clear table.
            sql = "delete from %s where id > 0";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            sql = "delete from %s where id > 0";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, baseTableName));
        }

        // Bad cases that should fail.
        sql = "alter table %s add newdept int after newdept";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Unknown column 'newdept'");
        sql = "alter table %s modify newdept int after newdept";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Unknown column 'newdept'");
        sql = "alter table %s change newdept newdept int after newdept";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Unknown column 'newdept'");
        sql = "alter table %s change address newdept int after newdept";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Unknown column 'newdept'");
        sql = "alter table %s change address newdept int after nodept";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Unknown column 'nodept'");
        sql = "alter table %s add newdept int after nodept";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Unknown column 'nodept'");
    }

    private static String reverseValues(String valueString) {
        List<String> values = new ArrayList<>();
        String regex = "\\((.*?)\\)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(valueString);
        while (matcher.find()) {
            values.add(matcher.group());
        }

        List<String> reversedValues = new ArrayList<>();
        for (String value : values) {
            String[] splitValue = value.substring(1, value.length() - 1).split(",");
            Collections.reverse(Arrays.asList(splitValue));
            reversedValues.add("(" + String.join(",", splitValue) + ")");
        }
        return String.join(",", reversedValues);
    }

    private static final String[][] TEST_PARAMS = new String[][] {
        new String[] {
            "1",
            "add dept int, add company int",
            "(1111,'aaa',1112,1113)",
            "(2221,'bbb',2222,2223),(3331,'ccc',3332,3333),(4441,'ddd',4442,4443)",
            "(3331,'ccc',3332,3333)", "name='ccc++'",
            "(company,dept,name,id)"},
        new String[] {
            "2",
            "modify dept bigint after id, change company corp varchar(64) after dept",
            "(1111,1112,'aaaa','aaab')",
            "(2221,2222,'bbba','bbbb'),(3331,3332,'ccca','cccb'),(4441,4442,'ddda','dddb')",
            "(3331,3332,'ccca','cccb')", "name='ccc++', corp='ccc++'",
            "(name,corp,dept,id)"},
        new String[] {
            "3",
            "change name nickname varchar(64) after id, add boss int first",
            "(1111,1112,'aaaa',1113,'aaab')",
            "(2221,2222,'bbba',2223,'bbbb'),(3331,3332,'ccca',3333,'cccb'),(4441,4442,'ddda',4443,'dddb')",
            "(3331,3332,'ccca',3333,'cccb')", "nickname='ccc++'",
            "(corp,dept,nickname,id,boss)"},
        new String[] {
            "4",
            "add age int after nickname",
            "(1111,1112,'aaaa',1113,1114,'aaab')",
            "(2221,2222,'bbba',2223,2224,'bbbb'),(3331,3332,'ccca',3333,3334,'cccb'),(4441,4442,'ddda',4443,4444,'dddb')",
            "(3331,3332,'ccca',3333,3334,'cccb')", "corp='ccc++', nickname='ccc++'",
            "(corp,dept,age,nickname,id,boss)"},
        new String[] {
            "5",
            "drop boss, add leader char(1) after age",
            "(1111,'aaaa',1112,'Y',1113,'aaab')",
            "(2221,'bbba',2222,'N',2223,'bbbb'),(3331,'ccca',3332,'Y',3333,'cccb'),(4441,'ddda',4442,'N',4443,'dddb')",
            "(3331,'ccca',3332,'Y',3333,'cccb')", "nickname='ccc+++'",
            "(corp,dept,leader,age,nickname,id)"},
        new String[] {
            "6",
            "add address varchar(256), drop corp, add years int after leader",
            "(1111,'aaaa',1112,'n',1113,1114,'aaab')",
            "(2221,'bbba',2222,'y',2223,2224,'bbbb'),(3331,'ccca',3332,'Y',3333,3334,'cccb'),(4441,'ddda',4442,'N',4443,4444,'dddb')",
            "(3331,'ccca',3332,'n',3333,3334,'cccb')", "address='ccc++'",
            "(address,dept,years,leader,age,nickname,id)"}
    };

    @Test
    public void testSimpleInsertAfterInstantAddColumn() {
        setGlobalSupportInstantAddColumn(true);

        String simpleTableName = "test_instant_add_column_insert";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String sql = "create table %s (c1 int not null primary key, c2 int) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        sql = "create table %s (c1 int not null primary key, c2 int)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName));

        sql = "alter table %s add c3 varchar(16) after c1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName));

        sql = "insert into %s() values(2,'b',3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName));

        sql = "insert into %s(c2,c3,c1) values(2,'b',3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName));

        sql = "insert into %s(c1) values(4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(sql, tableName));

        sql = "select * from %s";
        selectContentSameAssert(String.format(sql, tableName), null, mysqlConnection, tddlConnection, false);
    }

    private void compareAllColumnPositions(String logicalSchemaName, String tableName, String physicalSchemaName,
                                           String simpleTableName) throws SQLException {
        compareColumnPositions(logicalSchemaName, simpleTableName, physicalSchemaName, simpleTableName);
        compareColumnPositions(logicalSchemaName, simpleTableName, String.format("desc %s", tableName));
        compareColumnPositions(logicalSchemaName, simpleTableName,
            String.format("show columns from %s from %s", simpleTableName, logicalSchemaName));
        compareColumnPositions(logicalSchemaName, simpleTableName, String.format(
            "select column_name from information_schema.columns where table_schema='%s' and table_name='%s' order by ordinal_position",
            logicalSchemaName, simpleTableName));
    }

    private void compareColumnPositions(String schemaName, String tableName, String statement)
        throws SQLException {
        Map<Integer, String> sysTableColumnPositions = fetchColumnPositionsFromSysTable(schemaName, tableName);
        Map<Integer, String> statementColumnPositions = fetchColumnPositionsFromStatement(statement);
        compareColumnPositions(sysTableColumnPositions, statementColumnPositions);
    }

    private void compareColumnPositions(String schemaName, String tableName, String phyDbName, String phyTableName)
        throws SQLException {
        Map<Integer, String> sysTableColumnPositions = fetchColumnPositionsFromSysTable(schemaName, tableName);
        Map<Integer, String> phyTableColumnPositions = fetchColumnPositionsFromPhyTable(phyDbName, phyTableName);
        compareColumnPositions(sysTableColumnPositions, phyTableColumnPositions);
    }

    private void compareColumnPositions(Map<Integer, String> sysTableColumnPositions,
                                        Map<Integer, String> phyTableColumnPositions) {
        printColumnPositions(sysTableColumnPositions, "System Table");
        printColumnPositions(phyTableColumnPositions, "Physical Info Schema");

        if (sysTableColumnPositions == null || sysTableColumnPositions.isEmpty() ||
            phyTableColumnPositions == null || phyTableColumnPositions.isEmpty()) {
            Assert.fail("Invalid column names and positions");
        }

        if (sysTableColumnPositions.size() != phyTableColumnPositions.size()) {
            Assert.fail("Different column sizes");
        }

        for (Integer columnPos : sysTableColumnPositions.keySet()) {
            if (!TStringUtil.equalsIgnoreCase(sysTableColumnPositions.get(columnPos),
                phyTableColumnPositions.get(columnPos))) {
                Assert.fail("Different column names '" + sysTableColumnPositions.get(columnPos) + "' and '"
                    + phyTableColumnPositions.get(columnPos) + "' at " + columnPos);
            }
        }
    }

    private Map<Integer, String> fetchColumnPositionsFromSysTable(String schemaName, String tableName)
        throws SQLException {
        Map<Integer, String> columnPositions = new HashMap<>();
        String sql = "select ordinal_position, column_name from columns "
            + "where table_schema='%s' and table_name='%s' order by ordinal_position";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName))) {
            while (rs.next()) {
                columnPositions.put(rs.getInt(1), rs.getString(2));
            }
        }
        return columnPositions;
    }

    private Map<Integer, String> fetchColumnPositionsFromPhyTable(String phyDbName, String phyTableName)
        throws SQLException {
        Map<Integer, String> columnPositions = new HashMap<>();
        String sql = "select ordinal_position, column_name from information_schema.columns "
            + "where table_schema='%s' and table_name='%s' order by ordinal_position";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, phyDbName, phyTableName))) {
            while (rs.next()) {
                columnPositions.put(rs.getInt(1), rs.getString(2));
            }
        }
        return columnPositions;
    }

    private Map<Integer, String> fetchColumnPositionsFromStatement(String statement) throws SQLException {
        Map<Integer, String> columnPositions = new HashMap<>();
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(statement)) {
                int index = 1;
                while (rs.next()) {
                    columnPositions.put(index++, rs.getString(1));
                }
            }
        }
        return columnPositions;
    }

    private void printColumnPositions(Map<Integer, String> columnPositions, String tableInfo) {
        if (MapUtils.isNotEmpty(columnPositions)) {
            StringBuilder buf = new StringBuilder();
            buf.append("\n").append("Column Positions from ").append(tableInfo).append(":\n");
            for (Map.Entry<Integer, String> entry : columnPositions.entrySet()) {
                buf.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
            }
            log.info(buf);
        } else {
            log.info("No Column Positions from " + tableInfo);
        }
    }

    @Test
    public void testAlterTableAddIndex() throws SQLException {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String simpleTableName = "test_add_index";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);

        String sql =
            "create table %s (c1 int not null primary key, c2 int, c3 int, c4 int, c5 int) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        Pair<String, String> phyDbTableName = fetchPhyDbAndTableNames(schemaName, simpleTableName, false);
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add index (c2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add index (c3,c4,c5)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add index (c4,c5)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add index (c3,c5), add index (c3,c5)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add index i_1(c2,c4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add index i_2(c3), add index i_3(c3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add unique (c2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add unique (c3,c4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add unique (c4,c5), add unique (c4,c5)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add unique u_4(c4,c5), add index u_5(c4,c5)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add unique u_6(c3,c4,c5)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add index (c5), drop index i_2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add unique (c5,c3), drop index u_4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        sql = "alter table %s add index i_2(c5,c3), add index i_4(c4,c2), add unique (c4,c3), add unique (c2,c5)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        dropTableIfExists(tableName);
    }

    private void compareIndexColumnInfo(String schemaName, String tableName, String phyDbName, String phyTableName)
        throws SQLException {
        Map<String, Map<Integer, String>> sysTableIndexColumnInfo =
            fetchIndexColumnInfoFromSysTable(schemaName, tableName);
        Map<String, Map<Integer, String>> phyTableIndexColumnInfo =
            fetchIndexColumnInfoFromPhyTable(phyDbName, phyTableName);
        compareIndexColumnInfo(sysTableIndexColumnInfo, phyTableIndexColumnInfo);
    }

    private void compareIndexColumnInfo(Map<String, Map<Integer, String>> sysTableIndexColumnInfo,
                                        Map<String, Map<Integer, String>> phyTableIndexColumnInfo) {
        printIndexColumnInfo(sysTableIndexColumnInfo, "System Table");
        printIndexColumnInfo(phyTableIndexColumnInfo, "Physical Info Schema");

        if (MapUtils.isEmpty(sysTableIndexColumnInfo) && MapUtils.isEmpty(phyTableIndexColumnInfo)) {
            // No index exists.
            return;
        }

        if (MapUtils.isEmpty(sysTableIndexColumnInfo) || MapUtils.isEmpty(phyTableIndexColumnInfo)) {
            Assert.fail("Invalid Index Column Info");
        }

        if (sysTableIndexColumnInfo.size() != phyTableIndexColumnInfo.size()) {
            Assert.fail("Different Index Column Info sizes");
        }

        compareIndexColumnInfoInternal(sysTableIndexColumnInfo, phyTableIndexColumnInfo, "Physical Info Schema");
        compareIndexColumnInfoInternal(phyTableIndexColumnInfo, sysTableIndexColumnInfo, "System Table");
    }

    private void compareIndexColumnInfoInternal(Map<String, Map<Integer, String>> sourceIndexColumnInfo,
                                                Map<String, Map<Integer, String>> targetIndexColumnInfo,
                                                String tableInfo) {
        for (String indexName : sourceIndexColumnInfo.keySet()) {
            if (targetIndexColumnInfo.containsKey(indexName)) {
                compareSeqColumnInfo(sourceIndexColumnInfo.get(indexName), targetIndexColumnInfo.get(indexName));
            } else {
                Assert.fail("No Index '" + indexName + "' in " + tableInfo);
            }
        }
    }

    private void compareSeqColumnInfo(Map<Integer, String> sysTableSeqColumnInfo,
                                      Map<Integer, String> phyTableSeqColumnInfo) {
        if (MapUtils.isEmpty(sysTableSeqColumnInfo) || MapUtils.isEmpty(phyTableSeqColumnInfo)) {
            Assert.fail("Invalid Seq Column Info");
        }

        if (sysTableSeqColumnInfo.size() != phyTableSeqColumnInfo.size()) {
            Assert.fail("Different Seq Column Info sizes");
        }

        compareSeqColumnInfoInternal(sysTableSeqColumnInfo, "sys", phyTableSeqColumnInfo, "phy",
            "Physical Info Schema");

        compareSeqColumnInfoInternal(phyTableSeqColumnInfo, "phy", sysTableSeqColumnInfo, "sys",
            "System Table");
    }

    private void compareSeqColumnInfoInternal(Map<Integer, String> sourceSeqColumnInfo, String sourceInfo,
                                              Map<Integer, String> targetSeqColumnInfo, String targetInfo,
                                              String tableInfo) {
        for (Integer seq : sourceSeqColumnInfo.keySet()) {
            if (targetSeqColumnInfo.containsKey(seq)) {
                if (!TStringUtil.equalsIgnoreCase(sourceSeqColumnInfo.get(seq), targetSeqColumnInfo.get(seq))) {
                    Assert.fail("Different " + sourceInfo + " column '" + sourceSeqColumnInfo.get(seq) + "' and "
                        + targetInfo + " column '" + targetSeqColumnInfo.get(seq) + " at " + seq);
                }
            } else {
                Assert.fail("No Seq '" + seq + "' in " + tableInfo);
            }
        }
    }

    private void printIndexColumnInfo(Map<String, Map<Integer, String>> indexColumnInfo, String tableInfo) {
        if (MapUtils.isNotEmpty(indexColumnInfo)) {
            StringBuilder buf = new StringBuilder();
            buf.append("\n").append("Index Column Info from ").append(tableInfo).append(":\n");
            for (String indexName : indexColumnInfo.keySet()) {
                buf.append("Index '").append(indexName).append("' - ");
                if (MapUtils.isNotEmpty(indexColumnInfo.get(indexName))) {
                    for (Map.Entry<Integer, String> entry : indexColumnInfo.get(indexName).entrySet()) {
                        buf.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
                    }
                } else {
                    buf.append("No Seq_In_Index and Column Name for this index");
                }
                buf.append("\n");
            }
            log.info(buf);
        } else {
            log.info("No Index Column Info from " + tableInfo);
        }
    }

    private Map<String, Map<Integer, String>> fetchIndexColumnInfoFromSysTable(String schemaName, String tableName)
        throws SQLException {
        Map<String, Map<Integer, String>> indexColumnInfo = new HashMap<>();
        String sql = "select index_name,seq_in_index,column_name from indexes "
            + "where table_schema='%s' and table_name='%s' order by index_name,seq_in_index";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName))) {
            while (rs.next()) {
                String indexName = rs.getString(1);
                if (!indexColumnInfo.containsKey(indexName)) {
                    Map<Integer, String> seqColumns = new HashMap<>();
                    indexColumnInfo.put(indexName, seqColumns);
                }
                indexColumnInfo.get(indexName).put(rs.getInt(2), rs.getString(3));
            }
        }
        return indexColumnInfo;
    }

    private Map<String, Map<Integer, String>> fetchIndexColumnInfoFromPhyTable(String phyDbName, String phyTableName)
        throws SQLException {
        Map<String, Map<Integer, String>> indexColumnInfo = new HashMap<>();
        String sql = "select index_name,seq_in_index,column_name from information_schema.statistics "
            + "where table_schema='%s' and table_name='%s' order by index_name,seq_in_index";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, phyDbName, phyTableName))) {
            while (rs.next()) {
                String indexName = rs.getString(1);
                if (!indexColumnInfo.containsKey(indexName)) {
                    Map<Integer, String> seqColumns = new HashMap<>();
                    indexColumnInfo.put(indexName, seqColumns);
                }
                indexColumnInfo.get(indexName).put(rs.getInt(2), rs.getString(3));
            }
        }
        return indexColumnInfo;
    }

    private Pair<String, String> fetchPhyDbAndTableNames(String schemaName, String tableName, boolean isSingle)
        throws SQLException {
        String phyDbName = null, phyTableName = null;
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement()) {
            String sql = "show databases";
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String phyDatabase = rs.getString(1);
                    if (TStringUtil.startsWithIgnoreCase(phyDatabase, schemaName) &&
                        TStringUtil.containsIgnoreCase(phyDatabase, isSingle ? "single" : "000000")) {
                        phyDbName = phyDatabase;
                        break;
                    }
                }
            }
            sql = "select tb_name_pattern from tables_ext where table_schema='%s' and table_name='%s'";
            try (ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName))) {
                if (rs.next()) {
                    phyTableName = rs.getString(1);
                }
            }
        }
        return new Pair<>(phyDbName, phyTableName);
    }

    private String fetchPrimaryKeyColumns(String tableName) throws SQLException {
        String sql =
            "select column_name from indexes where table_schema='%s' and table_name='%s' and index_name='PRIMARY' order by seq_in_index";

        StringBuilder buf = new StringBuilder();

        try (Connection metaDbConn = getMetaConnection();
            PreparedStatement ps = metaDbConn
                .prepareStatement(String.format(sql, tddlDatabase1, tableName));
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                buf.append(",").append(rs.getString(1));
            }
        }

        return buf.length() > 0 ? buf.deleteCharAt(0).toString() : null;
    }

    @Test
    public void testForeignKey() {
        String parentNamePrefix = "fk_parent_";
        String parentSingle = schemaPrefix + parentNamePrefix + "s";
        String parentBroadcast = schemaPrefix + parentNamePrefix + "b";
        String parentShardingDb = schemaPrefix + parentNamePrefix + "x";
        String parentShardingDbTb = schemaPrefix + parentNamePrefix + "z";

        String childNamePrefix = "fk_child_";
        String childSingleToSingle = schemaPrefix + childNamePrefix + "s2s";
        String childSingleToOthers = schemaPrefix + childNamePrefix + "s2o";
        String childBroadcastToAll = schemaPrefix + parentNamePrefix + "b2a";
        String childShardingDbToAll = schemaPrefix + parentNamePrefix + "x2a";
        String childShardingDbTbToAll = schemaPrefix + parentNamePrefix + "z2a";

        String extSingle = "";
        String extBroadcast = "broadcast";
        String extShardingDb = "dbpartition by hash(id)";
        String extShardingDbTb = extShardingDb + " tbpartition by hash(id) tbpartitions 2";

        String errUnsupported = "Do not support foreign key";
        String errInvalidReference = "Do not support foreign key";

        // child tables first, then parent tables
        dropTablesIfExists(new String[] {
            childSingleToSingle, childSingleToOthers, childBroadcastToAll, childShardingDbToAll, childShardingDbTbToAll,
            parentSingle, parentBroadcast, parentShardingDb, parentShardingDbTb
        });

        // create parent tables
        createParentTables(new String[][] {
            new String[] {parentSingle, extSingle},
            new String[] {parentBroadcast, extBroadcast},
            new String[] {parentShardingDb, extShardingDb},
            new String[] {parentShardingDbTb, extShardingDbTb}
        });

        createChildTables(new String[][] {
            new String[] {childSingleToSingle, extSingle},
            new String[] {childSingleToOthers, extSingle},
            new String[] {childBroadcastToAll, extBroadcast},
            new String[] {childShardingDbToAll, extShardingDb},
            new String[] {childShardingDbTbToAll, extShardingDbTb}
        });

        alterChildTablesAddForeignKeys(new Object[][] {
            new Object[] {
                childSingleToSingle, parentSingle, false, errUnsupported},
            new Object[] {childSingleToOthers, parentBroadcast, false, errInvalidReference},
            new Object[] {childSingleToOthers, parentShardingDb, false, errInvalidReference},
            new Object[] {childSingleToOthers, parentShardingDbTb, false, errInvalidReference},
            new Object[] {childBroadcastToAll, parentSingle, false, errUnsupported},
            new Object[] {childBroadcastToAll, parentBroadcast, false, errUnsupported},
            new Object[] {childBroadcastToAll, parentShardingDb, false, errUnsupported},
            new Object[] {childBroadcastToAll, parentShardingDbTb, false, errUnsupported},
            new Object[] {childShardingDbToAll, parentSingle, false, errUnsupported},
            new Object[] {childShardingDbToAll, parentBroadcast, false, errUnsupported},
            new Object[] {childShardingDbToAll, parentShardingDb, false, errUnsupported},
            new Object[] {childShardingDbToAll, parentShardingDbTb, false, errUnsupported},
            new Object[] {childShardingDbTbToAll, parentSingle, false, errUnsupported},
            new Object[] {childShardingDbTbToAll, parentBroadcast, false, errUnsupported},
            new Object[] {childShardingDbTbToAll, parentShardingDb, false, errUnsupported},
            new Object[] {childShardingDbTbToAll, parentShardingDbTb, false, errUnsupported},
        });

        // child tables first, then parent tables
        dropTablesIfExists(new String[] {
            childSingleToSingle, childSingleToOthers, childBroadcastToAll, childShardingDbToAll, childShardingDbTbToAll,
            parentSingle, parentBroadcast, parentShardingDb, parentShardingDbTb
        });
    }

    private void createParentTables(String[][] params) {
        createTables("create table if not exists %s (id int not null primary key, age int) %s", params);
    }

    private void createChildTables(String[][] params) {
        createTables("create table if not exists %s (id int not null primary key, age int, pid int not null) %s",
            params);
    }

    private void createTables(String sqlTemplate, String[][] params) {
        for (String[] param : params) {
            String parentSql = String.format(sqlTemplate, param[0], param[1]);
            JdbcUtil.executeUpdateSuccess(tddlConnection, parentSql);
        }
    }

    private void alterChildTablesAddForeignKeys(Object[][] params) {
        String childTemplate = "alter table %s add foreign key (pid) references %s(id)";
        for (Object[] param : params) {
            String childSql = String.format(childTemplate, param[0], param[1]);
            if ((Boolean) param[2]) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, childSql);
            } else {
                JdbcUtil.executeUpdateFailed(tddlConnection, childSql, (String) param[3]);
            }
        }
    }

    @Test
    public void testAlterTableModifyChangeColumnAddKeys() throws SQLException {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String simpleTableName = "test_modify_with_pk";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);

        String createSql =
            "create table %s (id int not null, name char(32), item varchar(32), primary key (id)) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createSql, tableName));
        Pair<String, String> phyDbTableName = fetchPhyDbAndTableNames(schemaName, simpleTableName, true);
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        String dropPkSql = "alter table %s drop primary key";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropPkSql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        String alterSql =
            "alter table %s modify id bigint not null primary key auto_increment, modify name char(32) unique";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(alterSql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());
        checkSequenceExistence(schemaName, simpleTableName);

        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createSql, tableName));
        phyDbTableName = fetchPhyDbAndTableNames(schemaName, simpleTableName, true);
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropPkSql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());

        alterSql =
            "alter table %s change id id bigint primary key not null auto_increment, change name name char(32) unique";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(alterSql, tableName));
        compareIndexColumnInfo(schemaName, simpleTableName, phyDbTableName.getKey(), phyDbTableName.getValue());
        checkSequenceExistence(schemaName, simpleTableName);

        dropTableIfExists(tableName);
    }

    private void checkSequenceExistence(String schemaName, String tableName) throws SQLException {
        String sql = "select name from sequence where schema_name='%s' and name='AUTO_SEQ_%s'";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName))) {
            if (rs.next()) {
                // The sequence exists.
                return;
            }
        }
        Assert.fail(String.format("Not found sequence 'AUTO_SEQ_%s'", tableName));
    }

    @Test
    public void testTimeZone() throws SQLException {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String simpleTableName = "test_time_zone";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);

        String sql = "create table %s ("
            + "pk int not null primary key auto_increment,"
            + "c_timestamp_0 timestamp(6) not null default '2017-12-12 19:59:59.123000',"
            + "c_timestamp_1 timestamp(6) not null default '2017/12/12 19:59:59.123000',"
            + "c_timestamp_2 timestamp(6) not null default '2017-12-12 19:59:59.000456',"
            + "c_timestamp_3 timestamp(6) not null default '2017/12/12 19:59:59.000456',"
            + "pad int"
            + ") dbpartition by yyyymm(c_timestamp_0)";

        String showCreateTableTemplate = "CREATE TABLE `%s` (\n"
            + "\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "\t`c_timestamp_0` timestamp(6) NOT NULL DEFAULT '%s',\n"
            + "\t`c_timestamp_1` timestamp(6) NOT NULL DEFAULT '%s',\n"
            + "\t`c_timestamp_2` timestamp(6) NOT NULL DEFAULT '%s',\n"
            + "\t`c_timestamp_3` timestamp(6) NOT NULL DEFAULT '%s',\n"
            + "\t`pad` int(11) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`),\n"
            + "\tKEY `auto_shard_key_c_timestamp_0` USING BTREE (`c_timestamp_0`)\n";

        try {
            setTimeZone("+10:00");
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            checkTableMeta(schemaName, simpleTableName, tableName,
                String.format(showCreateTableTemplate, simpleTableName,
                    "2017-12-12 19:59:59.123000", "2017-12-12 19:59:59.123000",
                    "2017-12-12 19:59:59.000456", "2017-12-12 19:59:59.000456"
                ),
                new String[][] {
                    new String[] {"c_timestamp_0", "2017-12-12 17:59:59.123000"},
                    new String[] {"c_timestamp_1", "2017-12-12 17:59:59.123000"},
                    new String[] {"c_timestamp_2", "2017-12-12 17:59:59.000456"},
                    new String[] {"c_timestamp_3", "2017-12-12 17:59:59.000456"}
                },
                new String[][] {
                    new String[] {"c_timestamp_0", "2017-12-12 19:59:59.123000"},
                    new String[] {"c_timestamp_1", "2017-12-12 19:59:59.123000"},
                    new String[] {"c_timestamp_2", "2017-12-12 19:59:59.000456"},
                    new String[] {"c_timestamp_3", "2017-12-12 19:59:59.000456"}
                }
            );

            setTimeZone("+12:00");
            checkTableMeta(schemaName, simpleTableName, tableName,
                String.format(showCreateTableTemplate, simpleTableName,
                    "2017-12-12 21:59:59.123000", "2017-12-12 21:59:59.123000",
                    "2017-12-12 21:59:59.000456", "2017-12-12 21:59:59.000456"
                ),
                new String[][] {
                    new String[] {"c_timestamp_0", "2017-12-12 17:59:59.123000"},
                    new String[] {"c_timestamp_1", "2017-12-12 17:59:59.123000"},
                    new String[] {"c_timestamp_2", "2017-12-12 17:59:59.000456"},
                    new String[] {"c_timestamp_3", "2017-12-12 17:59:59.000456"}
                },
                new String[][] {
                    new String[] {"c_timestamp_0", "2017-12-12 21:59:59.123000"},
                    new String[] {"c_timestamp_1", "2017-12-12 21:59:59.123000"},
                    new String[] {"c_timestamp_2", "2017-12-12 21:59:59.000456"},
                    new String[] {"c_timestamp_3", "2017-12-12 21:59:59.000456"}
                }
            );

            sql = "insert into %s(c_timestamp_0,c_timestamp_1,c_timestamp_2,c_timestamp_3,pad) " +
                "values('2017-12-12 23:59:59.00000045','2017/12/12 23:59:59.00000045','2017-12-12 23:59:59.00000045',"
                + "'2017/12/12 23:59:59.00000045',111)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            checkQueryResult(tableName, false,
                new String[][] {
                    new String[] {
                        "2017-12-12 23:59:59.0", "2017-12-12 23:59:59.0",
                        "2017-12-12 23:59:59.0", "2017-12-12 23:59:59.0",
                        "111"
                    }
                }
            );

            sql = "insert into %s(pad) values(222)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            checkQueryResult(tableName, false,
                new String[][] {
                    new String[] {
                        "2017-12-12 23:59:59.0", "2017-12-12 23:59:59.0",
                        "2017-12-12 23:59:59.0", "2017-12-12 23:59:59.0",
                        "111"
                    },
                    new String[] {
                        "2017-12-12 21:59:59.123", "2017-12-12 21:59:59.123",
                        "2017-12-12 21:59:59.000456", "2017-12-12 21:59:59.000456",
                        "222"}
                }
            );

            showCreateTableTemplate = "CREATE TABLE `%s` (\n"
                + "\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                + "\t`c_timestamp_a` timestamp(6) NOT NULL DEFAULT '%s',\n"
                + "\t`c_timestamp_0` timestamp(6) NOT NULL DEFAULT '%s',\n"
                + "\t`c_timestamp_c` timestamp(6) NOT NULL DEFAULT '%s',\n"
                + "\t`c_timestamp_2` timestamp(6) NOT NULL DEFAULT '%s',\n"
                + "\t`c_timestamp_3` timestamp(6) NOT NULL DEFAULT '%s',\n"
                + "\t`pad` int(11) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`pk`),\n"
                + "\tKEY `auto_shard_key_c_timestamp_0` USING BTREE (`c_timestamp_0`)\n";

            sql = "alter table %s "
                + "add c_timestamp_a timestamp(6) not null default '2021/10/25 12:59:59.222000' after pk, "
                + "change c_timestamp_1 c_timestamp_c timestamp(6) not null default '2021-10-25 12:59:59.000444', "
                + "modify c_timestamp_2 timestamp(6) not null default '2021/10/25 12:59:59.789000', "
                + "alter c_timestamp_3 set default '2021-10-25 12:59:59.000789'";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            checkTableMeta(schemaName, simpleTableName, tableName,
                String.format(showCreateTableTemplate, simpleTableName,
                    "2021-10-25 12:59:59.222000", "2017-12-12 21:59:59.123000",
                    "2021-10-25 12:59:59.000444", "2021-10-25 12:59:59.789000",
                    "2021-10-25 12:59:59.000789", "100003"
                ),
                new String[][] {
                    new String[] {"c_timestamp_a", "2021-10-25 08:59:59.222000"},
                    new String[] {"c_timestamp_0", "2017-12-12 17:59:59.123000"},
                    new String[] {"c_timestamp_c", "2021-10-25 08:59:59.000444"},
                    new String[] {"c_timestamp_2", "2021-10-25 08:59:59.789000"},
                    new String[] {"c_timestamp_3", "2021-10-25 08:59:59.000789"}
                },
                new String[][] {
                    new String[] {"c_timestamp_a", "2021-10-25 12:59:59.222000"},
                    new String[] {"c_timestamp_0", "2017-12-12 21:59:59.123000"},
                    new String[] {"c_timestamp_c", "2021-10-25 12:59:59.000444"},
                    new String[] {"c_timestamp_2", "2021-10-25 12:59:59.789000"},
                    new String[] {"c_timestamp_3", "2021-10-25 12:59:59.000789"}
                }
            );

            sql = "insert into %s(pad) values(333)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            checkQueryResult(tableName, true,
                new String[][] {
                    new String[] {
                        "2021-10-25 12:59:59.222", "2017-12-12 23:59:59.0",
                        "2017-12-12 23:59:59.0", "2017-12-12 23:59:59.0",
                        "2017-12-12 23:59:59.0", "111"
                    },
                    new String[] {
                        "2021-10-25 12:59:59.222", "2017-12-12 21:59:59.123",
                        "2017-12-12 21:59:59.123", "2017-12-12 21:59:59.000456",
                        "2017-12-12 21:59:59.000456", "222"
                    },
                    new String[] {
                        "2021-10-25 12:59:59.222", "2017-12-12 21:59:59.123",
                        "2021-10-25 12:59:59.000444", "2021-10-25 12:59:59.789",
                        "2021-10-25 12:59:59.000789", "333"
                    }
                }
            );

            setTimeZone("+8:00");
            checkTableMeta(schemaName, simpleTableName, tableName,
                String.format(showCreateTableTemplate, simpleTableName,
                    "2021-10-25 08:59:59.222000", "2017-12-12 17:59:59.123000",
                    "2021-10-25 08:59:59.000444", "2021-10-25 08:59:59.789000",
                    "2021-10-25 08:59:59.000789", "200002"
                ),
                new String[][] {
                    new String[] {"c_timestamp_a", "2021-10-25 08:59:59.222000"},
                    new String[] {"c_timestamp_0", "2017-12-12 17:59:59.123000"},
                    new String[] {"c_timestamp_c", "2021-10-25 08:59:59.000444"},
                    new String[] {"c_timestamp_2", "2021-10-25 08:59:59.789000"},
                    new String[] {"c_timestamp_3", "2021-10-25 08:59:59.000789"}
                },
                new String[][] {
                    new String[] {"c_timestamp_a", "2021-10-25 08:59:59.222000"},
                    new String[] {"c_timestamp_0", "2017-12-12 17:59:59.123000"},
                    new String[] {"c_timestamp_c", "2021-10-25 08:59:59.000444"},
                    new String[] {"c_timestamp_2", "2021-10-25 08:59:59.789000"},
                    new String[] {"c_timestamp_3", "2021-10-25 08:59:59.000789"}
                }
            );
            checkQueryResult(tableName, true,
                new String[][] {
                    new String[] {
                        "2021-10-25 08:59:59.222", "2017-12-12 19:59:59.0",
                        "2017-12-12 19:59:59.0", "2017-12-12 19:59:59.0",
                        "2017-12-12 19:59:59.0", "111"
                    },
                    new String[] {
                        "2021-10-25 08:59:59.222", "2017-12-12 17:59:59.123",
                        "2017-12-12 17:59:59.123", "2017-12-12 17:59:59.000456",
                        "2017-12-12 17:59:59.000456", "222"
                    },
                    new String[] {
                        "2021-10-25 08:59:59.222", "2017-12-12 17:59:59.123",
                        "2021-10-25 08:59:59.000444", "2021-10-25 08:59:59.789",
                        "2021-10-25 08:59:59.000789", "333"
                    }
                }
            );
        } finally {
            // Reset to default time zone to avoid affecting other cases.
            setTimeZone("+8:00");
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testTimeZoneZeroValue() throws SQLException {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String simpleTableName = "test_time_zone_zero_value";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);

        String sql = "create table %s (id int,\n"
            + "\t fsp_def_val_0 timestamp default '2021-12-12 12:00:00',\n"
            + "\t fsp_def_val_1 timestamp default '2021-12-12 12:00:00.1',\n"
            + "\t fsp_def_val_2 timestamp default '2021-12-12 12:00:00.12',\n"
            + "\t fsp_def_val_3 timestamp default '2021-12-12 12:00:00.123',\n"
            + "\t fsp_def_val_4 timestamp default '2021-12-12 12:00:00.1234',\n"
            + "\t fsp_def_val_5 timestamp default '2021-12-12 12:00:00.12345',\n"
            + "\t fsp_def_val_6 timestamp default '2021-12-12 12:00:00.123456',\n"
            + "\t fsp_var_zero_00 timestamp(0) default '0000-00-00 00:00:00',\n"
            + "\t fsp_var_zero_01 timestamp(0) default '0000-00-00 00:00:00.0',\n"
            + "\t fsp_var_zero_10 timestamp(1) default '0000-00-00 00:00:00',\n"
            + "\t fsp_var_zero_11 timestamp(1) default '0000-00-00 00:00:00.0',\n"
            + "\t fsp_var_zero_12 timestamp(1) default '0000-00-00 00:00:00.00',\n"
            + "\t fsp_var_zero_20 timestamp(2) default '0000-00-00 00:00:00',\n"
            + "\t fsp_var_zero_21 timestamp(2) default '0000-00-00 00:00:00.0',\n"
            + "\t fsp_var_zero_22 timestamp(2) default '0000-00-00 00:00:00.00',\n"
            + "\t fsp_var_zero_23 timestamp(2) default '0000-00-00 00:00:00.000',\n"
            + "\t fsp_var_zero_30 timestamp(3) default '0000-00-00 00:00:00',\n"
            + "\t fsp_var_zero_31 timestamp(3) default '0000-00-00 00:00:00.0',\n"
            + "\t fsp_var_zero_33 timestamp(3) default '0000-00-00 00:00:00.000',\n"
            + "\t fsp_var_zero_34 timestamp(3) default '0000-00-00 00:00:00.0000',\n"
            + "\t fsp_var_zero_40 timestamp(4) default '0000-00-00 00:00:00',\n"
            + "\t fsp_var_zero_41 timestamp(4) default '0000-00-00 00:00:00.0',\n"
            + "\t fsp_var_zero_44 timestamp(4) default '0000-00-00 00:00:00.0000',\n"
            + "\t fsp_var_zero_45 timestamp(4) default '0000-00-00 00:00:00.00000',\n"
            + "\t fsp_var_zero_50 timestamp(5) default '0000-00-00 00:00:00',\n"
            + "\t fsp_var_zero_51 timestamp(5) default '0000-00-00 00:00:00.0',\n"
            + "\t fsp_var_zero_55 timestamp(5) default '0000-00-00 00:00:00.00000',\n"
            + "\t fsp_var_zero_56 timestamp(5) default '0000-00-00 00:00:00.000000',\n"
            + "\t fsp_var_zero_60 timestamp(6) default '0000-00-00 00:00:00',\n"
            + "\t fsp_var_zero_61 timestamp(6) default '0000-00-00 00:00:00.0',\n"
            + "\t fsp_var_zero_66 timestamp(6) default '0000-00-00 00:00:00.000000',\n"
            + "\t fsp_var_zero_67 timestamp(6) default '0000-00-00 00:00:00.0000000',\n"
            + "\t fsp_var_val_00 timestamp(0) default '2021-11-11 15:59:59',\n"
            + "\t fsp_var_val_01 timestamp(0) default '2021-11-11 15:59:59.1',\n"
            + "\t fsp_var_val_10 timestamp(1) default '2021-11-11 15:59:59',\n"
            + "\t fsp_var_val_11 timestamp(1) default '2021-11-11 15:59:59.1',\n"
            + "\t fsp_var_val_12 timestamp(1) default '2021-11-11 15:59:59.12',\n"
            + "\t fsp_var_val_20 timestamp(2) default '2021-11-11 15:59:59',\n"
            + "\t fsp_var_val_21 timestamp(2) default '2021-11-11 15:59:59.1',\n"
            + "\t fsp_var_val_22 timestamp(2) default '2021-11-11 15:59:59.12',\n"
            + "\t fsp_var_val_23 timestamp(2) default '2021-11-11 15:59:59.123',\n"
            + "\t fsp_var_val_30 timestamp(3) default '2021-11-11 15:59:59',\n"
            + "\t fsp_var_val_31 timestamp(3) default '2021-11-11 15:59:59.1',\n"
            + "\t fsp_var_val_33 timestamp(3) default '2021-11-11 15:59:59.123',\n"
            + "\t fsp_var_val_34 timestamp(3) default '2021-11-11 15:59:59.1234',\n"
            + "\t fsp_var_val_40 timestamp(4) default '2021-11-11 15:59:59',\n"
            + "\t fsp_var_val_41 timestamp(4) default '2021-11-11 15:59:59.1',\n"
            + "\t fsp_var_val_44 timestamp(4) default '2021-11-11 15:59:59.1234',\n"
            + "\t fsp_var_val_45 timestamp(4) default '2021-11-11 15:59:59.12345',\n"
            + "\t fsp_var_val_50 timestamp(5) default '2021-11-11 15:59:59',\n"
            + "\t fsp_var_val_51 timestamp(5) default '2021-11-11 15:59:59.1',\n"
            + "\t fsp_var_val_55 timestamp(5) default '2021-11-11 15:59:59.12345',\n"
            + "\t fsp_var_val_56 timestamp(5) default '2021-11-11 15:59:59.123456',\n"
            + "\t fsp_var_val_60 timestamp(6) default '2021-11-11 15:59:59',\n"
            + "\t fsp_var_val_61 timestamp(6) default '2021-11-11 15:59:59.1',\n"
            + "\t fsp_var_val_66 timestamp(6) default '2021-11-11 15:59:59.123456',\n"
            + "\t fsp_var_val_67 timestamp(6) default '2021-11-11 15:59:59.1234567'\n"
            + "\t)";

        String showCreateTableTemplate = "create table `%s` (\n"
            + "\t`id` int(11) DEFAULT NULL,\n"
            + "\t`fsp_def_val_0` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_def_val_1` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_def_val_2` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_def_val_3` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_def_val_4` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_def_val_5` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_def_val_6` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_zero_00` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\n"
            + "\t`fsp_var_zero_01` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\n"
            + "\t`fsp_var_zero_10` timestamp(1) NOT NULL DEFAULT '0000-00-00 00:00:00.0',\n"
            + "\t`fsp_var_zero_11` timestamp(1) NOT NULL DEFAULT '0000-00-00 00:00:00.0',\n"
            + "\t`fsp_var_zero_12` timestamp(1) NOT NULL DEFAULT '0000-00-00 00:00:00.0',\n"
            + "\t`fsp_var_zero_20` timestamp(2) NOT NULL DEFAULT '0000-00-00 00:00:00.00',\n"
            + "\t`fsp_var_zero_21` timestamp(2) NOT NULL DEFAULT '0000-00-00 00:00:00.00',\n"
            + "\t`fsp_var_zero_22` timestamp(2) NOT NULL DEFAULT '0000-00-00 00:00:00.00',\n"
            + "\t`fsp_var_zero_23` timestamp(2) NOT NULL DEFAULT '0000-00-00 00:00:00.00',\n"
            + "\t`fsp_var_zero_30` timestamp(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000',\n"
            + "\t`fsp_var_zero_31` timestamp(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000',\n"
            + "\t`fsp_var_zero_33` timestamp(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000',\n"
            + "\t`fsp_var_zero_34` timestamp(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000',\n"
            + "\t`fsp_var_zero_40` timestamp(4) NOT NULL DEFAULT '0000-00-00 00:00:00.0000',\n"
            + "\t`fsp_var_zero_41` timestamp(4) NOT NULL DEFAULT '0000-00-00 00:00:00.0000',\n"
            + "\t`fsp_var_zero_44` timestamp(4) NOT NULL DEFAULT '0000-00-00 00:00:00.0000',\n"
            + "\t`fsp_var_zero_45` timestamp(4) NOT NULL DEFAULT '0000-00-00 00:00:00.0000',\n"
            + "\t`fsp_var_zero_50` timestamp(5) NOT NULL DEFAULT '0000-00-00 00:00:00.00000',\n"
            + "\t`fsp_var_zero_51` timestamp(5) NOT NULL DEFAULT '0000-00-00 00:00:00.00000',\n"
            + "\t`fsp_var_zero_55` timestamp(5) NOT NULL DEFAULT '0000-00-00 00:00:00.00000',\n"
            + "\t`fsp_var_zero_56` timestamp(5) NOT NULL DEFAULT '0000-00-00 00:00:00.00000',\n"
            + "\t`fsp_var_zero_60` timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',\n"
            + "\t`fsp_var_zero_61` timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',\n"
            + "\t`fsp_var_zero_66` timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',\n"
            + "\t`fsp_var_zero_67` timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',\n"
            + "\t`fsp_var_val_00` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_01` timestamp NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_10` timestamp(1) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_11` timestamp(1) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_12` timestamp(1) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_20` timestamp(2) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_21` timestamp(2) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_22` timestamp(2) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_23` timestamp(2) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_30` timestamp(3) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_31` timestamp(3) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_33` timestamp(3) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_34` timestamp(3) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_40` timestamp(4) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_41` timestamp(4) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_44` timestamp(4) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_45` timestamp(4) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_50` timestamp(5) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_51` timestamp(5) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_55` timestamp(5) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_56` timestamp(5) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_60` timestamp(6) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_61` timestamp(6) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_66` timestamp(6) NOT NULL DEFAULT '%s',\n"
            + "\t`fsp_var_val_67` timestamp(6) NOT NULL DEFAULT '%s'\n";

        try {
            setTimeZone("+12:00");
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
            checkTableMeta(schemaName, simpleTableName, tableName,
                String.format(showCreateTableTemplate, simpleTableName,
                    "2021-12-12 12:00:00", "2021-12-12 12:00:00", "2021-12-12 12:00:00", "2021-12-12 12:00:00",
                    "2021-12-12 12:00:00", "2021-12-12 12:00:00", "2021-12-12 12:00:00",
                    "2021-11-11 15:59:59", "2021-11-11 15:59:59",
                    "2021-11-11 15:59:59.0", "2021-11-11 15:59:59.1", "2021-11-11 15:59:59.1",
                    "2021-11-11 15:59:59.00", "2021-11-11 15:59:59.10", "2021-11-11 15:59:59.12",
                    "2021-11-11 15:59:59.12",
                    "2021-11-11 15:59:59.000", "2021-11-11 15:59:59.100", "2021-11-11 15:59:59.123",
                    "2021-11-11 15:59:59.123",
                    "2021-11-11 15:59:59.0000", "2021-11-11 15:59:59.1000", "2021-11-11 15:59:59.1234",
                    "2021-11-11 15:59:59.1235",
                    "2021-11-11 15:59:59.00000", "2021-11-11 15:59:59.10000", "2021-11-11 15:59:59.12345",
                    "2021-11-11 15:59:59.12346",
                    "2021-11-11 15:59:59.000000", "2021-11-11 15:59:59.100000", "2021-11-11 15:59:59.123456",
                    "2021-11-11 15:59:59.123457"
                ),
                new String[][] {
                    new String[] {"fsp_def_val_0", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_1", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_2", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_3", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_4", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_5", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_6", "2021-12-12 08:00:00"},
                    new String[] {"fsp_var_zero_00", "0000-00-00 00:00:00"},
                    new String[] {"fsp_var_zero_01", "0000-00-00 00:00:00"},
                    new String[] {"fsp_var_zero_10", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_11", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_12", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_20", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_21", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_22", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_23", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_30", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_31", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_33", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_34", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_40", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_41", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_44", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_45", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_50", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_51", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_55", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_56", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_60", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_61", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_66", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_67", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_val_00", "2021-11-11 11:59:59"},
                    new String[] {"fsp_var_val_01", "2021-11-11 11:59:59"},
                    new String[] {"fsp_var_val_10", "2021-11-11 11:59:59.0"},
                    new String[] {"fsp_var_val_11", "2021-11-11 11:59:59.1"},
                    new String[] {"fsp_var_val_12", "2021-11-11 11:59:59.1"},
                    new String[] {"fsp_var_val_20", "2021-11-11 11:59:59.00"},
                    new String[] {"fsp_var_val_21", "2021-11-11 11:59:59.10"},
                    new String[] {"fsp_var_val_22", "2021-11-11 11:59:59.12"},
                    new String[] {"fsp_var_val_23", "2021-11-11 11:59:59.12"},
                    new String[] {"fsp_var_val_30", "2021-11-11 11:59:59.000"},
                    new String[] {"fsp_var_val_31", "2021-11-11 11:59:59.100"},
                    new String[] {"fsp_var_val_33", "2021-11-11 11:59:59.123"},
                    new String[] {"fsp_var_val_34", "2021-11-11 11:59:59.123"},
                    new String[] {"fsp_var_val_40", "2021-11-11 11:59:59.0000"},
                    new String[] {"fsp_var_val_41", "2021-11-11 11:59:59.1000"},
                    new String[] {"fsp_var_val_44", "2021-11-11 11:59:59.1234"},
                    new String[] {"fsp_var_val_45", "2021-11-11 11:59:59.1235"},
                    new String[] {"fsp_var_val_50", "2021-11-11 11:59:59.00000"},
                    new String[] {"fsp_var_val_51", "2021-11-11 11:59:59.10000"},
                    new String[] {"fsp_var_val_55", "2021-11-11 11:59:59.12345"},
                    new String[] {"fsp_var_val_56", "2021-11-11 11:59:59.12346"},
                    new String[] {"fsp_var_val_60", "2021-11-11 11:59:59.000000"},
                    new String[] {"fsp_var_val_61", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_66", "2021-11-11 11:59:59.123456"},
                    new String[] {"fsp_var_val_67", "2021-11-11 11:59:59.123457"}
                },
                new String[][] {
                    new String[] {"fsp_def_val_0", "2021-12-12 12:00:00"},
                    new String[] {"fsp_def_val_1", "2021-12-12 12:00:00"},
                    new String[] {"fsp_def_val_2", "2021-12-12 12:00:00"},
                    new String[] {"fsp_def_val_3", "2021-12-12 12:00:00"},
                    new String[] {"fsp_def_val_4", "2021-12-12 12:00:00"},
                    new String[] {"fsp_def_val_5", "2021-12-12 12:00:00"},
                    new String[] {"fsp_def_val_6", "2021-12-12 12:00:00"},
                    new String[] {"fsp_var_zero_00", "0000-00-00 00:00:00"},
                    new String[] {"fsp_var_zero_01", "0000-00-00 00:00:00"},
                    new String[] {"fsp_var_zero_10", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_11", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_12", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_20", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_21", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_22", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_23", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_30", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_31", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_33", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_34", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_40", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_41", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_44", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_45", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_50", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_51", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_55", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_56", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_60", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_61", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_66", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_67", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_val_00", "2021-11-11 15:59:59"},
                    new String[] {"fsp_var_val_01", "2021-11-11 15:59:59"},
                    new String[] {"fsp_var_val_10", "2021-11-11 15:59:59.000000"},
                    new String[] {"fsp_var_val_11", "2021-11-11 15:59:59.100000"},
                    new String[] {"fsp_var_val_12", "2021-11-11 15:59:59.100000"},
                    new String[] {"fsp_var_val_20", "2021-11-11 15:59:59.000000"},
                    new String[] {"fsp_var_val_21", "2021-11-11 15:59:59.100000"},
                    new String[] {"fsp_var_val_22", "2021-11-11 15:59:59.120000"},
                    new String[] {"fsp_var_val_23", "2021-11-11 15:59:59.120000"},
                    new String[] {"fsp_var_val_30", "2021-11-11 15:59:59.000000"},
                    new String[] {"fsp_var_val_31", "2021-11-11 15:59:59.100000"},
                    new String[] {"fsp_var_val_33", "2021-11-11 15:59:59.123000"},
                    new String[] {"fsp_var_val_34", "2021-11-11 15:59:59.123000"},
                    new String[] {"fsp_var_val_40", "2021-11-11 15:59:59.000000"},
                    new String[] {"fsp_var_val_41", "2021-11-11 15:59:59.100000"},
                    new String[] {"fsp_var_val_44", "2021-11-11 15:59:59.123400"},
                    new String[] {"fsp_var_val_45", "2021-11-11 15:59:59.123500"},
                    new String[] {"fsp_var_val_50", "2021-11-11 15:59:59.000000"},
                    new String[] {"fsp_var_val_51", "2021-11-11 15:59:59.100000"},
                    new String[] {"fsp_var_val_55", "2021-11-11 15:59:59.123450"},
                    new String[] {"fsp_var_val_56", "2021-11-11 15:59:59.123460"},
                    new String[] {"fsp_var_val_60", "2021-11-11 15:59:59.000000"},
                    new String[] {"fsp_var_val_61", "2021-11-11 15:59:59.100000"},
                    new String[] {"fsp_var_val_66", "2021-11-11 15:59:59.123456"},
                    new String[] {"fsp_var_val_67", "2021-11-11 15:59:59.123457"}
                }
            );

            setTimeZone("+8:00");
            checkTableMeta(schemaName, simpleTableName, tableName,
                String.format(showCreateTableTemplate, simpleTableName,
                    "2021-12-12 08:00:00", "2021-12-12 08:00:00", "2021-12-12 08:00:00", "2021-12-12 08:00:00",
                    "2021-12-12 08:00:00", "2021-12-12 08:00:00", "2021-12-12 08:00:00",
                    "2021-11-11 11:59:59", "2021-11-11 11:59:59",
                    "2021-11-11 11:59:59.0", "2021-11-11 11:59:59.1", "2021-11-11 11:59:59.1",
                    "2021-11-11 11:59:59.00", "2021-11-11 11:59:59.10", "2021-11-11 11:59:59.12",
                    "2021-11-11 11:59:59.12",
                    "2021-11-11 11:59:59.000", "2021-11-11 11:59:59.100", "2021-11-11 11:59:59.123",
                    "2021-11-11 11:59:59.123",
                    "2021-11-11 11:59:59.0000", "2021-11-11 11:59:59.1000", "2021-11-11 11:59:59.1234",
                    "2021-11-11 11:59:59.1235",
                    "2021-11-11 11:59:59.00000", "2021-11-11 11:59:59.10000", "2021-11-11 11:59:59.12345",
                    "2021-11-11 11:59:59.12346",
                    "2021-11-11 11:59:59.000000", "2021-11-11 11:59:59.100000", "2021-11-11 11:59:59.123456",
                    "2021-11-11 11:59:59.123457"
                ),
                new String[][] {
                    new String[] {"fsp_def_val_0", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_1", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_2", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_3", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_4", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_5", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_6", "2021-12-12 08:00:00"},
                    new String[] {"fsp_var_zero_00", "0000-00-00 00:00:00"},
                    new String[] {"fsp_var_zero_01", "0000-00-00 00:00:00"},
                    new String[] {"fsp_var_zero_10", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_11", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_12", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_20", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_21", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_22", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_23", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_30", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_31", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_33", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_34", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_40", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_41", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_44", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_45", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_50", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_51", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_55", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_56", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_60", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_61", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_66", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_67", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_val_00", "2021-11-11 11:59:59"},
                    new String[] {"fsp_var_val_01", "2021-11-11 11:59:59"},
                    new String[] {"fsp_var_val_10", "2021-11-11 11:59:59.0"},
                    new String[] {"fsp_var_val_11", "2021-11-11 11:59:59.1"},
                    new String[] {"fsp_var_val_12", "2021-11-11 11:59:59.1"},
                    new String[] {"fsp_var_val_20", "2021-11-11 11:59:59.00"},
                    new String[] {"fsp_var_val_21", "2021-11-11 11:59:59.10"},
                    new String[] {"fsp_var_val_22", "2021-11-11 11:59:59.12"},
                    new String[] {"fsp_var_val_23", "2021-11-11 11:59:59.12"},
                    new String[] {"fsp_var_val_30", "2021-11-11 11:59:59.000"},
                    new String[] {"fsp_var_val_31", "2021-11-11 11:59:59.100"},
                    new String[] {"fsp_var_val_33", "2021-11-11 11:59:59.123"},
                    new String[] {"fsp_var_val_34", "2021-11-11 11:59:59.123"},
                    new String[] {"fsp_var_val_40", "2021-11-11 11:59:59.0000"},
                    new String[] {"fsp_var_val_41", "2021-11-11 11:59:59.1000"},
                    new String[] {"fsp_var_val_44", "2021-11-11 11:59:59.1234"},
                    new String[] {"fsp_var_val_45", "2021-11-11 11:59:59.1235"},
                    new String[] {"fsp_var_val_50", "2021-11-11 11:59:59.00000"},
                    new String[] {"fsp_var_val_51", "2021-11-11 11:59:59.10000"},
                    new String[] {"fsp_var_val_55", "2021-11-11 11:59:59.12345"},
                    new String[] {"fsp_var_val_56", "2021-11-11 11:59:59.12346"},
                    new String[] {"fsp_var_val_60", "2021-11-11 11:59:59.000000"},
                    new String[] {"fsp_var_val_61", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_66", "2021-11-11 11:59:59.123456"},
                    new String[] {"fsp_var_val_67", "2021-11-11 11:59:59.123457"}
                },
                new String[][] {
                    new String[] {"fsp_def_val_0", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_1", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_2", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_3", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_4", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_5", "2021-12-12 08:00:00"},
                    new String[] {"fsp_def_val_6", "2021-12-12 08:00:00"},
                    new String[] {"fsp_var_zero_00", "0000-00-00 00:00:00"},
                    new String[] {"fsp_var_zero_01", "0000-00-00 00:00:00"},
                    new String[] {"fsp_var_zero_10", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_11", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_12", "0000-00-00 00:00:00.0"},
                    new String[] {"fsp_var_zero_20", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_21", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_22", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_23", "0000-00-00 00:00:00.00"},
                    new String[] {"fsp_var_zero_30", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_31", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_33", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_34", "0000-00-00 00:00:00.000"},
                    new String[] {"fsp_var_zero_40", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_41", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_44", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_45", "0000-00-00 00:00:00.0000"},
                    new String[] {"fsp_var_zero_50", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_51", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_55", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_56", "0000-00-00 00:00:00.00000"},
                    new String[] {"fsp_var_zero_60", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_61", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_66", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_zero_67", "0000-00-00 00:00:00.000000"},
                    new String[] {"fsp_var_val_00", "2021-11-11 11:59:59"},
                    new String[] {"fsp_var_val_01", "2021-11-11 11:59:59"},
                    new String[] {"fsp_var_val_10", "2021-11-11 11:59:59.000000"},
                    new String[] {"fsp_var_val_11", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_12", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_20", "2021-11-11 11:59:59.000000"},
                    new String[] {"fsp_var_val_21", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_22", "2021-11-11 11:59:59.120000"},
                    new String[] {"fsp_var_val_23", "2021-11-11 11:59:59.120000"},
                    new String[] {"fsp_var_val_30", "2021-11-11 11:59:59.000000"},
                    new String[] {"fsp_var_val_31", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_33", "2021-11-11 11:59:59.123000"},
                    new String[] {"fsp_var_val_34", "2021-11-11 11:59:59.123000"},
                    new String[] {"fsp_var_val_40", "2021-11-11 11:59:59.000000"},
                    new String[] {"fsp_var_val_41", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_44", "2021-11-11 11:59:59.123400"},
                    new String[] {"fsp_var_val_45", "2021-11-11 11:59:59.123500"},
                    new String[] {"fsp_var_val_50", "2021-11-11 11:59:59.000000"},
                    new String[] {"fsp_var_val_51", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_55", "2021-11-11 11:59:59.123450"},
                    new String[] {"fsp_var_val_56", "2021-11-11 11:59:59.123460"},
                    new String[] {"fsp_var_val_60", "2021-11-11 11:59:59.000000"},
                    new String[] {"fsp_var_val_61", "2021-11-11 11:59:59.100000"},
                    new String[] {"fsp_var_val_66", "2021-11-11 11:59:59.123456"},
                    new String[] {"fsp_var_val_67", "2021-11-11 11:59:59.123457"}
                }
            );
        } finally {
            // Reset to default time zone to avoid affecting other cases.
            setTimeZone("+8:00");
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testHintIgnoreErrorCode() throws SQLException {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String simpleTableName = "test_hint_ignore_error_code";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);

        //given: a partitioned table
        String createSql = "create table %s (id int not null, name char(32), item varchar(32), primary key (id)) " +
            "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 12";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createSql, tableName));

        List<org.apache.calcite.util.Pair<String, String>> topoList = showTopologyWithGroup(tddlConnection, tableName);

        //when: one of the physical table already have index i1
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "/*+TDDL:node='%s'*/alter table %s add index i1 (id)", topoList.get(0).getKey(), topoList.get(0).getValue()
        ));

        //then: add index i1 to local table will cause error
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("alter table %s add index i1 (id)", tableName), "Duplicate key name");

        //then: with hint 'PHYSICAL_DDL_IGNORED_ERROR_CODE', add index i1 to local table will be success
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("/*+TDDL:cmd_extra(PHYSICAL_DDL_IGNORED_ERROR_CODE='1061')*/" +
                "alter table %s add index i1 (id)", tableName));
    }

    private void checkTableMeta(String schemaName, String simpleTableName, String fullTableName, String expectedShow,
                                String[][] expectedMeta, String[][] expectedInfoSchema) throws SQLException {
        checkShowCreateTable(fullTableName, expectedShow);
        checkColumnsInMetaDb(schemaName, simpleTableName, expectedMeta);
        checkInfoSchemaColumns(schemaName, simpleTableName, expectedInfoSchema);
    }

    private void checkShowCreateTable(String fullTableName, String expectedShow) throws SQLException {
        String sql = "show create table " + fullTableName;
        String result = null;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                result = rs.getString(2);
            }
        }
        if (isMySQL80()) {
            result = result.replace(" DEFAULT COLLATE = utf8mb4_0900_ai_ci", "");
        }
        Assert.assertTrue(compareShowCreateTable(result, expectedShow));
    }

    private boolean compareShowCreateTable(String actual, String expected) {
        if (TStringUtil.isBlank(actual) || TStringUtil.isBlank(expected)) {
            log.error("Actual or expected result of show create table is empty");
            return false;
        }
        String origActual = actual, origExpected = expected;
        if (!TStringUtil.containsIgnoreCase(replaceUnnecessaryChars(actual), replaceUnnecessaryChars(expected))) {
            printShowCreateTable(origActual, origExpected);
            return false;
        }
        return true;
    }

    private void printShowCreateTable(String actual, String expected) {
        StringBuilder msg = new StringBuilder();
        msg.append("Actual Show Create Table:").append("\n");
        msg.append(actual).append("\n");
        msg.append("Expected Show Create Table:").append("\n");
        msg.append(expected);
        log.error(msg);
    }

    private String replaceUnnecessaryChars(String str) {
        str = TStringUtil.replace(str, "\n", "");
        str = TStringUtil.replace(str, "\t", "");
        str = TStringUtil.replace(str, " ", "");
        return str;
    }

    private void checkColumnsInMetaDb(String schemaName, String simpleTableName, String[][] expectedResult)
        throws SQLException {
        String sql = String.format(
            "select column_name,column_default from columns "
                + "where table_schema='%s' and table_name='%s' and data_type='timestamp' "
                + "order by ordinal_position",
            schemaName, simpleTableName);
        List<Pair<String, String>> columnDefaults = new ArrayList<>();
        try (Statement stmt = getMetaConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                columnDefaults.add(new Pair<>(rs.getString(1), rs.getString(2)));
            }
        }
        Assert.assertTrue(compareColumnDefaults(columnDefaults, expectedResult));
    }

    private void checkInfoSchemaColumns(String schemaName, String simpleTableName, String[][] expectedResult)
        throws SQLException {
        String sql = String.format(
            "select column_name,column_default from information_schema.columns "
                + "where table_schema='%s' and table_name='%s' and data_type='timestamp' "
                + "order by ordinal_position",
            schemaName, simpleTableName);
        List<Pair<String, String>> columnDefaults = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                columnDefaults.add(new Pair<>(rs.getString(1), rs.getString(2)));
            }
        }
        Assert.assertTrue(compareColumnDefaults(columnDefaults, expectedResult));
    }

    private boolean compareColumnDefaults(List<Pair<String, String>> actual, String[][] expected) {
        if (actual == null || actual.isEmpty() || expected == null || expected.length == 0) {
            log.error("Actual or expected column defaults is empty");
            return false;
        }
        if (actual.size() != expected.length) {
            printColumnDefaults(actual, expected);
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            if (!TStringUtil.equalsIgnoreCase(actual.get(i).getKey(), expected[i][0]) ||
                !TStringUtil.equalsIgnoreCase(actual.get(i).getValue(), expected[i][1])) {
                printColumnDefaults(actual, expected);
                return false;
            }
        }
        return true;
    }

    private void printColumnDefaults(List<Pair<String, String>> actual, String[][] expected) {
        StringBuilder actualMsg = new StringBuilder();
        actualMsg.append("Actual Column Defaults:").append("\n");
        for (Pair<String, String> row : actual) {
            actualMsg.append(row.getKey()).append(", ").append(row.getValue()).append("\n");
        }
        log.error(actualMsg);

        StringBuilder expectedMsg = new StringBuilder();
        expectedMsg.append("Expected Column Defaults:").append("\n");
        for (String[] row : expected) {
            expectedMsg.append(row[0]).append(", ").append(row[1]).append("\n");
        }
        log.error(expectedMsg);
    }

    private void checkQueryResult(String fullTableName, boolean afterAlterTable, String[][] expectedResult)
        throws SQLException {
        String sql =
            "select c_timestamp_0, c_timestamp_1, c_timestamp_2, c_timestamp_3, pad from %s order by pad";
        if (afterAlterTable) {
            sql = "select c_timestamp_a, c_timestamp_0, c_timestamp_c, c_timestamp_2, c_timestamp_3, pad "
                + "from %s order by pad";
        }
        List<List<String>> result = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, fullTableName))) {
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                row.add(rs.getString(1));
                row.add(rs.getString(2));
                row.add(rs.getString(3));
                row.add(rs.getString(4));
                row.add(rs.getString(5));
                if (afterAlterTable) {
                    row.add(rs.getString(6));
                }
                result.add(row);
            }
        }
        Assert.assertTrue(compareQueryResult(result, expectedResult));
    }

    private boolean compareQueryResult(List<List<String>> actual, String[][] expected) {
        if (actual == null || actual.isEmpty() || expected == null || expected.length == 0) {
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            List<String> rowActual = actual.get(i);
            String[] rowExpected = expected[i];
            for (int j = 0; j < rowExpected.length; j++) {
                if (!TStringUtil.equalsIgnoreCase(rowActual.get(j), rowExpected[j])) {
                    printQueryResult(actual, expected);
                    return false;
                }
            }
        }
        return true;
    }

    private void printQueryResult(List<List<String>> actual, String[][] expected) {
        StringBuilder actualMsg = new StringBuilder();
        actualMsg.append("Actual Query Result:").append("\n");
        for (List<String> row : actual) {
            for (String column : row) {
                actualMsg.append(column).append(", ");
            }
            actualMsg.append("\n");
        }
        log.error(actualMsg);

        StringBuilder expectedMsg = new StringBuilder();
        expectedMsg.append("Expected Query Result:").append("\n");
        for (String[] row : expected) {
            for (String column : row) {
                expectedMsg.append(column).append(", ");
            }
            expectedMsg.append("\n");
        }
        log.error(expectedMsg);
    }

    private void setTimeZone(String gmtTime) {
        String sql = "set time_zone='%s'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, gmtTime));
    }

    private void enableSetGlobalSession() {
        String sql = "set enable_set_global=true";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void setGlobalSupportInstantAddColumn(boolean supported) {
        enableSetGlobalSession();
        String sql = "set global support_instant_add_column=%s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, supported ? "on" : "off"));
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }
    }

    private boolean checkInstantAddColumnVariables(boolean supported) throws SQLException {
        boolean allExpected = true;

        String debugInfo = "IAC(%s): %s for %s from %s";

        String queryInstConfig = String.format("select param_key, param_val from inst_config where param_key='%s'",
            ConnectionProperties.SUPPORT_INSTANT_ADD_COLUMN);
        allExpected &= isVariableExpected(queryInstConfig, supported ? "true" : "false");

        log.info(String.format(debugInfo, tddlDatabase1, allExpected, supported, queryInstConfig));

        String showVariables = String.format("show variables like '%s'", Attribute.XDB_VARIABLE_INSTANT_ADD_COLUMN);
        allExpected &= isVariableExpected(showVariables, supported ? "ON" : "OFF");

        log.info(String.format(debugInfo, tddlDatabase1, allExpected, supported, showVariables));

        return allExpected;
    }

    private boolean isVariableExpected(String sql, String expected) throws SQLException {
        try (Connection conn = getMetaConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return expected.equalsIgnoreCase(rs.getString(2));
            }
        }
        log.error(String.format("IAC(%s): no result from %s", tddlDatabase1, sql));
        return false;
    }

    private void checkVirtualStatistics(String expectedTableName, String[] expectedColumns) throws SQLException {
        Connection conn = TStringUtil.isBlank(tddlDatabase2) ? tddlConnection : getTddlConnection2();
        String sql = String.format(
            "select table_name, column_name from virtual_statistic where table_name = '%s' order by column_name",
            expectedTableName);

        List<String> actualColumns = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                actualColumns.add(rs.getString(2));
            }
        }

        if (expectedColumns == null || expectedColumns.length == 0) {
            if (actualColumns.isEmpty()) {
                return;
            } else {
                Assert.fail("There are still obsolete statistics for " + expectedTableName);
            }
        } else {
            if (actualColumns.isEmpty()) {
                Assert.fail("Missed statistics for " + expectedTableName);
            } else {
                if (expectedColumns.length != actualColumns.size()) {
                    printStatistics(expectedTableName, expectedColumns, actualColumns);
                    Assert.fail("Inconsistent number of columns in statistics for " + expectedTableName);
                } else {
                    for (int i = 0; i < expectedColumns.length; i++) {
                        if (!TStringUtil.equalsIgnoreCase(expectedColumns[i], actualColumns.get(i))) {
                            printStatistics(expectedTableName, expectedColumns, actualColumns);
                            Assert.fail("Inconsistent column names in statistics for " + expectedTableName);
                        }
                    }
                }
            }
        }
    }

    private void printStatistics(String expectedTableName, String[] expectedColumns, List<String> actualColumns) {
        log.error("Table Statistics for " + expectedTableName + ":\n");
        StringBuilder buf = new StringBuilder();
        buf.append("Expected Columns: ");
        Arrays.stream(expectedColumns).forEach(c -> buf.append(c).append("\n"));
        buf.append("Actual Columns: ");
        actualColumns.stream().forEach(c -> buf.append(c).append("\n"));
        log.error(buf);
    }

    @Test
    public void testAlterColumnBinaryDefaultValue() {
        String tableName = "test_bin_col_default_val_primary";
        String gsiName = "test_bin_col_default_val_gsi";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTable = String.format("create table %s ("
            + "`pk` int primary key auto_increment, "
            + "`char_col` varchar(20) default 'ggg', "
            + "`char_col_1` varchar(20) default 'ggg', "
            + "`pad` varchar(20) default 'ggg' "
            + ")", tableName);
        String partitionDef = " dbpartition by hash(`pk`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String createGsi =
            String.format(
                "create global index %s on %s(`char_col`) covering(`char_col_1`) dbpartition by hash(`char_col`)",
                gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        // Use upsert to fill all default values on CN
        String insert = String.format("insert into %s(`pk`) values (null) on duplicate key update pad=null", tableName);
        String select = String.format("select `char_col`,`char_col_1` from %s", tableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Set default, non-binary to binary
        String alter = String.format("alter table %s alter column `char_col` set default x'686868'", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Set default, binary to non-binary
        alter = String.format("alter table %s alter column `char_col` set default 'iii'", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Drop default, then add binary
        alter = String.format("alter table %s alter column `char_col` drop default", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        alter =
            String.format("alter table %s alter column `char_col` set default b'11010100110101001101010'", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Drop default, then add non-binary
        alter = String.format("alter table %s alter column `char_col` drop default", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        alter = String.format("alter table %s alter column `char_col` set default 'kkk'", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Modify column, non-binary to binary
        alter = ALLOW_ALTER_GSI_INDIRECTLY_HINT + String.format(
            "alter table %s modify column `char_col_1` varchar(10) default x'6c6c6c'", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Modify column, binary to non-binary
        alter = ALLOW_ALTER_GSI_INDIRECTLY_HINT + String.format(
            "alter table %s modify column `char_col_1` varchar(10) default 'mmm'", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Change column, non-binary to binary
        alter = ALLOW_ALTER_GSI_INDIRECTLY_HINT + String.format(
            "alter table %s change column `char_col_1` `char_col_2` varchar(10) default x'6e6e6e'", tableName);
        select = String.format("select `char_col`,`char_col_2` from %s", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Change column, binary to non-binary
        alter = ALLOW_ALTER_GSI_INDIRECTLY_HINT + String.format(
            "alter table %s change column `char_col_2` `char_col_3` varchar(10) default 'ooo'", tableName);
        select = String.format("select `char_col`,`char_col_3` from %s", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Add column, binary
        alter = ALLOW_ALTER_GSI_INDIRECTLY_HINT + String.format(
            "alter table %s add column `char_col_4` varchar(10) default x'707070'", tableName);
        select = String.format("select `char_col`,`char_col_3`,`char_col_4` from %s", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // Add column, non-binary
        alter = ALLOW_ALTER_GSI_INDIRECTLY_HINT + String.format(
            "alter table %s add column `char_col_5` varchar(10) default 'qqq'", tableName);
        select = String.format("select `char_col`,`char_col_3`,`char_col_4`,`char_col_5` from %s", tableName);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testRepartitionWithGsiBinaryDefaultValue() throws Exception {
        String tableName = "test_bin_col_default_val_primary_1";
        String gsiName = "test_bin_col_default_val_gsi_1";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTable = String.format("create table %s ("
            + "`pk` int primary key auto_increment, "
            + "`bin_col` varbinary(20) default x'0A08080E10011894AB0E', "
            + "`pad` varchar(20) default 'ggg' "
            + ")", tableName);
        String partitionDef = " dbpartition by hash(`pk`)";
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String createGsi =
            String.format("create global index %s on %s(`pk`) dbpartition by hash(`pk`)", gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        // Use upsert to test default value on CN
        String upsert = String.format("insert into %s(`pk`) values (null) on duplicate key update pad=null", tableName);
        // Use insert to test default value on DN
        String insert = String.format("insert into %s(`pk`) values (null)", tableName);
        String select = String.format("select `bin_col` from %s", tableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        String newPartitionDef = " dbpartition by hash(`pk`) tbpartition by hash(`pk`) tbpartitions 3";
        String alter = String.format("alter table %s %s", tableName, newPartitionDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
    }

}

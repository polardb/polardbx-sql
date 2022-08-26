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

package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * 建表时, 用各种sequence建表, 同时执行插入, 并判断插入的结果是否正确
 * <p>
 * 如果是单值插入，返回的肯定是刚插入的值；
 * 如果是多值/批量插入，有两种情况：
 * 1. 如果是insert into xxx values (),(),... 这种insert多值的情况，select last_insert_id()返回的是这一批次的最小值；
 * 2. 如果是insert select的话，select last_insert_id()返回的是这一批次的最大值
 */
@NotThreadSafe

public class CreateTableWithNewSequenceTest extends BaseSequenceTestCase {
    private Log log = LogFactory.getLog(CreateTableWithNewSequenceTest.class);

    private String sqlPostFix;
    private String seqType;

    @Parameterized.Parameters(name = "{index}:seqType={0}, sqlpostfix={1}, schema={2}")
    public static List<String[]> prepareData() {
        String[][] postFix = {
            {"", "", ""},
            {"", "dbpartition by hash(id)", ""},
            {"", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
            {"", "tbpartition by hash(id) tbpartitions 2", ""},
            {"", "broadcast", ""},
            {"by simple", "dbpartition by hash(id)", ""},
            {"by simple", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
            {"by simple", "tbpartition by hash(id) tbpartitions 2", ""},
            {"by simple", "broadcast", ""},

            // simple with cache is unused !
//            {"by simple with cache", "dbpartition by hash(id)", ""},
//            {"by simple with cache", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
//            {"by simple with cache", "tbpartition by hash(id) tbpartitions 2", ""},
//            {"by simple with cache", "broadcast", ""},

            {"by group", "dbpartition by hash(id)", ""},
            {"by group", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
            {"by group", "tbpartition by hash(id) tbpartitions 2", ""},
            {"by group", "broadcast", ""},
            {"by time", "dbpartition by hash(id)", ""},
            {"by time", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
            {"by time", "tbpartition by hash(id) tbpartitions 2", ""},
            {"by time", "broadcast", ""},
            {"", "", PropertiesUtil.polardbXShardingDBName2()},
            {"", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {
                "", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
                PropertiesUtil.polardbXShardingDBName2()},
            {"", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            {"", "broadcast", PropertiesUtil.polardbXShardingDBName2()},
            {"by simple", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {
                "by simple", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
                PropertiesUtil.polardbXShardingDBName2()},
            {"by simple", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            {"by simple", "broadcast", PropertiesUtil.polardbXShardingDBName2()},
            //{"by simple with cache", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            //{"by simple with cache", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",PropertiesUtil.polardbXShardingDBName2()},
            //{"by simple with cache", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            //{"by simple with cache", "broadcast", PropertiesUtil.polardbXShardingDBName2()},
            {"by group", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {
                "by group", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
                PropertiesUtil.polardbXShardingDBName2()},
            {"by group", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            {"by group", "broadcast", PropertiesUtil.polardbXShardingDBName2()},
            {"by time", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {
                "by time", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
                PropertiesUtil.polardbXShardingDBName2()},
            {"by time", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            {"by time", "broadcast", PropertiesUtil.polardbXShardingDBName2()}};
        // String[][] postFix = {{""}};
        return Arrays.asList(postFix);
    }

    public CreateTableWithNewSequenceTest(String seqType, String sqlPostFix, String schema) {
        this.seqType = seqType;
        this.sqlPostFix = sqlPostFix;
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
    }

    /**
     * @since 5.1.24testBatchInsert2
     */
    @Test
    public void testCreateTableNormal() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_3";
        String seqName = schemaPrefix + "AUTO_SEQ_" + "sequence_ddl_test_3";
        dropTableIfExists(tableName);

        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment  %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name) values ('one'), ('two')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 指定值与隐式值
        sql = "insert into " + tableName + " (id, name) values (null, 'one'), (null, 'two'),(0, 'one'), (0, 'two')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 显示指定值与隐式值结合
        sql = "insert into " + tableName + " (id, name) values (100, '100'), (null, 'two'),(0, 'one')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        //判断show create table
        assertThat(showCreateTable(tddlConnection, tableName)).contains(seqType.toUpperCase());
        if (!sqlPostFix.isEmpty()) {
            //确认last insert id是正确的,
//            if(seqType.contains("time") || seqType.contains("group")){
//                assertLastInsertIdSame(tableName, "ID");
//            }else{
//                assertThat(getLastInsertId(polarDbXConnection)).isEqualTo(100);
//                assertThat(getIdentity(polarDbXConnection)).isEqualTo(100);
//            }
            simpleCheckSequence(seqName, seqType);
        }

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).contains(100L);
            assertThat(ids).containsNoneOf(101L, 102L);
        } else {
            assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L, 6L, 100L, 101L, 102L);
        }

        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateTableAutoIncrementNot1() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_2";
        String seqName = schemaPrefix + "AUTO_SEQ_" + "sequence_ddl_test_2";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) auto_increment=40 %s",
            tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name) values ('one'), ('two')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (null, 'one'), (null, 'two'), (0, 'one'), (0, 'two')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 显示指定值与隐式值结合
        sql = "insert into " + tableName + " (id, name) values (100, '100'), (null, 'two'),(0, 'one')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).contains(100L);
            assertThat(ids).containsNoneOf(40L, 41L, 42L, 43L, 44L, 45L, 101L, 102L);
        } else {
            assertThat(ids).containsAtLeast(40L, 41L, 42L, 43L, 44L, 45L, 100L, 101L, 102L).inOrder();
        }

        if (!sqlPostFix.isEmpty()) {
//            assertLastInsertIdSame(tableName, "ID");
            simpleCheckSequence(seqName, seqType);
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testBatchInsert() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_2";
        String seqName = schemaPrefix + "AUTO_SEQ_" + "sequence_ddl_test_2";
        dropTableIfExists(tableName);

        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s ",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 批量插入可以正常增长
        sql = "insert into " + tableName
            + " (id, name) values (1, 'one'),(2, 'two'), (null, 'three'), (4, 'four'), (0, 'five')";
        final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "id");
        assertThat(ps.executeUpdate()).isEqualTo(5);

        final ResultSet rs = ps.getGeneratedKeys();
        final List<Long> autoGenKeys = new ArrayList<>();
        try {
            while (rs.next()) {
                autoGenKeys.add(rs.getLong(1));
            }
        } finally {
            rs.close();
        }

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(1L, 2L, 4L);
            assertThat(ids).containsNoneOf(3L, 5L);
        } else {
            assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L).inOrder();
            assertThat(autoGenKeys).containsAtLeast(3L, 4L, 5L, 6L, 7L).inOrder();
        }

        if (!sqlPostFix.isEmpty()) {
            simpleCheckSequence(seqName, seqType);
        }
//        Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 1, 2, 3, 4, 5 }));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testBatchInsert2() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_2";
        dropTableIfExists(tableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 批量插入可以正常增长
        sql = "insert into " + tableName
            + " (id, name) values (null, 'one'),(0, 'two'), (null, 'three'), (4, 'four'), (5, 'five')";
        final PreparedStatement ps = JdbcUtil.preparedStatementAutoGen(sql, null, tddlConnection, "id");
        assertThat(ps.executeUpdate()).isEqualTo(5);

        final ResultSet rs = ps.getGeneratedKeys();
        final List<Long> autoGenKeys = new ArrayList<>();
        try {
            while (rs.next()) {
                autoGenKeys.add(rs.getLong(1));
            }
        } finally {
            rs.close();
        }

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(4L, 5L);
            assertThat(ids).containsNoneOf(1L, 2L, 3L);
        } else {
            assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L).inOrder();
            assert ids != null;
            assertThat(autoGenKeys).containsAtLeastElementsIn(ids).inOrder();
        }
//        Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 1, 2, 3, 4, 5 }));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testBatchInsert3() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_2";
        dropTableIfExists(tableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 批量插入可以正常增长
        sql = "insert into " + tableName
            + " (id, name) values (1, 'one'),(2, 'two'), (3, 'three'), (null, 'four'), (0, 'five')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(1L, 2L, 3L);
            assertThat(ids).containsNoneOf(4L, 5L);
        } else {
            assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L).inOrder();
        }

//        Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 1, 2, 3, 4, 5 }));
        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testJDBCBatchInsert() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_6";
        PreparedStatement tddlPrepare = null;
        dropTableIfExists(tableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {

            sql = "insert into " + tableName + " (name) values (?)";
            tddlPrepare = tddlConnection.prepareStatement(sql);

            for (int i = 1; i <= 3; i++) {
                tddlPrepare.setObject(1, "jdbc" + i);
                tddlPrepare.addBatch();
            }
            tddlPrepare.executeBatch();

            sql = "insert into " + tableName + " (id, name) values (?, ?)";
            tddlPrepare = tddlConnection.prepareStatement(sql);
            for (int i = 4; i <= 6; i++) {
                tddlPrepare.setObject(1, 0);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }
            for (int i = 7; i <= 10; i++) {
                tddlPrepare.setObject(1, null);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }

            for (int i = 11; i <= 13; i++) {
                tddlPrepare.setObject(1, i);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }
            for (int i = 14; i <= 16; i++) {
                tddlPrepare.setObject(1, null);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }

            tddlPrepare.executeBatch();

            sql = "select ID from " + tableName + " order by ID";
            List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
            if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
                assertThat(ids).containsAtLeast(11L, 12L, 13L);
                assertThat(ids).containsNoneOf(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 14L, 15L, 16L);
            } else {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L)
                    .inOrder();
            }

            dropTableIfExists(tableName);
        } finally {
            tddlPrepare.close();
        }
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testJDBCBatchInsert2() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_6";
        PreparedStatement tddlPrepare = null;
        dropTableIfExists(tableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {

            sql = "insert into " + tableName + " (id, name) values (?, ?)";
            tddlPrepare = tddlConnection.prepareStatement(sql);
            tddlPrepare.setObject(1, 0);
            tddlPrepare.setObject(2, "jdbc" + 0);
            tddlPrepare.addBatch();

            for (int i = 6; i < 10; i++) {
                tddlPrepare.setObject(1, i);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }

            tddlPrepare.executeBatch();

            sql = "select ID from " + tableName + " order by ID";
            List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
            if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
                assertThat(ids).containsAtLeast(6L, 7L, 8L, 9L);
                assertThat(ids).doesNotContain(1L);
            } else {
                assertThat(ids).containsAtLeast(1L, 6L, 7L, 8L, 9L).inOrder();
            }
//            Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 1, 6, 7, 8, 9 }));
            dropTableIfExists(tableName);
        } finally {
            tddlPrepare.close();
        }
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testJDBCBatchInsert3() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_6";
        PreparedStatement tddlPrepare = null;
        dropTableIfExists(tableName);
        String sql = String
            .format(" create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {

            sql = "insert into " + tableName + " (id, name) values (?, ?)";
            tddlPrepare = tddlConnection.prepareStatement(sql);

            for (int i = 6; i < 10; i++) {
                tddlPrepare.setObject(1, i);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }

            for (int i = 0; i < 3; i++) {
                tddlPrepare.setObject(1, 0);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }

            for (int i = 0; i < 3; i++) {
                tddlPrepare.setObject(1, null);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }

            tddlPrepare.executeBatch();

            sql = "select ID from " + tableName + " order by ID";
            List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
            if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
                assertThat(ids).containsAtLeast(6L, 7L, 8L, 9L);
                assertThat(ids).containsNoneOf(10L, 11L, 12L, 13L, 14L, 15L);
            } else {
                assertThat(ids).containsAtLeast(6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L).inOrder();
            }
//            Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }));
            dropTableIfExists(tableName);
        } finally {
            tddlPrepare.close();
        }
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testJDBCBatchInsert4() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_6";
        PreparedStatement tddlPrepare = null;
        dropTableIfExists(tableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {

            sql = "insert into " + tableName + " (name) values (?)";
            tddlPrepare = tddlConnection.prepareStatement(sql);

            for (int i = 1; i <= 3; i++) {
                tddlPrepare.setObject(1, "jdbc" + i);
                tddlPrepare.addBatch();
            }
            tddlPrepare.executeBatch();

            sql = "insert into " + tableName + " (id, name) values (?, ?)";
            tddlPrepare = tddlConnection.prepareStatement(sql);
            for (int i = 4; i <= 6; i++) {
                tddlPrepare.setObject(1, 0);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }
            for (int i = 7; i <= 10; i++) {
                tddlPrepare.setObject(1, null);
                tddlPrepare.setObject(2, "jdbc" + i);
                tddlPrepare.addBatch();
            }

            tddlPrepare.executeBatch();

            sql = "select ID from " + tableName + " order by ID";
            List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
            if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
                assertThat(ids).containsNoneOf(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
            } else {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L).inOrder();
            }
//            Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }));

            dropTableIfExists(tableName);
        } finally {
            tddlPrepare.close();
        }
    }

    /**
     * @since 5.1.24
     */
//    @Ignore("预期结果不确定,暂时忽略")
    @Test
    public void testBatchInsertFailed() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_3";
        dropTableIfExists(tableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 批量插入因为值重复而失败
        sql = "insert into " + tableName
            + " (id, name) values (100, 'one'),(101, 'two'), (null, 'three'), (100, 'four')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate");

        sql = "insert into "
            + tableName
            + " (name) values ('one'), ('two'),('three'),('four'),('five'),('six'),('seven'), ('eight'),('nine'),('ten')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids)
                .containsNoneOf(103L, 104L, 105L, 106L, 107L, 108L, 109L, 110L, 111L, 112L, 113L, 114L, 115L);
        } else {
            if (sqlPostFix.equals("")) {
                assertThat(ids).containsAtLeast(106L, 107L, 108L, 109L, 110L, 111L, 112L, 113L, 114L, 115L).inOrder();
            } else {
                assertThat(ids).containsAtLeast(103L, 104L, 105L, 106L, 107L, 108L, 109L, 110L, 111L, 112L).inOrder();
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testInsertSelect() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_4";
        String srcTableName = schemaPrefix + "src_table";
        dropTableIfExists(tableName);
        dropTableIfExists(srcTableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create table %s (id int , name varchar(20))", srcTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 源表中插入四条数据
        sql = "insert into " + srcTableName + " (id, name) values ( 0, '1'), (null, '2'), (0, '3'), (null, '4')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name)  select name from " + srcTableName + " order by name";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name)  select id, name from " + srcTableName + " order by name";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name)  values ('five') ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsNoneOf(1L, 2L, 3L, 4L, 8L, 9L, 10L, 11L, 15L);
        } else {
            if (sqlPostFix.equals("")) {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 8L, 9L, 10L, 11L, 15L).inOrder();
            } else {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L).inOrder();
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testInsertSelect2() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_4";
        String srcTableName = schemaPrefix + "src_table";
        dropTableIfExists(tableName);
        dropTableIfExists(srcTableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create table %s (id int , name varchar(20))", srcTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 源表中插入四条数据
        sql = "insert into " + srcTableName + " (id, name) values ( 1, '1'), (null, '2'), (3, '3'), (0, '4')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name)  select id, name from " + srcTableName + " order by name";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name)  values ('five') ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(1L, 3L);
            assertThat(ids).containsNoneOf(2L, 4L, 5L, 6L);
        } else {
            if (sqlPostFix.equals("")) {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 6L).inOrder();
            } else {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L).inOrder();
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testInsertSelect3() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_4";
        String srcTableName = schemaPrefix + "src_table";
        dropTableIfExists(tableName);
        dropTableIfExists(srcTableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create table %s (id int , name varchar(20))", srcTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 源表中插入四条数据
        sql = "insert into " + srcTableName + " (id, name) values ( 3, '1'), (4, '2'), (5, '3'), (6, '4')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name)  select id, name from " + srcTableName + " order by name";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name)  values ('five') ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(3L, 4L, 5L, 6L);
            assertThat(ids).doesNotContain(7L);
        } else {
            assertThat(ids).containsAtLeast(3L, 4L, 5L, 6L, 7L).inOrder();
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testInsertSelect4() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_4";
        String srcTableName = schemaPrefix + "src_table";
        dropTableIfExists(tableName);
        dropTableIfExists(srcTableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create table %s (id int , name varchar(20))", srcTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 源表中插入四条数据
        sql = "insert into " + srcTableName + " (id, name) values ( null, '1'), (0, '2'), (5, '3'), (6, '4')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name)  select id, name from " + srcTableName + " order by name";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name)  values ('five') ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(5L, 6L);
            assertThat(ids).containsNoneOf(1L, 2L, 7L);
        } else {
            assertThat(ids).containsAtLeast(1L, 2L, 5L, 6L, 7L).inOrder();
        }

//        Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 1, 2, 5, 6, 7 }));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testInsertSelect5() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_4";
        String srcTableName = schemaPrefix + "src_table";
        dropTableIfExists(tableName);
        dropTableIfExists(srcTableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create table %s (id int , name varchar(20))", srcTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 源表中插入四条数据
        sql = "insert into " + srcTableName + " (id, name) values ( 5, '1'), (6, '2'), (null, '3'), (0, '4')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name)  select id, name from " + srcTableName + " order by name";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name)  values ('five') ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(5L, 6L);
            assertThat(ids).containsNoneOf(8L, 9L, 10L);
        } else {
            if (sqlPostFix.equalsIgnoreCase("")) {
                assertThat(ids).containsAtLeast(5L, 6L, 7L, 8L, 10L).inOrder();
            } else {
                assertThat(ids).containsAtLeast(5L, 6L, 7L, 8L, 9L).inOrder();
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testInsertSelect6() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_4";
        String srcTableName = schemaPrefix + "src_table";
        dropTableIfExists(tableName);
        dropTableIfExists(srcTableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create table %s (id int , name varchar(20))", srcTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 源表中插入四条数据
        sql = "insert into " + srcTableName + " (id, name) values ( 5, '1'), (6, '2'), (null, '3'), (0, '4')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name)  values (7, 'five') ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name)  select id, name from " + srcTableName + " order by name";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (name)  values ('five') ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(5L, 6L, 7L);
            assertThat(ids).containsNoneOf(8L, 9L, 10L, 11L);
        } else {
            if (sqlPostFix.equalsIgnoreCase("")) {
                assertThat(ids).containsAtLeast(5L, 6L, 7L, 8L, 9L, 11L).inOrder();
            } else {
                assertThat(ids).containsAtLeast(5L, 6L, 7L, 8L, 9L, 10L).inOrder();
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Ignore("预期结果不确定,暂时ignore")
    @Test
    public void testInsertSelectFailed() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_4";
        String srcTableName = schemaPrefix + "src_table";
        dropTableIfExists(tableName);
        dropTableIfExists(srcTableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create table %s (id int , name varchar(20))", srcTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 源表中插入四条数据
        sql = "insert into " + srcTableName + " (id, name) values ( null, '1'), (0, '2'), (3, '3'), (4, '4')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name)  select id, name from " + srcTableName + " order by name";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = "insert into " + tableName + " (name)  values ('five') ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).containsAtLeast(3L, 4L);
            assertThat(ids).containsNoneOf(1L, 2L, 7L, 8L);
        } else {
            if (sqlPostFix.equalsIgnoreCase("")) {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 8L).inOrder();
            } else {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 7L).inOrder();
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testInsertInMultiThread() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_7";
        dropTableIfExists(tableName);
        String sql = String
            .format("create table %s (id bigint primary key not null auto_increment %s, name varchar(20)) %s",
                tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (0,  'one'), (0, 'two')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Connection conn1 = null;
        Connection conn2 = null;
        try {
            conn1 = getPolardbxConnection();
            conn2 = getPolardbxConnection();

            // We must ensure that all reads from one group use the same connection.
            // 5.2 version: insert select is pushed down.
            String sqlInsertSelect =
                "insert into " + tableName + " (name)  select /*+TDDL: cmd_extra(MERGE_UNION_SIZE=0)*/ name from "
                    + tableName + " where name != 'auto'";

            Thread t1 = new Thread(new SQLRunner(sqlInsertSelect, 10, conn1));

            String sqlInsert = "insert into " + tableName + " (name)  values('auto')";
            Thread t2 = new Thread(new SQLRunner(sqlInsert, 100, conn2));

            t1.start();
            t1.join();
            t2.start();

            t2.join();

        } finally {
            conn1.close();
            conn2.close();
        }

        sql = "select ID from " + tableName + " order by ID";
        List<Long> expectResult = new ArrayList<Long>();
        for (long i = 1; i <= 2148; i++) {
            expectResult.add(i);
        }
        // 单库的insert select会出现跳跃id,这里只判断长度
        if (isSpecialSequence(seqType)) {

            assertThat(
                JdbcUtil.selectIds(String.format("select count(*) as ID from %s ", tableName), "ID", tddlConnection))
                .contains(2148L);
//            Assert.assertTrue(isIDSame(,
//                "ID",
//                new Integer[] { 2148 }));
        } else {
            assertThat(JdbcUtil.selectIds(sql, "ID", tddlConnection)).containsAtLeastElementsIn(expectResult);
        }

        dropTableIfExists(tableName);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateTableTypeTest() throws Exception {
        String[] types = {
            "INT", " INT UNSIGNED", "INTEGER", " INTEGER UNSIGNED", "SMALLINT", " SMALLINT UNSIGNED",
            "TINYINT", " TINYINT UNSIGNED", "MEDIUMINT", " MEDIUMINT UNSIGNED", "BIGINT", " BIGINT UNSIGNED"};

        for (String type : types) {
            //time 类型的sequence只用于bigint
            if (seqType.contains("time") && !type.contains("BIGINT")) {
                continue;
            }
            // group类型sequence不适用于
            if ((seqType.contains("group") || seqType.isEmpty()) && (type.contains("TINY") || type.contains("SMALL"))) {
                continue;
            }

            if (seqType.isEmpty() && type.contains("TINYINT UNSIGNED")) {
                continue;
            }

            log.info("type is " + type);
            String tableName = schemaPrefix + "sequence_ddl_test_8";
            dropTableIfExists(tableName);
            String sql = String
                .format("create table %s (id %s primary key not null auto_increment %s, name varchar(20)) %s",
                    tableName, type, seqType, sqlPostFix);
            log.info(sql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "insert into " + tableName + " (name) values ('one'), ('two')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // 指定值与隐式值
            sql = "insert into " + tableName + " (id, name) values (null, 'one'), (null, 'two'),(0, 'one'), (0, 'two')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // 显示指定值与隐式值结合
            sql = "insert into " + tableName + " (id, name) values (100, '100'), (null, 'two'),(0, 'one')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // 显示指定值字符串与隐式值结合
            sql = "insert into " + tableName + " (id, name) values ('120', '100'), (null, 'two'),(0, 'one')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "select ID from " + tableName + " order by ID";
            List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
            if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
                assertThat(ids).containsAtLeast(100L, 120L);
                assertThat(ids).containsNoneOf(1L, 2L, 3L, 4L, 5L, 6L, 101L, 102L, 121L, 122L);
            } else {
                assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L, 6L, 100L, 101L, 102L, 120L, 121L, 122L).inOrder();
            }

//            Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 1, 2, 3, 4, 5, 6, 100, 101, 102, 201, 202, 203 }));
            dropTableIfExists(tableName);

        }

    }

    /**
     * @since 5.1.24
     */
    @Ignore("暂时不支持,以后也许会支持")
    @Test
    public void testCreateTableFloatTest() throws Exception {
        String[] types = {"FLOAT(23, 2)", "DOUBLE(53,2)"};
        String tableName = schemaPrefix + "sequence_ddl_test_8";

        for (String type : types) {
            dropTableIfExists(tableName);
            String sql = String.format(
                "create table %s (auto_id %s primary key not null auto_increment %s, id int, name varchar(20)) %s",
                tableName, type, seqType, sqlPostFix);
            log.info(sql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "insert into " + tableName + " (id, name) values (1, 'one'), (2, 'two')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // 指定值与隐式值
            sql = "insert into " + tableName
                + " (auto_id, id, name) values (null, 3,  'one'), (null, 4, 'two'),(0, 5, 'one'), (0, 6, 'two')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // 显示指定值与隐式值结合
            sql = "insert into " + tableName
                + " (auto_id, id, name) values (100 ,7, '100'), (null, 8, 'two'),(0, 9, 'one')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // 显示指定值与隐式值结合
            sql = "insert into " + tableName
                + " (auto_id, id, name) values (301.5 ,13, '100'), (null, 14, 'two'),(0, 15, 'one')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // 显示指定值字符串与隐式值结合
            sql = "insert into " + tableName
                + " (auto_id, id, name) values ('401.5',16,  '100'), (null, 17,'two'),(0, 18,'one')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "select ID from " + tableName + " order by ID";
            List<Double> ids = JdbcUtil.selectDoubleIds(sql, "ID", tddlConnection);
//            if(seqType.contains("time") || seqType.contains("group")){
//                assertThat(ids).containsAtLeast(100, 201L);
//                assertThat(ids).containsNoneOf(1L, 2L, 3L, 4L, 5L, 6L, 101L, 102L, 202L, 203L);
//            }else{
            assertThat(ids)
                .containsAtLeast(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0)
                .inOrder();
//            }

//            Assert.assertTrue(isIDSame(sql, "ID", new Integer[] { 1, 2, 3, 4, 5, 6, 100, 101, 102, 201, 202, 203 }));
            dropTableIfExists(tableName);

        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAutoIncrementNotShardColumn() throws Exception {
        String tableName = schemaPrefix + "sequence_ddl_test_11";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (auto_id bigint primary key not null auto_increment %s, id int,  name varchar(20)) %s",
            tableName, seqType, sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (4, 'one'), (5, 'two')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 指定值与隐式值
        sql = "insert into " + tableName
            + " (auto_id, id,  name) values (null, 1, 'one'), (null, 2, 'two'),(0, 3, 'one'), (0, 3, 'two')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 显示指定值与隐式值结合
        sql = "insert into " + tableName
            + " (auto_id, id, name) values (100,6,  '100'), (null, 7, 'two'),(0, 8, 'one')";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select auto_id from " + tableName + " order by auto_id";
        List<Long> ids = JdbcUtil.selectIds(sql, "auto_id", tddlConnection);
        if (isSpecialSequence(seqType) && (!sqlPostFix.isEmpty())) {
            assertThat(ids).contains(100L);
            assertThat(ids).containsNoneOf(1L, 2L, 3L, 4L, 5L, 6L, 101L, 102L);
        } else {
            assertThat(ids).containsAtLeast(1L, 2L, 3L, 4L, 5L, 6L, 100L, 101L, 102L).inOrder();
        }
//        Assert.assertTrue(isIDSame(sql, "auto_id", new Integer[] { 1, 2, 3, 4, 5, 6, 100, 101, 102 }));
        dropTableIfExists(tableName);

    }

    private void assertLastInsertIdSame(String tableName, String columnName) throws Exception {
        String sql = String.format("select max(%s) as ID from %s", columnName, tableName);
        List<Long> ids = JdbcUtil.selectIds(sql, "ID", tddlConnection);
        assertThat(getLastInsertId(tddlConnection)).isEqualTo(ids.get(0));
        assertThat(getIdentity(tddlConnection)).isEqualTo(ids.get(0));
    }
}


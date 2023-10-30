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

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.enableAsyncDDL;


public class DropTableSqlTest extends AsyncDDLBaseNewDBTestCase {

    private List<Connection> phyConnectionList;

    public DropTableSqlTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Before
    public void beforeDropTableSqlTest() {
        this.phyConnectionList =
            StringUtils.isBlank(tddlDatabase2) ? getMySQLPhysicalConnectionList(tddlDatabase1) :
                getMySQLPhysicalConnectionList(tddlDatabase2);
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testDropSequence() {
        String table = schemaPrefix + "sequence";
        String sql = "/*+TDDL:CMD_EXTRA(FORCE_DDL_ON_LEGACY_ENGINE=TRUE）*/drop table if exists " + table;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testDropSequenceOpt() {
        String table = schemaPrefix + "sequence_opt";
        String sql = "/*+TDDL:CMD_EXTRA(FORCE_DDL_ON_LEGACY_ENGINE=TRUE）*/drop table if exists " + table;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testDropTableWithDbName() throws Exception {
        String sql = "drop table if exists abc.table1234567 ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testDropTableWithDbNameExist() throws Exception {
        String app = StringUtils.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String sql = String.format("drop table if exists %s.table1234567 ", app);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testDropTableExistWithDbNameExist() throws Exception {
        String app = StringUtils.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = app + "." + "droptest";
        // 删表
        String sql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        // 建表
        sql = String.format("create table %s(id int, name varchar(20)) dbpartition by hash(id)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        // 删表
        sql = String.format("drop table if exists %s ", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertFalse(DataValidator.isShowTableExist(tableName, tddlConnection));
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDropTableCreateFailed() {
        String simpleTableName = "gxw_drop_test_1";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`),KEY `idx_txt`(`ttt`) USING BTREE) ENGINE=InnoDB ";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        if (enableAsyncDDL) {
            Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        } else {
            Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        }

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDropTableCreateFailedShard() {
        String simpleTableName = "gxw_drop_test_2";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        // 一个不会被预发检查过滤但是会执行失败的语句，这里只会建规则，不会建成功表
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`),KEY `idx_txt`(`ttt`) USING BTREE) ENGINE=InnoDB dbpartition by hash(id)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        if (enableAsyncDDL) {
            Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        } else {
            Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        }

        dropTableIfExists(tableName);
        Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));

    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDropTableCreateFailedShardTb() {
        String simpleTableName = "gxw_drop_test_3";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        // 一个不会被预发检查过滤但是会执行失败的语句，这里只会建规则，不会建成功表
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` text NULL,PRIMARY KEY (`id`),KEY `idx_txt`(`ttt`) USING BTREE) ENGINE=InnoDB dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        if (enableAsyncDDL) {
            Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));
        } else {
            Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));
        }

        dropTableIfExists(tableName);
        Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));

    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDropTableCreateFGsiBackfillTestailedShardDbPartly() {
        String simpleTableName = "gxw_drop_test_4";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);
        // 一个不会被预发检查过滤但是会执行失败的语句，这里只会建规则，不会建成功表
        String sql = "  CREATE TABLE "
            + tableName
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, `ttt` int ,PRIMARY KEY (`id`),KEY `idx_txt`(`ttt`) USING BTREE) ENGINE=InnoDB dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isShowTableExist(tableName, tddlDatabase2));

        // 删掉某个分库的一张表
        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName, 1);
        dropOneDbInMysql(phyConnectionList, physicalTableName, 1);

        dropTableIfExists(tableName);
        Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));

    }

    /**
     * @since 5.1.21
     */
    @Test
    public void testDropTableNotExist() {
        String tableName = schemaPrefix + "gxw_drop_test_2";
        Assert.assertFalse(isShowTableExist(tableName, tddlDatabase2));

        dropTableFaild(tableName);

        dropTableIfExists(tableName);
    }

}

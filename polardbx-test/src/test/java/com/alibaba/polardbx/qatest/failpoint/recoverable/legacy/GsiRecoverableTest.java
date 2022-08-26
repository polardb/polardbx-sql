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

package com.alibaba.polardbx.qatest.failpoint.recoverable.legacy;

import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.failpoint.base.BaseFailPointTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Litmus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GsiRecoverableTest extends BaseFailPointTestCase {

    private String PRIMARY_TABLE_NAME;
    private String GSI_NAME;
    private String createTableStmt;
    private String createGsiStmt;
    private String dropGsiStmt;
    private String alterTableAddGsiStmt;
    private String showCreateTableAssert;

    /**
     * create primary table
     */
    @Before
    public void doBefore() {
        clearFailPoints();
        PRIMARY_TABLE_NAME = randomTableName("gsi_recoverable_test_primary", 4);
        GSI_NAME = randomTableName("gsi_recoverable_test_gsi", 4);
        createTableStmt = String.format(createTableStmtTemplate, PRIMARY_TABLE_NAME);
        createGsiStmt = String.format(
            "CREATE GLOBAL INDEX %s ON %s(id2) DBPARTITION BY HASH (id2)",
            GSI_NAME,
            PRIMARY_TABLE_NAME
        );
        showCreateTableAssert = String.format(SHOW_CREATE_TABLE_ASSERT_TEMPLATE,
            PRIMARY_TABLE_NAME,
            GSI_NAME
        );
        dropGsiStmt = String.format("drop index %s on %s",
            GSI_NAME,
            PRIMARY_TABLE_NAME
        );
        alterTableAddGsiStmt = String.format("alter table %s add global index %s(id2) DBPARTITION BY HASH (id2)",
            PRIMARY_TABLE_NAME,
            GSI_NAME
        );

        JdbcUtil.executeUpdateSuccess(failPointConnection, "drop table if exists " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
    }

    @After
    public void doAfter() {
        clearFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_BACK_AND_FORTH() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true");
        executeDDL();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_FAIL_ONCE() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_FAIL_ONCE, "true");
        executeDDL();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_EXECUTE_TWICE() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true");
        executeDDL();
    }

    @Test
    public void test_FP_RANDOM_PHYSICAL_DDL_EXCEPTION() {
        enableFailPoint(FailPointKey.FP_RANDOM_PHYSICAL_DDL_EXCEPTION, "15");
        executeDDL();
    }

    @Test
    public void test_FP_RANDOM_FAIL() {
        enableFailPoint(FailPointKey.FP_RANDOM_FAIL, "10");
        executeDDL();
    }

    @Test
    public void test_FP_RANDOM_SUSPEND() {
        enableFailPoint(FailPointKey.FP_RANDOM_SUSPEND, "5,3000");
        executeDDL();
    }

    protected void executeDDL() {
        //create gsi
        JdbcUtil.executeUpdateSuccess(failPointConnection, createGsiStmt);

        // CREATE GLOBAL INDEX
        TableChecker tableChecker = getTableChecker(failPointConnection, PRIMARY_TABLE_NAME);
        tableChecker.identicalTableDefinitionTo(showCreateTableAssert, true, Litmus.THROW);

        // SHOW INDEX
        ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(failPointConnection, PRIMARY_TABLE_NAME);
        showIndexChecker.identicalToTableDefinition(showCreateTableAssert, true, Litmus.THROW);

//        JdbcUtil.executeUpdateSuccess(failPointConnection, dropGsiStmt);
//        JdbcUtil.executeUpdateSuccess(failPointConnection, alterTableAddGsiStmt);
//
//        // CREATE GLOBAL INDEX
//        tableChecker = getTableChecker(failPointConnection, PRIMARY_TABLE_NAME);
//        tableChecker.identicalTableDefinitionTo(showCreateTableAssert, true, Litmus.THROW);
//
//        // SHOW INDEX
//        showIndexChecker = getShowIndexGsiChecker(failPointConnection, PRIMARY_TABLE_NAME);
//        showIndexChecker.identicalToTableDefinition(showCreateTableAssert, true, Litmus.THROW);
    }

    private String createTableStmtTemplate = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
        + "\t`c2` int(20) DEFAULT NULL,\n"
        + "\t`tint` tinyint(10) UNSIGNED ZEROFILL DEFAULT NULL,\n"
        + "\t`sint` smallint(6) DEFAULT '1000',\n"
        + "\t`mint` mediumint(9) DEFAULT NULL,\n"
        + "\t`bint` bigint(20) DEFAULT NULL COMMENT ' bigint',\n"
        + "\t`dble` double(10, 2) DEFAULT NULL,\n"
        + "\t`fl` float(10, 2) DEFAULT NULL,\n"
        + "\t`dc` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`num` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`dt` date DEFAULT NULL,\n"
        + "\t`ti` time DEFAULT NULL,\n"
        + "\t`tis` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
        + "\t`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`dti` datetime DEFAULT NULL,\n"
        + "\t`vc` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`vc2` varchar(100) COLLATE utf8_bin NOT NULL,\n"
        + "\t`tb` tinyblob,\n"
        + "\t`bl` blob,\n"
        + "\t`mb` mediumblob,\n"
        + "\t`lb` longblob,\n"
        + "\t`tt` tinytext COLLATE utf8_bin,\n"
        + "\t`mt` mediumtext COLLATE utf8_bin,\n"
        + "\t`lt` longtext COLLATE utf8_bin,\n"
        + "\t`en` enum('1', '2') COLLATE utf8_bin NOT NULL,\n"
        + "\t`st` set('5', '6') COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`id1` int(11) DEFAULT NULL,\n"
        + "\t`id2` int(11) DEFAULT NULL,\n"
        + "\t`id3` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL,\n"
        + "\t`vc1` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`vc3` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`),\n"
        + "\tUNIQUE `idx3` USING BTREE (`vc1`(20)),\n"
        + "\tKEY `idx1` USING HASH (`id1`),\n"
        + "\tKEY `idx2` USING HASH (`id2`),\n"
        + "\tFULLTEXT KEY `idx4` (`id3`)\n"
        + "\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 ) tbpartition BY HASH (id1) tbpartitions 3";

    private String SHOW_CREATE_TABLE_ASSERT_TEMPLATE = "CREATE TABLE %s (\n"
        + "\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
        + "\t`c2` int(20) DEFAULT NULL,\n"
        + "\t`tint` tinyint(10) UNSIGNED ZEROFILL DEFAULT NULL,\n"
        + "\t`sint` smallint(6) DEFAULT '1000',\n"
        + "\t`mint` mediumint(9) DEFAULT NULL,\n"
        + "\t`bint` bigint(20) DEFAULT NULL COMMENT ' bigint',\n"
        + "\t`dble` double(10, 2) DEFAULT NULL,\n"
        + "\t`fl` float(10, 2) DEFAULT NULL,\n"
        + "\t`dc` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`num` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`dt` date DEFAULT NULL,\n"
        + "\t`ti` time DEFAULT NULL,\n"
        + "\t`tis` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
        + "\t`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`dti` datetime DEFAULT NULL,\n"
        + "\t`vc` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`vc2` varchar(100) COLLATE utf8_bin NOT NULL,\n"
        + "\t`tb` tinyblob,\n"
        + "\t`bl` blob,\n"
        + "\t`mb` mediumblob,\n"
        + "\t`lb` longblob,\n"
        + "\t`tt` tinytext COLLATE utf8_bin,\n"
        + "\t`mt` mediumtext COLLATE utf8_bin,\n"
        + "\t`lt` longtext COLLATE utf8_bin,\n"
        + "\t`en` enum('1', '2') COLLATE utf8_bin NOT NULL,\n"
        + "\t`st` set('5', '6') COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`id1` int(11) DEFAULT NULL,\n"
        + "\t`id2` int(11) DEFAULT NULL,\n"
        + "\t`id3` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL,\n"
        + "\t`vc1` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`vc3` varchar(100) COLLATE utf8_bin DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`),\n"
        + "\tUNIQUE KEY `idx3` USING BTREE (`vc1`(20)),\n"
        + "\tKEY `idx1` USING HASH (`id1`),\n"
        + "\tKEY `idx2` USING HASH (`id2`),\n"
        + "\tFULLTEXT KEY `idx4` (`id3`),\n"
        + "\tGLOBAL INDEX %s(`id2`) COVERING (`pk`, `id1`) DBPARTITION BY HASH(`id2`)\n"
        + ") ENGINE = InnoDB AUTO_INCREMENT = 2 DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_bin AVG_ROW_LENGTH = 100 COMMENT 'abcd'  dbpartition by hash(`id1`) tbpartition by hash(`id1`) tbpartitions 3";
}
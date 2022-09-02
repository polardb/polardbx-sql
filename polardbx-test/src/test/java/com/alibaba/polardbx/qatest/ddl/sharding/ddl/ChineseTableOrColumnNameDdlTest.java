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

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * 表名和列名为中文字符串且使用partition方式的DDL测试类
 *
 * @author arnkore 2016-07-18 16:43
 */

public class ChineseTableOrColumnNameDdlTest extends DDLBaseNewDBTestCase {

    private String tableName = "";

    public ChineseTableOrColumnNameDdlTest(boolean croassSchema) {
        this.crossSchema = croassSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    @Before
    public void init() throws Exception {

        this.tableName = schemaPrefix + "呵呵";
        prepareTable();
    }

    @After
    public void destroy() throws Exception {
        dropTableIfExists("呵呵");
    }

    private void prepareTable() throws Exception {
        dropTableIfExists(tableName);

        String createTableSql = "CREATE TABLE "
            + tableName
            + " (\n"
            + "  `编号` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n"
            + "  `名字` varchar(45) NOT NULL,\n"
            + "  `addr` varchar(45) NOT NULL,\n"
            + "  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `UPDATE_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'test',\n"
            + "  PRIMARY KEY (`编号`),\n"
            + "  KEY `IDX_testNoPK_Name` (`名字`)\n"
            + ") ENGINE=InnoDB AUTO_INCREMENT=10000 DEFAULT CHARSET=utf8 dbpartition by hash(`编号`);";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

    }

    @Test
    public void partitionCreateTest() throws Exception {
        // logic in init method's prepareTable call.
    }

    @Test
    public void partitionAlterTest() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "ALTER TABLE  " + tableName
            + "  ADD COLUMN `性别` CHAR(1) NOT NULL");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "ALTER TABLE  " + tableName
            + " CHANGE COLUMN `gmt_create` `创建时间` timestamp NULL");
        //        tddlUpdateData("ALTER TABLE `呵呵` RENAME COLUMN `UPDATE_TIME` TO `更新时间`");
    }

    @Test
    public void partitionDropTest() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE " + tableName);
    }

    @Test
    public void partitionTruncateTest() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "TRUNCATE TABLE  " + tableName);
    }

    @Test
    public void nonAsciiOrChineseCharacterInterceptTest() throws Exception {
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set names latin1");

            String sql = "CREATE TABLE "
                + schemaPrefix
                + "中文_jkakf84124 (\n"
                + "  `编号` int(11) NOT NULL,\n"
                + "  `姓名` varchar(45) DEFAULT NULL,\n"
                + "  `性别` varchar(45) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`编号`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`编号`) dbpartitions 2 tbpartition by hash(`编号`) tbpartitions 2;";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        } finally {
            // 恢复字符集 
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set names utf8");
        }

    }
}

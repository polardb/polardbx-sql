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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ParameteredTypeTest extends ReadBaseTestCase {

    private String baseOneTableName1;
    private String baseOneTableName2;

    public ParameteredTypeTest() {
        this.baseOneTableName1 = "crm_student_answer";
        this.baseOneTableName2 = "crm_student_answer_gsi";
    }

    private void prepareTable() {
        String sql1 = "CREATE TABLE " + baseOneTableName1 + " (\n"
            + "        `id` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `catalog_name` varchar(255) DEFAULT NULL,\n"
            + "        `catalog_name1` varchar(255) DEFAULT NULL character set gbk,\n"
            + "        `user_id` varchar(255) DEFAULT NULL COMMENT '用户id',\n"
            + "        PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 11105348 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC  dbpartition by hash(`user_id`) tbpartition by hash(`user_id`) tbpartitions 4;";

        String sql2 = "CREATE TABLE " + baseOneTableName2 + " (\n"
            + "        `id` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `catalog_name` varchar(255) DEFAULT NULL,\n"
            + "        `user_id` varchar(255) DEFAULT NULL COMMENT '用户id',\n"
            + "        `catalog_name1` varchar(255) DEFAULT NULL character set gbk,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        GLOBAL INDEX `c_s_a_r_id`(`id`) COVERING (`user_id`) DBPARTITION BY HASH(`id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 11105348 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC  dbpartition by hash(`user_id`) tbpartition by hash(`user_id`) tbpartitions 4;";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
    }

    private void dropTableIfExists(String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Before
    public void initData() throws Exception {
        dropTableIfExists(baseOneTableName1);
        dropTableIfExists(baseOneTableName2);
        prepareTable();
    }

    @After
    public void afterData() throws Exception {
        dropTableIfExists(baseOneTableName1);
        dropTableIfExists(baseOneTableName2);
    }

    @Test
    @Ignore("fix by ???")
    public void testUpdateOnHex() throws SQLException {
        //no gsi
        String insertSql1 = "insert into " + baseOneTableName1
            + " (`id`, `catalog_name`, `user_id`, `catalog_name1`) values (4, x'E58891E7BD9AE6B688E781ADE588B6E5BAA6', x'363233313364613336306232393636633230343363643234', x'D0CCB7A3CFFBC3F0D6C6B6C8');";
        String updateSql1 = "update " + baseOneTableName1
            + " set `id` = 4,`catalog_name` = x'E58891E7BD9AE6B688E781ADE588B6E5BAA6', `catalog_name1` = x'D0CCB7A3CFFBC3F0D6C6B6C8', `user_id` = x'363233313364613336306232393636633230343363643234' WHERE `id` = 4 limit 1;";
        String selectSql1 = "select * from " + baseOneTableName1;
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateSql1);
        ResultSet resultSet1 = JdbcUtil.executeQuery(selectSql1, tddlConnection);
        Assert.assertTrue(resultSet1.next());
        String catalog1 = resultSet1.getString(2);
        Assert.assertTrue("刑罚消灭制度".equalsIgnoreCase(catalog1));
        catalog1 = resultSet1.getString(3);
        Assert.assertTrue("刑罚消灭制度".equalsIgnoreCase(catalog1));

        //gsi
        String insertSql2 = "insert into " + baseOneTableName2
            + " (`id`, `catalog_name`, `user_id`, `catalog_name1`) values (4, x'E58891E7BD9AE6B688E781ADE588B6E5BAA6', x'363233313364613336306232393636633230343363643234', x'D0CCB7A3CFFBC3F0D6C6B6C8');";
        String updateSql2 = "update " + baseOneTableName2
            + " set `id` = 4,`catalog_name` = x'E58891E7BD9AE6B688E781ADE588B6E5BAA6', `catalog_name1` = x'D0CCB7A3CFFBC3F0D6C6B6C8', `user_id` = x'363233313364613336306232393636633230343363643234' WHERE `id` = 4 limit 1;";
        String selectSql2 = "select * from " + baseOneTableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateSql2);
        ResultSet resultSet2 = JdbcUtil.executeQuery(selectSql2, tddlConnection);
        Assert.assertTrue(resultSet2.next());
        String catalog2 = resultSet2.getString(2);
        Assert.assertTrue("刑罚消灭制度".equalsIgnoreCase(catalog2));
        catalog2 = resultSet1.getString(3);
        Assert.assertTrue("刑罚消灭制度".equalsIgnoreCase(catalog2));
    }
}

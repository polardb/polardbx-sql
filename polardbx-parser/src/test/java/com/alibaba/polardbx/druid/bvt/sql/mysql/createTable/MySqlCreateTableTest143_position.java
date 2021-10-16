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

package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

/**
 * @version 1.0
 * @ClassName MySqlCreateTableTest143_position
 * @description
 * @Author zzy
 * @Date 2019-05-09 10:45
 */
public class MySqlCreateTableTest143_position extends MysqlTest {

    public void test_0() throws Exception {

        String sql = "CREATE TABLE `resume_position_portrait` (\n" +
                "  `resume_id` varchar(36) NOT NULL,\n" +
                "  `method` varchar(50) NOT NULL,\n" +
                "  `position` varchar(256) NOT NULL,\n" +
                "  `position_level1` longtext,\n" +
                "  `position_level2` longtext,\n" +
                "  `probability` float NOT NULL,\n" +
                "  `success` bit(1) NOT NULL,\n" +
                "  `updated` datetime(6) NOT NULL,\n" +
                "  `created` datetime DEFAULT NULL,\n" +
                "  PRIMARY KEY (`resume_id`),\n" +
                "  INDEX `IX_resume_position_portrait_probability` (`probability`) USING BTREE,\n" +
                "  KEY `IX_resume_position_portrait_method` (`method`) USING BTREE,\n" +
                "  KEY `IX_resume_position_portrait_method_probability` (`method`,`probability`) USING BTREE,\n" +
                "  KEY `IX_resume_position_portrait_position_probability` (`position`(191),`probability`) USING BTREE,\n" +
                "  KEY `ix_probability_method` (`probability`,`method`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by hash(`resume_id`) tbpartition by hash(`resume_id`) tbpartitions 4";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE `resume_position_portrait` (\n" +
                "\t`resume_id` varchar(36) NOT NULL,\n" +
                "\t`method` varchar(50) NOT NULL,\n" +
                "\t`position` varchar(256) NOT NULL,\n" +
                "\t`position_level1` longtext,\n" +
                "\t`position_level2` longtext,\n" +
                "\t`probability` float NOT NULL,\n" +
                "\t`success` bit(1) NOT NULL,\n" +
                "\t`updated` datetime(6) NOT NULL,\n" +
                "\t`created` datetime DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`resume_id`),\n" +
                "\tINDEX `IX_resume_position_portrait_probability` USING BTREE(`probability`),\n" +
                "\tKEY `IX_resume_position_portrait_method` USING BTREE (`method`),\n" +
                "\tKEY `IX_resume_position_portrait_method_probability` USING BTREE (`method`, `probability`),\n" +
                "\tKEY `IX_resume_position_portrait_position_probability` USING BTREE (`position`(191), `probability`),\n" +
                "\tKEY `ix_probability_method` (`probability`, `method`)\n" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n" +
                "DBPARTITION BY hash(`resume_id`)\n" +
                "TBPARTITION BY hash(`resume_id`) TBPARTITIONS 4", stmt.toString());

        assertEquals("create table `resume_position_portrait` (\n" +
                "\t`resume_id` varchar(36) not null,\n" +
                "\t`method` varchar(50) not null,\n" +
                "\t`position` varchar(256) not null,\n" +
                "\t`position_level1` longtext,\n" +
                "\t`position_level2` longtext,\n" +
                "\t`probability` float not null,\n" +
                "\t`success` bit(1) not null,\n" +
                "\t`updated` datetime(6) not null,\n" +
                "\t`created` datetime default null,\n" +
                "\tprimary key (`resume_id`),\n" +
                "\tindex `IX_resume_position_portrait_probability` using BTREE(`probability`),\n" +
                "\tkey `IX_resume_position_portrait_method` using BTREE (`method`),\n" +
                "\tkey `IX_resume_position_portrait_method_probability` using BTREE (`method`, `probability`),\n" +
                "\tkey `IX_resume_position_portrait_position_probability` using BTREE (`position`(191), `probability`),\n" +
                "\tkey `ix_probability_method` (`probability`, `method`)\n" +
                ") engine = InnoDB default charset = utf8mb4\n" +
                "dbpartition by hash(`resume_id`)\n" +
                "tbpartition by hash(`resume_id`) tbpartitions 4", stmt.toLowerCaseString());


    }
}

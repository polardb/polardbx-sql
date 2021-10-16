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

public class MySqlCreateTableTest154_hint
        extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE `ngram_2_t1_1` (\n" +
                "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
                "  `title` varchar(200) DEFAULT NULL,\n" +
                "  `content` text,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  FULLTEXT KEY `ft_content` (`content`) /*!50100 WITH PARSER `ngram` */ ,\n" +
                "  FULLTEXT KEY `ft_title` (`title`) /*!50100 WITH PARSER `ngram` */\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=gb2312\n";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());


        assertEquals("CREATE TABLE `ngram_2_t1_1` (\n" +
                "\t`id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
                "\t`title` varchar(200) DEFAULT NULL,\n" +
                "\t`content` text,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tFULLTEXT KEY `ft_content` (`content`),\n" +
                "\tFULLTEXT KEY `ft_title` (`title`)\n" +
                ") ENGINE = InnoDB AUTO_INCREMENT = 3 DEFAULT CHARSET = gb2312", stmt.toString());

        assertEquals("create table `ngram_2_t1_1` (\n" +
                "\t`id` int(10) unsigned not null auto_increment,\n" +
                "\t`title` varchar(200) default null,\n" +
                "\t`content` text,\n" +
                "\tprimary key (`id`),\n" +
                "\tfulltext key `ft_content` (`content`),\n" +
                "\tfulltext key `ft_title` (`title`)\n" +
                ") engine = InnoDB auto_increment = 3 default charset = gb2312", stmt.toLowerCaseString());

    }





}
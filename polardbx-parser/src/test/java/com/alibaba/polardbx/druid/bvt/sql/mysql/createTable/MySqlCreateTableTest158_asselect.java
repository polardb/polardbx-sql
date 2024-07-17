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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import org.junit.Test;

public class MySqlCreateTableTest158_asselect extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "create table tmp_eric (pk int key, ia int unique) replace as select * from t;";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE tmp_eric (\n" +
            "\tpk int PRIMARY KEY,\n" +
            "\tia int UNIQUE\n" +
            ")\n" +
            "REPLACE \n" +
            "AS\n" +
            "SELECT *\n" +
            "FROM t;", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "create table tmp_eric (pk int key, ia int unique) ignore as select * from t;";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE tmp_eric (\n" +
            "\tpk int PRIMARY KEY,\n" +
            "\tia int UNIQUE\n" +
            ")\n" +
            "IGNORE \n" +
            "AS\n" +
            "SELECT *\n" +
            "FROM t;", stmt.toString());
    }

    public void test_3() throws Exception {
        String sql = "create table tmp_eric (pk int key, ia int unique) replace ignore as select * from t;";

        try {
            MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
            fail();
        } catch (Exception e) {
        }
    }

    public void test_4() throws Exception {
        String sql = "CREATE TABLE `D` (\n"
            + " `id` bigint(20) NOT NULL comment 'xxx' AUTO_INCREMENT,\n"
            + " `c1` tinyint(1) DEFAULT NULL,\n"
            + " `c2` tinyint(4) DEFAULT NULL,\n"
            + " PRIMARY KEY (`id`)\n"
            + ") replace as select * from a";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE `D` (\n" +
            "\t`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'xxx',\n" +
            "\t`c1` tinyint(1) DEFAULT NULL,\n" +
            "\t`c2` tinyint(4) DEFAULT NULL,\n" +
            "\tPRIMARY KEY (`id`)\n" +
            ")\n" +
            "REPLACE \n" +
            "AS\n" +
            "SELECT *\n" +
            "FROM a", stmt.toString());
    }

    public void test_5() throws Exception {
        String sql = "CREATE TABLE artists_and_works\n" +
            "  SELECT artist.name, COUNT(work.artist_id) AS number_of_works\n" +
            "  FROM artist LEFT JOIN work ON artist.id = work.artist_id\n" +
            "  GROUP BY artist.id;";

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE artists_and_works\n" +
            "AS\n" +
            "SELECT artist.name, COUNT(work.artist_id) AS number_of_works\n" +
            "FROM artist\n" +
            "\tLEFT JOIN work ON artist.id = work.artist_id\n" +
            "GROUP BY artist.id;", stmt.toString());
    }
}
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

public class MySqlCreateTableTest107 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE \n" +
                "aliolap152578dbopt.aliolap152578dbopt_tbl1 (\n" +
                "col_id_int int NOT NULL , \n" +
                "col2_tinyint tinyint , \n" +
                "col3_boolean boolean , \n" +
                "col4_smallint smallint , \n" +
                "col5_int int , \n" +
                "col6_bigint bigint , \n" +
                "col7_float float , \n" +
                "col8_double double , \n" +
                "col9_date date , \n" +
                "col10_time time , \n" +
                "col11_timestamp timestamp , \n" +
                "col12_varchar varchar(1000) , \n" +
                "col13_multivalue multivalue  delimiter ',' , \n" +
                "primary key (col_id_int,col6_bigint)\n" +
                ") \n" +
                "PARTITION BY HASH KEY(col_id_int) PARTITION NUM 100\n" +
                "SUBPARTITION BY LIST(col6_bigint BIGINT)\n" +
                "SUBPARTITION OPTIONS(available_Partition_Num=100)\n" +
                "TABLEGROUP aliolap152578dbopt_tg1\n" +
                "OPTIONS(UPDATETYPE='realtime')\n" +
                ";";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(14, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE aliolap152578dbopt.aliolap152578dbopt_tbl1 (\n"
                     + "\tcol_id_int int NOT NULL,\n"
                     + "\tcol2_tinyint tinyint,\n" + "\tcol3_boolean boolean,\n"
                     + "\tcol4_smallint smallint,\n"
                     + "\tcol5_int int,\n" + "\tcol6_bigint bigint,\n"
                     + "\tcol7_float float,\n"
                     + "\tcol8_double double,\n" + "\tcol9_date date,\n"
                     + "\tcol10_time time,\n"
                     + "\tcol11_timestamp timestamp,\n" + "\tcol12_varchar varchar(1000),\n"
                     + "\tcol13_multivalue multivalue DELIMITER ',',\n"
                     + "\tPRIMARY KEY (col_id_int, col6_bigint)\n"
                     + ")\n"
                     + "OPTIONS (UPDATETYPE = 'realtime')\n"
                     + "PARTITION BY HASH KEY(col_id_int) PARTITION NUM 100\n"
                     + "SUBPARTITION BY LIST (col6_bigint BIGINT)\n"
                     + "SUBPARTITION OPTIONS (available_Partition_Num = 100)\n"
                     + "TABLEGROUP aliolap152578dbopt_tg1;", stmt.toString());
    }
}
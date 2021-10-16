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

package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 * @ClassName MySqlCreateTableUnitTest
 * @description
 * @Author zzy
 * @Date 2019-05-31 17:39
 */
public class MySqlCreateTableUnitTest extends MysqlTest {

    public void test_0_table_options() throws Exception {
        String sql = "auto_increment 1\n" +
                "avg_row_length 1\n" +
                "default character set utf8\n" +
                "checksum 0\n" +
                "default collate utf8_unicode_ci\n" +
                "comment 'hehe'\n" +
                "compression 'LZ4'\n" +
                "connection 'conn'\n" +
                "index directory 'path'\n" +
                "delay_key_write 1\n" +
                "encryption 'N'\n" +
                "engine innodb\n" +
                "insert_method no\n" +
                "key_block_size 32\n" +
                "max_rows 999\n" +
                "min_rows 1\n" +
                "pack_keys default\n" +
                "password 'psw'\n" +
                "row_format dynamic\n" +
                "stats_auto_recalc default\n" +
                "stats_persistent default\n" +
                "stats_sample_pages 10\n" +
                "tablespace `tbs_name` storage memory\n" +
                "union (tb1,tb2,tb3)\n" +
                "auto_increment 1";
        MySqlExprParser parser = new MySqlExprParser(sql);

        List<SQLAssignItem> list = new ArrayList<SQLAssignItem>();
        assertTrue(parser.parseTableOptions(list, null));

        MySqlOutputVisitor visitor = new MySqlOutputVisitor(System.out);
        for (SQLAssignItem item : list) {
            visitor.visit(item);
            visitor.println();
        }
    }

}

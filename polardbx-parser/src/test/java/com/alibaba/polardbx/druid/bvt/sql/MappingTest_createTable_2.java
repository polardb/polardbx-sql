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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.Collections;
import java.util.Map;

/**
 * Created by wenshao on 16/9/25.
 */
public class MappingTest_createTable_2 extends TestCase {
    String sql = "create table database_source.table_source(\n" +
            "source_key int,\n" +
            "source_value varchar(32),\n" +
            "primary key(source_key)\n" +
            ");";

    Map<String, String> mapping = Collections.singletonMap("database_source.table_source", "database_target.table_target");

    public void test_mapping_createTable() throws Exception {
        String result = SQLUtils.refactor(sql, null, mapping);
        assertEquals("CREATE TABLE database_target.table_target (\n" +
                "\tsource_key int,\n" +
                "\tsource_value varchar(32),\n" +
                "\tPRIMARY KEY (source_key)\n" +
                ");", result);
    }

    public void test_mapping_createTable_mysql() throws Exception {
        String result = SQLUtils.refactor(sql, JdbcConstants.MYSQL, mapping);
        assertEquals("CREATE TABLE database_target.table_target (\n" +
                "\tsource_key int,\n" +
                "\tsource_value varchar(32),\n" +
                "\tPRIMARY KEY (source_key)\n" +
                ");", result);
    }
}

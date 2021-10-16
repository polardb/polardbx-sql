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
public class MappingTest_insert extends TestCase {
    private String sql = "insert into user (id, name) values (123, 'abc')";
    Map<String, String> mapping = Collections.singletonMap("user", "user_01");

    public void test_mapping() throws Exception {
        String result = SQLUtils.refactor(sql, null, mapping);
        assertEquals("INSERT INTO user_01 (id, name)\n" +
                "VALUES (123, 'abc')", result);
    }

    public void test_mapping_mysql() throws Exception {
        String result = SQLUtils.refactor(sql, JdbcConstants.MYSQL, mapping);
        assertEquals("INSERT INTO user_01 (id, name)\n" +
                "VALUES (123, 'abc')", result);
    }

}

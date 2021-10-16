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

import com.alibaba.polardbx.druid.sql.PagerUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class PagerUtilsTest_hasUnorderedLimit extends TestCase {

    public void test_false() throws Exception {
        String sql = " select * from test t order by id limit 3";
        assertFalse(PagerUtils.hasUnorderedLimit(sql, JdbcConstants.MYSQL));
    }

    public void test_false_1() throws Exception {
        String sql = " select * from test t";
        assertFalse(PagerUtils.hasUnorderedLimit(sql, JdbcConstants.MYSQL));
    }

    public void test_true() throws Exception {
        String sql = " select * from test t limit 3";
        assertTrue(PagerUtils.hasUnorderedLimit(sql, JdbcConstants.MYSQL));
    }

    public void test_true_subquery() throws Exception {
        String sql = "select * from(select * from test t limit 3) x";
        assertTrue(PagerUtils.hasUnorderedLimit(sql, JdbcConstants.MYSQL));
    }

    public void test_true_subquery_2() throws Exception {
        String sql = "select * from (select * from test t order by id desc) z limit 100";
        assertFalse(PagerUtils.hasUnorderedLimit(sql, JdbcConstants.MYSQL));
    }
}

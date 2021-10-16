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

package com.alibaba.polardbx.druid.bvt.sql.builder;

import com.alibaba.polardbx.druid.sql.builder.SQLBuilderFactory;
import com.alibaba.polardbx.druid.sql.builder.SQLSelectBuilder;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;

public class BuilderSelectTest extends TestCase {

    public void test_0() throws Exception {
        SQLSelectBuilder builder = SQLBuilderFactory.createSelectSQLBuilder(JdbcConstants.MYSQL);

        builder.from("mytable");
        builder.select("f1", "f2", "f3 F3", "count(*) cnt");
        builder.groupBy("f1");
        builder.having("count(*) > 1");
        builder.orderBy("f1", "f2 desc");
        builder.whereAnd("f1 > 0");

        String sql = builder.toString();
        System.out.println(sql);
        Assert.assertEquals("SELECT f1, f2, f3 AS F3, count(*) AS cnt\n" +
                "FROM mytable\n" +
                "WHERE f1 > 0\n" +
                "GROUP BY f1\n" +
                "HAVING count(*) > 1\n" +
                "ORDER BY f1, f2 DESC", sql);
    }
}

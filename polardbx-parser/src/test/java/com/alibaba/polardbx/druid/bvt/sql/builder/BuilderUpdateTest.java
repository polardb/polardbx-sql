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
import com.alibaba.polardbx.druid.sql.builder.SQLUpdateBuilder;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;

public class BuilderUpdateTest extends TestCase {

    public void test_0() throws Exception {
        SQLUpdateBuilder builder = SQLBuilderFactory.createUpdateBuilder(JdbcConstants.MYSQL);

        builder //
        .from("mytable") //
        .whereAnd("f1 > 0") //
        .set("f1 = f1 + 1", "f2 = ?");

        String sql = builder.toString();
        System.out.println(sql);
        Assert.assertEquals("UPDATE mytable" //
                            + "\nSET f1 = f1 + 1, f2 = ?" //
                            + "\nWHERE f1 > 0", sql);
    }
}

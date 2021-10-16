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

package com.alibaba.polardbx.optimizer.parse.mysql.ansiquote;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class MySQLANSIQuoteTransformerTest {

    @Test
    public void test() throws Exception {
        String sql = "select \"hello\", \"中文\", 无引号列 from \"t1\", \"表2\" where \"foo\" = 'bar'";
        MySQLANSIQuoteTransformer ansiQuoteTransformer = new MySQLANSIQuoteTransformer(ByteString.from(sql));
        String transformed = ansiQuoteTransformer.getTransformerdSql().toString();
        Assert.assertEquals("select `hello`, `中文`, 无引号列 from `t1`, `表2` where `foo` = 'bar'", transformed);
    }
}
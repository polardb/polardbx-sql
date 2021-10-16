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

package com.alibaba.polardbx.druid.bvt.bug;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class Issue69146 extends TestCase {
    public void test_for_issue() throws Exception {
        String sql = "select * from shard_src_p where id = 1 and id = 1 and id = 2;";

        List out = new ArrayList();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql, null, out);

        assertEquals("SELECT *\n" +
                "FROM shard_src_p\n" +
                "WHERE id = ?;", psql);

        assertEquals("[1, 1, 2]", out.toString());
    }
}

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

package com.alibaba.polardbx.druid.bvt.sql.mysql.rebalance;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author moyi
 * @since 2021/04
 */
public class MysqlRebalanceTest {

    @Test
    public void testRebalance() {
        List<String> testCases = Arrays.asList(
            "REBALANCE TABLE tbl",
            "REBALANCE CLUSTER",
            "REBALANCE TABLE tbl THRESHOLD=0.1",
            "REBALANCE TABLE tbl THRESHOLD=0.1 POLICY='partition_count' ",
            "REBALANCE CLUSTER drain_node='abc'",
            "REBALANCE DATABASE",
            "REBALANCE CLUSTER ASYNC=true",
            "REBALANCE CLUSTER DEBUG=true"
        );

        for (String sql : testCases) {
            MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql));
            SQLStatement stmt = parser.parseStatement();

            String output = SQLUtils.toMySqlString(stmt);

            output = StringUtils.deleteWhitespace(output);
            sql = StringUtils.deleteWhitespace(sql);
            Assert.assertEquals(sql, output);
        }
    }
}

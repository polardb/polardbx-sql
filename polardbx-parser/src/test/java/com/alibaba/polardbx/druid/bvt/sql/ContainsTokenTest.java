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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.Token;
import junit.framework.TestCase;

public class ContainsTokenTest extends TestCase {
    public void test_1() throws Exception {
        assertTrue(SQLParserUtils.containsAny("a minus b", DbType.mysql, Token.MINUS));
        assertFalse(SQLParserUtils.containsAny("a = 'minus'", DbType.mysql, Token.MINUS));
    }

    public void test_2() throws Exception {
        assertTrue(SQLParserUtils.containsAny("a minus b", DbType.mysql, Token.MINUS, Token.UNION));
        assertFalse(SQLParserUtils.containsAny("a = 'minus'", DbType.mysql, Token.MINUS, Token.UNION));
    }

    public void test_3() throws Exception {
        assertTrue(
                SQLParserUtils.containsAny("a minus b", DbType.mysql, Token.MINUS, Token.UNION, Token.INTERSECT)
        );

        assertFalse(
                SQLParserUtils.containsAny("a = 'minus'", DbType.mysql, Token.MINUS, Token.UNION, Token.INTERSECT)
        );
    }

    public void test_4() throws Exception {
        assertTrue(SQLParserUtils.containsAny("a minus b", DbType.mysql, Token.MINUS, Token.UNION, Token.INTERSECT, Token.EXCEPT));
        assertFalse(SQLParserUtils.containsAny("a = 'minus'", DbType.mysql, Token.MINUS, Token.UNION, Token.INTERSECT, Token.EXCEPT));
    }
}

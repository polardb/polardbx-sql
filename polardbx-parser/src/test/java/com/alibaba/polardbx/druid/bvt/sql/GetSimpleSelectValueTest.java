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
import junit.framework.TestCase;

public class GetSimpleSelectValueTest extends TestCase {
    public void test_getSimpleSelectValue_0() throws Exception {
        assertEquals(1, SQLParserUtils.getSimpleSelectValue("select 1", DbType.mysql));
        assertEquals(123, SQLParserUtils.getSimpleSelectValue("select 123", DbType.mysql));
    }

    public void test_getSimpleSelectValue_char() throws Exception {
        assertEquals("1", SQLParserUtils.getSimpleSelectValue("select '1'", DbType.mysql));
        assertEquals("123", SQLParserUtils.getSimpleSelectValue("select '123'", DbType.mysql));
        assertEquals("123", SQLParserUtils.getSimpleSelectValue("select N'123'", DbType.mysql));
    }


    public void test_getSimpleSelectValue_null() throws Exception {
        assertNull(SQLParserUtils.getSimpleSelectValue("select now()", DbType.mysql));
        assertNull(SQLParserUtils.getSimpleSelectValue("select * from t", DbType.mysql));
        assertNull(SQLParserUtils.getSimpleSelectValue("select 1 from t", DbType.mysql));
    }

    public void test_getSimpleSelectValue_values() throws Exception {
        assertEquals("1", SQLParserUtils.getSimpleSelectValue("values '1'", DbType.mysql));
        assertEquals("123", SQLParserUtils.getSimpleSelectValue("values '123'", DbType.mysql));
        assertEquals("123", SQLParserUtils.getSimpleSelectValue("values N'123'", DbType.mysql));
    }

    public void test_getSimpleSelectValue_FromDual() throws Exception {
        assertEquals(1, SQLParserUtils.getSimpleSelectValue("select 1 from dual", DbType.mysql));
        assertEquals(123, SQLParserUtils.getSimpleSelectValue("select 123 from dual", DbType.mysql));
    }
}

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

package com.alibaba.polardbx.druid.bvt.sql.replace;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import junit.framework.TestCase;

public class ReplaceBackQuoteTest extends TestCase {
    public void test_0() throws Exception {
        assertEquals("select \"t1\".\"f1\" as \"FF1\" from \"kkk\" as \"t1\" where \"t1\".\"f2\" = 3 and \"t1\" = 'a`b`c'",
                SQLParserUtils.replaceBackQuote("select `t1`.`f1` as `FF1` from `kkk` as `t1` where `t1`.`f2` = 3 and `t1` = 'a`b`c'", DbType.mysql)
        );
    }

    public void test_1() throws Exception {
        assertEquals("select \"c1\" from \"t1\" where \"c1\" = 'd'",
                SQLParserUtils.replaceBackQuote("select \"c1\" from \"t1\" where \"c1\" = `'d'`", DbType.mysql)
        );
    }

    public void test_2() throws Exception {
        assertEquals("select \"c1\" from \"t1\" where \"c1\" = 'd' and \"c2\" = 3",
                SQLParserUtils.replaceBackQuote("select `c1` from `t1` where `c1` = `'d'` and `c2` = 3", DbType.mysql)
        );
    }
    public void test_3() throws Exception {
        assertEquals("select ordinal_position,column_name,data_type,type_name,column_comment from WAYRYD_3GJSJSYCX.columns where lower(table_schema) = 'wayryd_3gjsjsycx' and lower(table_name) = 'nb_app_express' order by ordinal_position",
                SQLParserUtils.replaceBackQuote("select ordinal_position,column_name,data_type,type_name,column_comment from WAYRYD_3GJSJSYCX.columns where lower(table_schema) = `'wayryd_3gjsjsycx'` and lower(table_name) = `'nb_app_express'` order by ordinal_position", DbType.mysql)
        );
    }
    public void test_4() throws Exception {
        assertEquals("select 'a'",
                SQLParserUtils.replaceBackQuote("select `'a'`", DbType.mysql)
        );
    }
}

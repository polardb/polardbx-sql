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

package com.alibaba.polardbx.parser;

import junit.framework.TestCase;

public class CalciteSelectVisitorTest3_is_json extends TestCase {
    public void test_for_calcite() throws Exception {
        String sql = "select "
            + "\"product_name\" is json, "
            + "\"product_name\" is json value, "
            + "\"product_name\" is json object, "
            + "\"product_name\" is json array, "
            + "\"product_name\" is json scalar, "
            + "\"product_name\" is not json, "
            + "\"product_name\" is not json value, "
            + "\"product_name\" is not json object, "
            + "\"product_name\" is not json array, "
            + "\"product_name\" is not json scalar "
            + "from \"product\"";

//        SqlNode sqlNode1 = SqlParser.create(sql).parseQuery();
//        System.out.println(sqlNode1);
//
//        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
//
//        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(new ContextParameters(false), new ExecutionContext());
//        stmt.accept(visitor);
//
//        SqlSelect sqlNode = (SqlSelect)visitor.getSqlNode();
//        System.out.println(sqlNode);

//        assertEquals(FnvHash.hashCode64(sqlNode1.toString())
//                , FnvHash.hashCode64(sqlNode.toString()));
    }
}

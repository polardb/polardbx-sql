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

package com.alibaba.polardbx.druid.bvt.sql.feature;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import junit.framework.TestCase;

public class TestEnableMultiUnion extends TestCase
{
    public void test_union() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("WITH TRADE_BOOKING AS (\n" +
                "\tSELECT '10488874982' ext_booking_sn\n" +
                ")\n");

        for (int i = 0; i < 100000; ++i) {
            if (i != 0) {
                sb.append(" UNION ");
            }
            sb.append("(SELECT * FROM  `TRADE_BOOKING` WHERE `ext_booking_sn` = '" + i + "')\n");
        }
        String sql = sb.toString();

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.EnableMultiUnion);

        MySqlASTVisitorAdapter v = new MySqlASTVisitorAdapter();
        stmt.accept(v);
    }

    public void test_union_1() throws Exception {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 100000; ++i) {
            if (i != 0) {
                sb.append(" UNION ");
            }
            sb.append("SELECT * FROM  `TRADE_BOOKING` WHERE `ext_booking_sn` = '" + i + "'\n");
        }
        String sql = sb.toString();

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.EnableMultiUnion);

        MySqlASTVisitorAdapter v = new MySqlASTVisitorAdapter();
        stmt.accept(v);
    }

    public void test_unionAll() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("WITH TRADE_BOOKING AS (\n" +
                "\tSELECT '10488874982' ext_booking_sn\n" +
                ")\n");

        for (int i = 0; i < 100000; ++i) {
            if (i != 0) {
                sb.append(" UNION ALL ");
            }
            sb.append("(SELECT * FROM  `TRADE_BOOKING` WHERE `ext_booking_sn` = '" + i + "')\n");
        }
        String sql = sb.toString();

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.EnableMultiUnion);

        MySqlASTVisitorAdapter v = new MySqlASTVisitorAdapter();
        stmt.accept(v);
    }

    public void test_unionDistinct() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("WITH TRADE_BOOKING AS (\n" +
                "\tSELECT '10488874982' ext_booking_sn\n" +
                ")\n");

        for (int i = 0; i < 100000; ++i) {
            if (i != 0) {
                sb.append(" UNION DISTINCT ");
            }
            sb.append("(SELECT * FROM  `TRADE_BOOKING` WHERE `ext_booking_sn` = '" + i + "')\n");
        }
        String sql = sb.toString();

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.EnableMultiUnion);

        MySqlASTVisitorAdapter v = new MySqlASTVisitorAdapter();
        stmt.accept(v);
    }

    public void testOrderBy() throws Exception {
        String sql = "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey+1 AS a FROM orders WHERE orderstatus = 'F' UNION ALL \n" +
                "  SELECT orderkey FROM orders WHERE orderkey % 2 = 0 UNION ALL \n" +
                "  (SELECT orderkey+custkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                ") \n" +
                "WHERE a < 20 OR a > 100 \n" +
                "ORDER BY a";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.EnableMultiUnion);
        assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT orderkey + 1 AS a\n" +
                "\tFROM orders\n" +
                "\tWHERE orderstatus = 'F'\n" +
                "\tUNION ALL\n" +
                "\tSELECT orderkey\n" +
                "\tFROM orders\n" +
                "\tWHERE orderkey % 2 = 0\n" +
                "\tUNION ALL\n" +
                "\t(SELECT orderkey + custkey\n" +
                "\tFROM orders\n" +
                "\tORDER BY orderkey\n" +
                "\tLIMIT 10)\n" +
                ")\n" +
                "WHERE a < 20\n" +
                "\tOR a > 100\n" +
                "ORDER BY a", stmt.toString());
    }
}

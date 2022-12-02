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

import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

public class TruncateParserTest extends TestCase {
    private static final List<SQLParserFeature> DEFAULT_FEATURES = Arrays.asList(
        SQLParserFeature.TDDLHint,
        SQLParserFeature.EnableCurrentUserExpr,
        SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DRDSBaseline,
        SQLParserFeature.DrdsGSI,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DrdsCCL
    );

    public void testTruncateInsert() {
        String sql = "truncate table test.t1 insert into test.t1 values (1, 1);";
        parseSqlShouldFail(sql);
    }

    public void testTruncateInsertWithHint() {
        String sql = "/*TDDL: FORBID_EXECUTE_DML_ALL=TRUE*/ truncate table test.t1 insert into test.t1 values (1, 1);";
        parseSqlShouldFail(sql);
    }

    public void testTruncateInsertWithHint2() {
        String sql = "/*+TDDL: FORBID_EXECUTE_DML_ALL=TRUE*/ truncate table test.t1 insert into test.t1 values (1, 1);";
        parseSqlShouldFail(sql);
    }

    private void parseSqlShouldFail(String sql) {
        MySqlStatementParser parser =
            new MySqlStatementParser(ByteString.from(sql), DEFAULT_FEATURES.toArray(new SQLParserFeature[0]));
        try {
            parser.parseStatementList().get(0);
        } catch (ParserException e) {
            return;
        }
        Assert.fail(String.format("parse sql: %s should fail, but not!", sql));
    }
}

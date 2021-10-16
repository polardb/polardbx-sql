/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;

public class MySqlSelectTest_fullwidth_characters extends MysqlTest {

    private static final Charset GBK = Charset.forName("gbk");
    private static final Charset UTF8 = Charset.forName("utf-8");

    private final static SQLParserFeature[] defaultFeatures = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache,
        SQLParserFeature.OptimizedForParameterized,
    };

    public void test_0() throws Exception {
        String sql = "insert into t (id, val) values （123, 456）";
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
    }

    public void test_2() {
        String sql = "select * from t where DATE_FORMAT(fd_create_time,'%Y-%m-%d')='2020-02-22'";
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
    }

    public void test_3() {
        String sql = "select unix_timestamp(now())-(update_time/1000)/3600 from t";
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
    }

    public void test_4() {
        String sql = "insert into t10 (id, name, ship_time) values (2462, '811', null）;";
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
    }

    public void test_InsertColumnsCache() {
        for (int i = 0; i < 3; i++) {
            String query = "INSERT INTO g_brand_tuichus (openid,sid，type) VALUES ('0','4207446210e93c34',23)";
            ByteString sql = new ByteString(query.getBytes(UTF8), UTF8);

            SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, defaultFeatures);
            List<SQLStatement> stmtList = parser.parseStatementList();
            System.out.println(stmtList.get(0).toString());
        }
    }

    public void test_backslash() {
        String sql = "update tb_category set descript='苟\\'利\\国\\n家\\生\\\\死''以'";
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
    }

    public void testSpecialGbkCharacter() {
        String query = "select '碶' as `乣`;\nselect /*碶\n乣*/ 1";
        ByteString sql = new ByteString(query.getBytes(GBK), GBK);
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql);
        List<SQLStatement> stmtList = parser.parseStatementList();
    }

    public void test_6() {
        String sql = "SELECT COUNT(t0t.5分钟任务数) AS f8 from t";
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
    }

    public void test_7() {
        String sql = "select `标识符`, 标识符, 标识笧, `标识笧` from t";
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        Assert.assertEquals("SELECT `标识符`, 标识符, 标识笧, `标识笧`\nFROM t", stmt.toString());
    }

    public void test_binaryLiteralString_utf8() {
        ByteString sql = buildBinaryCase(UTF8);
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, defaultFeatures);
        List<SQLStatement> stmtList = parser.parseStatementList();
    }

    public void test_binaryLiteralString_gbk() {
        ByteString sql = buildBinaryCase(GBK);
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, defaultFeatures);
        List<SQLStatement> stmtList = parser.parseStatementList();
    }

    private ByteString buildBinaryCase(Charset charset) {
        ByteBuffer buffer = ByteBuffer.allocate(2500);
        buffer.put("%header%".getBytes());

        int startPos = buffer.position();
        buffer.put("insert into test values (1, '苟利国家', _binary '".getBytes());
        Random rand = new Random(0);
        for (int i = 0; i < 2000; i++) {
            byte b = (byte)(rand.nextInt(256));
            // Ref. com.mysql.jdbc.PreparedStatement#escapeblockFast
            if (b == '\0') {
                buffer.put((byte) '\\').put((byte) '0');
            } else if (b == '\\' || b == '\'') {
                buffer.put((byte) '\\').put(b);
            } else {
                buffer.put(b);
            }
        }
        buffer.put("', '生死以')".getBytes());
        int endPos = buffer.position();
        buffer.put("%tail%".getBytes());

        return new ByteString(buffer.array(), startPos, endPos - startPos, charset);
    }
}

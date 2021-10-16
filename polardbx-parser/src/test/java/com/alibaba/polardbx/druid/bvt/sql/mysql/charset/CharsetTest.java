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

package com.alibaba.polardbx.druid.bvt.sql.mysql.charset;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.druid.sql.parser.CharTypes.hexToBytes;

public class CharsetTest {
    String[] cs = {
        "big5",
        "dec8",
        "cp850",
        "hp8",
        "koi8r",
        "latin1",
        "latin2",
        "swe7",
        "ascii",
        "ujis",
        "sjis",
        "hebrew",
        "tis620",
        "euckr",
        "koi8u",
        "gb2312",
        "greek",
        "cp1250",
        "gbk",
        "latin5",
        "armscii8",
        "utf8",
        "ucs2",
        "cp866",
        "keybcs2",
        "macce",
        "macroman",
        "cp852",
        "latin7",
        "utf8mb4",
        "cp1251",
        "utf16",
        "utf16le",
        "cp1256",
        "cp1257",
        "utf32",
        "binary",
        "geostd8",
        "cp932",
        "eucjpms",
        "gb18030",
    };

    @Test
    public void testFunction() {
        String sql = "select concat(_gbk'a' collate gbk_chinese_ci, _big5 x'b' collate big5_bin)";
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.DrdsMisc);
        stmt.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLBinaryOpExpr x) {
                SQLExpr left = x.getLeft();
                SQLExpr right = x.getRight();
                Assert.assertTrue(left instanceof MySqlCharExpr);
                Assert.assertTrue(right instanceof MySqlCharExpr);

                MySqlCharExpr mySqlCharExpr1 = (MySqlCharExpr) left;
                MySqlCharExpr mySqlCharExpr2 = (MySqlCharExpr) right;

                Assert.assertEquals(mySqlCharExpr1.getCharset(), "_gbk");
                Assert.assertEquals(mySqlCharExpr1.getCollate(), "gbk_chinese_ci");
                Assert.assertEquals(mySqlCharExpr1.getText(), "a");
                Assert.assertEquals(mySqlCharExpr1.isHex(), false);

                Assert.assertEquals(mySqlCharExpr2.getCharset(), "_big5");
                Assert.assertEquals(mySqlCharExpr2.getCollate(), "big5_bin");
                Assert.assertEquals(mySqlCharExpr2.getText(), "b");
                Assert.assertEquals(mySqlCharExpr2.isHex(), true);

                return false;
            }
        });
    }

    @Test
    public void testLiterals() {
        for (String s : cs) {
            String sql = "select _" + s + " 0xD0B0D0B2D0B2";
            SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.DrdsMisc);
            stmt.accept(new MySqlCharExprVisitor(s, null, "D0B0D0B2D0B2", true));

            sql = "select _" + s + " 0xD0B0D0B2D0B2 collate my_collate";
            stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.DrdsMisc);
            stmt.accept(new MySqlCharExprVisitor(s, "my_collate", "D0B0D0B2D0B2", true));

            sql = "select _" + s + " X'0xD0B0D0B2D0B2'";
            stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.DrdsMisc);
            stmt.accept(new MySqlCharExprVisitor(s, null, "0xD0B0D0B2D0B2", true));

            sql = "select _" + s + " X'0xD0B0D0B2D0B2' collate my_collate";
            stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.DrdsMisc);
            stmt.accept(new MySqlCharExprVisitor(s, "my_collate", "0xD0B0D0B2D0B2", true));

            sql = "select _" + s + " 'D0B0D0B2D0B2'";
            stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.DrdsMisc);
            stmt.accept(new MySqlCharExprVisitor(s, null, "D0B0D0B2D0B2", false));

            sql = "select _" + s + " 'D0B0D0B2D0B2' collate my_collate";
            stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.DrdsMisc);
            stmt.accept(new MySqlCharExprVisitor(s, "my_collate", "D0B0D0B2D0B2", false));
        }
    }

    private class MySqlCharExprVisitor extends MySqlASTVisitorAdapter {
        private String charset;
        private String collate;
        private String text;
        private boolean isHex;

        public MySqlCharExprVisitor(String charset, String collate, String text, boolean isHex) {
            this.charset = "_" + charset;
            this.collate = collate;
            this.text = text;
            this.isHex = isHex;
        }

        @Override
        public boolean visit(MySqlCharExpr x) {
            Assert.assertEquals(x.getCharset(), charset);
            Assert.assertEquals(x.getCollate(), collate);
            Assert.assertEquals(x.getText(), text);
            Assert.assertEquals(x.isHex(), isHex);
            return false;
        }
    }

    @Test
    public void testBinaryLiterals() throws IOException {
        byte[] sqlBytes = load("bvt/parser/mysql-charset.txt");
        ByteString sql = new ByteString(sqlBytes, Charsets.UTF_8);

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();
        SQLStatement stmt = stmtList.get(0);

        System.out.println(stmt.toString());

        Appendable buffer = new StringBuffer();
        List<Object> parameters = new ArrayList<Object>();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(buffer, true);
        visitor.setOutputParameters(parameters);
        stmt.accept(visitor);

        Assert.assertEquals("SELECT ? AS hello, ? AS hex, ? AS 岂, ? AS 岂, ? AS special", buffer.toString());
        Assert.assertArrayEquals(hexToBytes("68656C6C6F2CA1B2C3D421"), (byte[]) parameters.get(0));
        Assert.assertArrayEquals(hexToBytes("A1B2C3D4"), (byte[]) parameters.get(1));
        Assert.assertArrayEquals(hexToBytes("E88B9FF09F988E"), (byte[]) parameters.get(2));
        Assert.assertEquals("苟\uD83D\uDE0E", parameters.get(3));
        Assert.assertArrayEquals(hexToBytes("6162A1B2273132C3D45C2524A5B6"), (byte[]) parameters.get(4));
    }

    private byte[] load(String path) throws IOException {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        return IOUtils.toByteArray(inputStream);
    }
}

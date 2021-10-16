/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.parser;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.StringUtils;

public class SQLParserUtils {

    public static SQLStatementParser createSQLStatementParser(String sql, DbType dbType) {
        SQLParserFeature[] features;
        if (DbType.mysql == dbType) {
            features = new SQLParserFeature[] {SQLParserFeature.KeepComments};
        } else {
            features = new SQLParserFeature[] {};
        }
        return createSQLStatementParser(sql, dbType, features);
    }

    public static SQLStatementParser createSQLStatementParser(String sql, DbType dbType, boolean keepComments) {
        SQLParserFeature[] features;
        if (keepComments) {
            features = new SQLParserFeature[] {SQLParserFeature.KeepComments};
        } else {
            features = new SQLParserFeature[] {};
        }

        return createSQLStatementParser(sql, dbType, features);
    }

    public static SQLStatementParser createSQLStatementParser(String sql, String dbType, SQLParserFeature... features) {
        return createSQLStatementParser(sql, dbType == null ? null : DbType.valueOf(dbType), features);
    }

    public static SQLStatementParser createSQLStatementParser(String sqlString, DbType dbType,
                                                              SQLParserFeature... features) {
        return createSQLStatementParser(ByteString.from(sqlString), dbType, features);
    }

    public static SQLStatementParser createSQLStatementParser(ByteString sql, DbType dbType,
                                                              SQLParserFeature... features) {
        return new MySqlStatementParser(sql, features);
    }

    public static SQLExprParser createExprParser(String sqlString, DbType dbType, SQLParserFeature... features) {
        ByteString sql = ByteString.from(sqlString);
        return new MySqlExprParser(sql, features);
    }

    public static Lexer createLexer(String sql, DbType dbType) {
        return createLexer(sql, dbType, new SQLParserFeature[0]);
    }

    public static Lexer createLexer(String sqlString, DbType dbType, SQLParserFeature... features) {
        ByteString sql = ByteString.from(sqlString);

        return new MySqlLexer(sql);
    }

    public static SQLSelectQueryBlock createSelectQueryBlock(DbType dbType) {
        return new MySqlSelectQueryBlock();
    }

    public static SQLType getSQLType(String sql, DbType dbType) {
        Lexer lexer = createLexer(sql, dbType);
        return lexer.scanSQLType();
    }

    public static SQLType getSQLTypeV2(String sql, DbType dbType) {
        Lexer lexer = createLexer(sql, dbType);
        return lexer.scanSQLTypeV2();
    }

    public static boolean startsWithHint(String sql, DbType dbType) {
        Lexer lexer = createLexer(sql, dbType);
        lexer.nextToken();
        return lexer.token() == Token.HINT;
    }

    public static boolean containsAny(String sql, DbType dbType, Token token) {
        Lexer lexer = createLexer(sql, dbType);
        for (; ; ) {
            lexer.nextToken();
            final Token tok = lexer.token;
            switch (tok) {
            case EOF:
            case ERROR:
                return false;
            default:
                if (tok == token) {
                    return true;
                }
                break;
            }
        }
    }

    public static boolean containsAny(String sql, DbType dbType, Token token1, Token token2) {
        Lexer lexer = createLexer(sql, dbType);
        for (; ; ) {
            lexer.nextToken();
            final Token tok = lexer.token;
            switch (tok) {
            case EOF:
            case ERROR:
                return false;
            default:
                if (tok == token1 || tok == token2) {
                    return true;
                }
                break;
            }
        }
    }

    public static boolean containsAny(String sql, DbType dbType, Token token1, Token token2, Token token3) {
        Lexer lexer = createLexer(sql, dbType);
        for (; ; ) {
            lexer.nextToken();
            final Token tok = lexer.token;
            switch (tok) {
            case EOF:
            case ERROR:
                return false;
            default:
                if (tok == token1 || tok == token2 || tok == token3) {
                    return true;
                }
                break;
            }
        }
    }

    public static boolean containsAny(String sql, DbType dbType, Token... tokens) {
        if (tokens == null) {
            return false;
        }

        Lexer lexer = createLexer(sql, dbType);
        for (; ; ) {
            lexer.nextToken();
            final Token tok = lexer.token;
            switch (tok) {
            case EOF:
            case ERROR:
                return false;
            default:
                for (int i = 0; i < tokens.length; i++) {
                    if (tokens[i] == tok) {
                        return true;
                    }
                }
                break;
            }
        }
    }

    public static Object getSimpleSelectValue(String sql, DbType dbType) {
        return getSimpleSelectValue(sql, dbType, null);
    }

    public static Object getSimpleSelectValue(String sql, DbType dbType, SimpleValueEvalHandler handler) {
        Lexer lexer = createLexer(sql, dbType);
        lexer.nextToken();

        if (lexer.token != Token.SELECT && lexer.token != Token.VALUES) {
            return null;
        }

        lexer.nextTokenValue();

        SQLExpr expr = null;
        Object value;
        switch (lexer.token) {
        case LITERAL_INT:
            value = lexer.integerValue();
            break;
        case LITERAL_CHARS:
        case LITERAL_NCHARS:
            value = lexer.stringVal();
            break;
        case LITERAL_FLOAT:
            value = lexer.decimalValue();
            break;
        default:
            if (handler == null) {
                return null;
            }

            expr = new SQLExprParser(lexer).expr();
            try {
                value = handler.eval(expr);
            } catch (Exception error) {
                // skip
                value = null;
            }
            break;
        }

        lexer.nextToken();

        if (lexer.token == Token.FROM) {
            lexer.nextToken();
            if (lexer.token == Token.DUAL) {
                lexer.nextToken();
            } else {
                return null;
            }
        }
        if (lexer.token != Token.EOF) {
            return null;
        }

        return value;
    }

    public static interface SimpleValueEvalHandler {
        Object eval(SQLExpr expr);
    }

    public static String replaceBackQuote(String sql, DbType dbType) {
        int i = sql.indexOf('`');

        if (i == -1) {
            return sql;
        }

        char[] chars = sql.toCharArray();
        Lexer lexer = SQLParserUtils.createLexer(sql, dbType);

        int len = chars.length;
        int off = 0;

        for_:
        for (; ; ) {
            lexer.nextToken();

            int p0, p1;
            char c0, c1;
            switch (lexer.token) {
            case IDENTIFIER:
                p0 = lexer.startPos + off;
                p1 = lexer.pos - 1 + off;
                c0 = chars[p0];
                c1 = chars[p1];
                if (c0 == '`' && c1 == '`') {
                    if (p1 - p0 > 2 && chars[p0 + 1] == '\'' && chars[p1 - 1] == '\'') {
                        System.arraycopy(chars, p0 + 1, chars, p0, p1 - p0 - 1);
                        System.arraycopy(chars, p1 + 1, chars, p1 - 1, chars.length - p1 - 1);
                        len -= 2;
                        off -= 2;
                    } else {
                        chars[p0] = '"';
                        chars[p1] = '"';
                    }

                }
                break;
            case EOF:
            case ERROR:
                break for_;
            default:
                break;
            }
        }

        return new String(chars, 0, len);
    }

    public static String addBackQuote(String sql, DbType dbType) {
        if (StringUtils.isEmpty(sql)) {
            return sql;
        }
        SQLStatementParser parser = createSQLStatementParser(sql, dbType);
        StringBuffer buf = new StringBuffer(sql.length() + 20);
        SQLASTOutputVisitor out = SQLUtils.createOutputVisitor(buf, DbType.mysql);
        out.config(VisitorFeature.OutputNameQuote, true);

        SQLType sqlType = getSQLType(sql, dbType);
        if (sqlType == SQLType.INSERT) {

            parser.config(SQLParserFeature.InsertReader, true);

            SQLInsertStatement stmt = (SQLInsertStatement) parser.parseStatement();
            int startPos = parser.getLexer().startPos;

            stmt.accept(out);

            if (stmt.getQuery() == null) {
                buf.append(' ');
                buf.append(sql, startPos, sql.length());
            }
        } else {
            SQLStatement stmt = parser.parseStatement();
            stmt.accept(out);
        }

        return buf.toString();
    }
}

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

package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.exception.SqlParserException;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken.OP_MINUS;

/**
 * @author chenmo.cm
 */
public class SqlTypeParser {

    protected enum SpecialIdentifier {
        INFORMATION_SCHEMA,
        CHARSET, SHOW_INSTANCE_TYPE,
    }

    protected static final Map<String, SpecialIdentifier> specialIdentifiers = new HashMap<>();

    static {
        specialIdentifiers.put("INFORMATION_SCHEMA", SpecialIdentifier.INFORMATION_SCHEMA);
        specialIdentifiers.put("CHARSET", SpecialIdentifier.CHARSET);
    }

    private static final Logger logger = LoggerFactory.getLogger(SqlTypeParser.class);

    public static final SqlTypeParser INSTANCE = new SqlTypeParser();

    private SqlTypeParser() {
    }

    @Deprecated
    public static SqlType typeOf(String sql) {
        return INSTANCE.parseInner(sql);
    }

    private SqlType parseInner(String sql) throws SqlParserException {
        SqlType type = null;
        try {
            MySQLLexer lexer = new MySQLLexer(sql);

            boolean breakloop = false;
            while (true) {
                if (breakloop || lexer.token() == MySQLToken.EOF) {
                    break;
                }
                boolean needNext = true;

                switch (lexer.token()) {
                case KW_INSERT:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_LOW_PRIORITY:
                        type = SqlType.INSERT;
                        break;
                    case KW_DELAYED:
                        type = SqlType.INSERT;
                        break;
                    case KW_HIGH_PRIORITY:
                        type = SqlType.INSERT;
                        break;
                    case KW_IGNORE:
                        type = SqlType.INSERT;
                        break;
                    case KW_INTO:
                        type = SqlType.INSERT;
                        break;
                    case IDENTIFIER:
                        type = SqlType.INSERT;
                        break;
                    default:
                        needNext = false;
                        break;
                    }
                    break;
                case KW_UPDATE:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_LOW_PRIORITY:
                        type = SqlType.UPDATE;
                        break;
                    case KW_IGNORE:
                        type = SqlType.UPDATE;
                        break;
                    case IDENTIFIER:
                        if (type != SqlType.INSERT && type != SqlType.INSERT_INTO_SELECT) {
                            type = SqlType.UPDATE;
                        }
                        break;
                    case PUNC_LEFT_PAREN:
                        if (type != SqlType.INSERT && type != SqlType.INSERT_INTO_SELECT) {
                            // UPDATE with subquery
                            type = SqlType.UPDATE;
                        }
                        break;
                    default:
                        needNext = false;
                        break;
                    }
                    break;
                case KW_REPLACE:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_LOW_PRIORITY:
                        type = SqlType.REPLACE;
                        break;
                    case KW_DELAYED:
                        type = SqlType.REPLACE;
                        break;
                    case KW_INTO:
                        type = SqlType.REPLACE;
                        break;
                    case IDENTIFIER:
                        type = SqlType.REPLACE;
                        break;
                    default:
                        needNext = false;
                        break;
                    }
                    break;
                case KW_FOR:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_UPDATE:
                        type = SqlType.SELECT_FOR_UPDATE;
                        break;
                    default:
                        needNext = false;
                        break;
                    }
                    break;
                case KW_DESC:
                case KW_DESCRIBE:
                    if (type == null) {
                        type = SqlType.DESC;
                        breakloop = true;
                    }
                    break;
                case KW_FROM:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case IDENTIFIER:
                        break;
                    default:
                        needNext = false;
                        break;
                    }
                    break;
                case KW_DELETE:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_LOW_PRIORITY:
                        type = SqlType.DELETE;
                        break;
                    case KW_IGNORE:
                        type = SqlType.DELETE;
                        break;
                    case KW_FROM:
                        type = SqlType.DELETE;
                        break;
                    case IDENTIFIER: // tablename or QUICK
                        type = SqlType.DELETE;
                        break;
                    default:
                        needNext = false;
                        break;
                    }
                    break;
                case PUNC_SEMICOLON:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case EOF:
                        needNext = false;
                        break;
                    default:
                        needNext = false;
                        break;
                    }
                    break;
                case KW_SHOW:
                    type = SqlType.SHOW;
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case IDENTIFIER:
                        break;
                    }
                    breakloop = true;
                    break;
                case KW_SELECT:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_SQL_CALC_FOUND_ROWS:
                        breakloop = true;
                        break;
                    default:
                        break;
                    }
                    break;
                case IDENTIFIER:
                    break;
                case OP_MINUS:
                    // TODO -- mast at the beginning of line
                    if (lexer.getCurrentIndex() == 1) {
                        lexer.nextToken();
                        if (OP_MINUS == lexer.token()) {
                            breakloop = true;
                        }
                    }
                    break;
                default:
                    break;
                } // end of switch

                if (breakloop) {
                    break;
                }

                if (needNext) {
                    lexer.nextToken();
                }
            }

            return type;

        } catch (SQLSyntaxErrorException e) {
            logger.warn("SqlTypeParser cannot get the type of SQL: " + sql + " cause: " + e.getMessage());
            return null;
        }
    }

}

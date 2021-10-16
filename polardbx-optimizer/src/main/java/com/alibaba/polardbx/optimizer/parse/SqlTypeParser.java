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

import com.alibaba.polardbx.druid.sql.SQLUtils;

import static com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken.IDENTIFIER;
import static com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken.KW_INDEX;
import static com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken.OP_MINUS;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;

import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.exception.SqlParserException;

/**
 * @author chenmo.cm
 */
public class SqlTypeParser {

    protected enum SpecialIdentifier {
        TRUNCATE, MERGE, DUMP, DEBUG, SAVEPOINT, RELOAD,
        NEXTVAL, INSPECT, RESYNC, TABLES, SYSTEM_VARIABLE, INFORMATION_SCHEMA, XA,
        CHARSET, SHOW_INSTANCE_TYPE, BASELINE, MOVE_DATABASE, CCL_RULE, CCL_RULES
    }

    protected static final Map<String, SpecialIdentifier> specialIdentifiers = new HashMap<>();

    static {
        specialIdentifiers.put("TRUNCATE", SpecialIdentifier.TRUNCATE);

        specialIdentifiers.put("MERGE", SpecialIdentifier.MERGE);
        specialIdentifiers.put("DUMP", SpecialIdentifier.DUMP);
        specialIdentifiers.put("DEBUG", SpecialIdentifier.DEBUG);
        specialIdentifiers.put("SAVEPOINT", SpecialIdentifier.SAVEPOINT);
        specialIdentifiers.put("RELOAD", SpecialIdentifier.RELOAD);
        specialIdentifiers.put("NEXTVAL", SpecialIdentifier.NEXTVAL);

        specialIdentifiers.put("TABLES", SpecialIdentifier.TABLES);
        specialIdentifiers.put("INFORMATION_SCHEMA", SpecialIdentifier.INFORMATION_SCHEMA);
        specialIdentifiers.put("XA", SpecialIdentifier.XA);
        specialIdentifiers.put("CHARSET", SpecialIdentifier.CHARSET);
        specialIdentifiers.put("INSTANCE_TYPE", SpecialIdentifier.SHOW_INSTANCE_TYPE);

        specialIdentifiers.put("CCL_RULE", SpecialIdentifier.CCL_RULE);
        specialIdentifiers.put("CCL_RULES", SpecialIdentifier.CCL_RULES);
    }

    private static final Logger logger = LoggerFactory.getLogger(SqlTypeParser.class);

    public static final SqlTypeParser INSTANCE = new SqlTypeParser();

    private SqlTypeParser() {
    }

    public static SqlType typeOf(String sql) {
        return INSTANCE.parseInner(sql);
    }

    private SqlType parseInner(String sql) throws SqlParserException {
        SqlType type = null;
        try {
            MySQLLexer lexer = new MySQLLexer(sql);

            SpecialIdentifier si = null;
            boolean isMulti = false;
            boolean breakloop = false;
            boolean moreThanOneToken = false;
            SqlType firstTypeOfMulti = SqlType.MULTI_STATEMENT;
            while (true) {
                if (breakloop || lexer.token() == MySQLToken.EOF) {
                    break;
                }
                boolean needNext = true;
                moreThanOneToken = true;

                switch (lexer.token()) {
                case KW_EXPLAIN:
                    type = SqlType.EXPLAIN;
                    breakloop = true;
                    break;

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
                    case KW_UPDATE:
                        lexer.nextToken();
                        switch (lexer.token()) {
                        case KW_LOW_PRIORITY:
                            type = SqlType.SELECT_FROM_UPDATE;
                            break;
                        case KW_IGNORE:
                            type = SqlType.SELECT_FROM_UPDATE;
                            break;
                        case IDENTIFIER:
                            type = SqlType.SELECT_FROM_UPDATE;
                            break;
                        default:
                            needNext = false;
                            break;
                        }
                        break;
                    case IDENTIFIER:
                        if (type == SqlType.SELECT_WITHOUT_TABLE) {
                            type = SqlType.SELECT;
                        }

                        si = specialIdentifiers.get(SQLUtils.normalizeNoTrim(lexer.stringValueUppercase()));
                        if (null != si) {

                            switch (si) {
                            case INFORMATION_SCHEMA:
                                type = SqlType.GET_INFORMATION_SCHEMA;
                                breakloop = true;
                                break;
                            default:
                                break;
                            }
                        }
                        break;
                    default:
                        needNext = false;
                        break;
                    }
                    break;
                case KW_SET:
                    if (type == null) {
                        breakloop = true;
                        type = SqlType.SET;
                    }
                    break;
                case KW_UNION:
                    if (type == SqlType.SELECT) {
                        type = SqlType.SELECT_UNION;
                    }
                    break;
                case KW_CALL:
                    type = SqlType.PROCEDURE;
                    breakloop = true;
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
                case KW_ALTER:
                    breakloop = true;
                    type = SqlType.ALTER;
                    break;
                case KW_LOAD:
                    breakloop = true;
                    type = SqlType.LOAD;
                    break;
                case KW_KILL:
                    breakloop = true;
                    type = SqlType.KILL;
                    break;
                case KW_LOCK:
                    // skip statements of 'select ... lock in share mode'
                    if (type == null) {
                        breakloop = true;
                        type = SqlType.LOCK_TABLES;
                    }
                    break;
                case KW_UNLOCK:
                    breakloop = true;
                    type = SqlType.UNLOCK_TABLES;
                    break;
                case KW_CHECK:
                    breakloop = true;
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_TABLE:
                        type = SqlType.CHECK_TABLE;
                        break;
                    case IDENTIFIER:
                        if ("GLOBAL".equals(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            if (lexer.token() == KW_INDEX) {
                                type = SqlType.CHECK_GLOBAL_INDEX;
                            }
                        }
                        break;
                    default:
                    }
                    break;
                case KW_DROP:
                    breakloop = true;
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_TABLE:
                        type = SqlType.DROP_TABLE;
                        break;
                    case IDENTIFIER:
                        si = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (si != null) {
                            switch (si) {
                            case CCL_RULE:
                                type = SqlType.DROP_CCL_RULE;
                                break;
                            }
                        } else {
                            type = SqlType.DROP;
                        }
                        break;
                    default:
                        type = SqlType.DROP;
                        break;
                    }
                    break;
                case KW_CREATE:
                    breakloop = true;
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_VIEW:
                        type = SqlType.CREATE_VIEW;
                        break;
                    case KW_TABLE:
                        type = SqlType.CREATE_TABLE;
                        break;
                    case IDENTIFIER:
                        si = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (si != null) {
                            switch (si) {
                            case CCL_RULE:
                                type = SqlType.CREATE_CCL_RULE;
                                break;
                            }
                        } else {
                            type = SqlType.CREATE;
                        }
                        break;
                    default:
                        type = SqlType.CREATE;
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
                        if (!isMulti || firstTypeOfMulti == null) {
                            // 考虑语句开始就是分号的情况
                            firstTypeOfMulti = type;
                        }
                        isMulti = true;
                        type = SqlType.MULTI_STATEMENT;
                        needNext = false;
                        break;
                    }
                    break;
                case KW_SHOW:
                    type = SqlType.SHOW;
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case IDENTIFIER:
                        si = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (si != null) {
                            switch (si) {
                            case CHARSET:
                                type = SqlType.SHOW_CHARSET;
                                break;
                            case SHOW_INSTANCE_TYPE:
                                type = SqlType.SHOW_INSTANCE_TYPE;
                                break;
                            case CCL_RULE:
                                type = SqlType.SHOW_CCL_RULE;
                                break;
                            }
                        }
                        break;
                    }
                    breakloop = true;
                    break;
                case KW_RENAME:
                    type = SqlType.RENAME;
                    breakloop = true;
                    break;
                case KW_OPTIMIZE:
                    type = SqlType.OPTIMIZE_TABLE;
                    breakloop = true;
                    break;
                case KW_ANALYZE:
                    type = SqlType.ANALYZE_TABLE;
                    breakloop = true;
                    break;
                case KW_SELECT:
                    if (type == null) {
                        type = SqlType.SELECT_WITHOUT_TABLE; // 没碰到from之前，先认为是select_without_table
                    }

                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_SQL_CALC_FOUND_ROWS:
                        if (type == null) {
                            type = SqlType.SELECT_WITHOUT_TABLE;
                        }
                        breakloop = true;
                        break;
                    case SYS_VAR:
                        type = SqlType.GET_SYSTEM_VARIABLE;
                        //breakloop = true;
                        break;
                    default:
                        break;
                    }
                    break;
                case SYS_VAR:
                    type = SqlType.GET_SYSTEM_VARIABLE;
                    //breakloop = true;
                    break;
                case KW_PURGE:
                    type = SqlType.PURGE;
                    breakloop = true;
                    break;
                case IDENTIFIER:
                    si = specialIdentifiers.get(lexer.stringValueUppercase());
                    if (si != null) {
                        switch (si) {
                        case TRUNCATE:
                            if (null == type) {
                                type = SqlType.TRUNCATE;
                                breakloop = true;
                            }
                            break;
                        case MERGE:
                            type = SqlType.MERGE;
                            breakloop = true;
                            break;
                        case DUMP:
                            type = SqlType.DUMP;
                            breakloop = true;
                            break;
                        case DEBUG:
                            type = SqlType.DEBUG;
                            breakloop = true;
                            break;
                        case SAVEPOINT:
                            type = SqlType.SAVE_POINT;
                            breakloop = true;
                            break;
                        case RELOAD:
                            if (null == type) {
                                breakloop = true;
                                type = SqlType.RELOAD;
                            }
                            break;
                        case NEXTVAL:
                            breakloop = true;
                            type = SqlType.GET_SEQUENCE;
                            break;
                        case INFORMATION_SCHEMA:
                            breakloop = true;
                            type = SqlType.GET_INFORMATION_SCHEMA;
                            break;
                        case BASELINE:
                            breakloop = true;
                            type = SqlType.BASELINE;
                            break;
                        case MOVE_DATABASE:
                            breakloop = true;
                            type = SqlType.MOVE_DATABASE;
                            break;

                        default:
                            break;
                        }
                    } else if ("CANCEL".equals(lexer.stringValueUppercase()) ||
                        "RECOVER".equals(lexer.stringValueUppercase()) ||
                        "ROLLBACK".equals(lexer.stringValueUppercase()) ||
                        "REMOVE".equals(lexer.stringValueUppercase()) ||
                        "INSPECT".equals(lexer.stringValueUppercase()) ||
                        "CLEAR".equals(lexer.stringValueUppercase())) {
                        lexer.nextToken();
                        if (IDENTIFIER == lexer.token() && "DDL".equals(lexer.stringValueUppercase())) {
                            breakloop = true;
                            type = SqlType.ASYNC_DDL;
                        }
                        if (IDENTIFIER == lexer.token() && "CCL_RULES".equals(lexer.stringValueUppercase())) {
                            breakloop = true;
                            type = SqlType.CLEAR_CCL_RULES;
                        }

                    } else if ("BASELINE".equals(lexer.stringValueUppercase())) {
                        breakloop = true;
                        type = SqlType.BASELINE;
                    }
                    break;
                case OP_MINUS:
                    // TODO -- mast at the beginning of line
                    if (lexer.getCurrentIndex() == 1) {
                        lexer.nextToken();
                        if (OP_MINUS == lexer.token()) {
                            type = SqlType.COMMENT;
                            breakloop = true;
                        }
                    }
                    break;
                case KW_CHANGE:
                    lexer.nextToken();
                    if (IDENTIFIER == lexer.token() && "DDL".equals(lexer.stringValueUppercase())) {
                        breakloop = true;
                        type = SqlType.ASYNC_DDL;
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

            if (isMulti) {
                type = SqlType.MULTI_STATEMENT;
                if (null != firstTypeOfMulti) {
                    switch (firstTypeOfMulti) {
                    case UPDATE:
                        type = SqlType.MULTI_STATEMENT_UPDATE;
                        break;
                    case DELETE:
                        type = SqlType.MULTI_STATEMENT_DELETE;
                        break;
                    case INSERT:
                        type = SqlType.MULTI_STATEMENT_INSERT;
                        break;
                    } // end of switch
                } // end of else
            } // end of if

            if (!moreThanOneToken && TStringUtil.isNotBlank(sql)) {
                type = SqlType.COMMENT;
            }

            return type;

        } catch (SQLSyntaxErrorException e) {
            logger.warn("SqlTypeParser cannot get the type of SQL: " + sql + " cause: " + e.getMessage());
            return null;
        }
    }

}

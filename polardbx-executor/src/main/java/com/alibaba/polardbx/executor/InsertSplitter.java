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

package com.alibaba.polardbx.executor;

import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.common.constants.BatchInsertAttribute;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.cursor.MultiResultCursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * For split, only supports syntax below:
 * <p>
 * [COMMENT, ...]
 * {INSERT | REPLACE} [IGNORE]
 * [INTO] [schema.]table_name
 * [(column_name, ...)]
 * {VALUES | VALUE} (valueList), ...
 * [ON DUPLICATE KEY UPDATE ...]
 */
public class InsertSplitter {

    /**
     * Num of values in each split sql
     */
    private long sqlSplitEachValues = BatchInsertAttribute.BATCH_INSERT_CHUNK_SIZE_DEFAULT;

    /**
     * When inserting into broadcast table, transaction policy may be modified.
     */
    private Function<ByteString, ResultCursor> executeFirstQuery;
    private Function<ByteString, ResultCursor> executeOtherQuery;

    /**
     * 1. in SPLIT mode.
     * 2. sql size is bigger than 256K.
     */
    public static boolean needSplit(ByteString sql, BatchInsertPolicy policy, ExecutionContext executionContext) {
        if (policy == BatchInsertPolicy.SPLIT) {
            if (sql.length() < executionContext.getParamManager().getLong(ConnectionParams.MAX_BATCH_INSERT_SQL_LENGTH)
                * 1024) {
                return false;
            }

            return true;
        }

        return false;
    }

    /**
     * When the sql is not expected INSERT statement, fallback to normal execution,
     * Which is happened to be the same with `firstTimeExecute`.
     */
    private ResultCursor normalExecute(ByteString sql) {
        return executeFirstQuery.apply(sql);
    }

    /**
     * Called by TConnection.executeSQL.
     */
    public ResultCursor execute(ByteString sql, ExecutionContext executionContext, BatchInsertPolicy policy,
                                Function<ByteString, ResultCursor> executeWithUpdateTransactionPolicy,
                                Function<ByteString, ResultCursor> executeQuery) {
        this.sqlSplitEachValues = executionContext.getParamManager().getLong(ConnectionParams.BATCH_INSERT_CHUNK_SIZE);

        this.executeFirstQuery = executeWithUpdateTransactionPolicy;
        this.executeOtherQuery = executeQuery;

        if (policy == BatchInsertPolicy.SPLIT) {
            return executeSplit(sql, executionContext);
        } else {
            return normalExecute(sql);
        }
    }

    /**
     * 1. parse hint+insertPrefix, values, suffix.
     * 2. composite new sqls, record last insert id.
     * 3. add affect rows and return ResultCursor.
     */
    private ResultCursor executeSplit(ByteString sql, ExecutionContext executionContext) {
        int affectRows = 0;

        // Lexer doesn't support escape characters, but MySqlLexer does.
        Lexer lexer = new MySqlLexer(sql);
        ByteString insertPrefix = parsePrefix(lexer);
        if (insertPrefix == null || insertPrefix.length() == 0) {
            return normalExecute(sql);
        }
        int pos = lexer.pos();

        int valuesNum = skipValues(lexer);
        if (valuesNum == 0) {
            return normalExecute(sql);
        }

        ByteString duplicateSuffix = parseSuffix(lexer);
        if (duplicateSuffix == null) {
            return normalExecute(sql);
        }

        // Used in assigning sequence to ensure the continuity of sequence values.
        Parameters parameters = executionContext.getParams();
        if (parameters != null) {
            parameters.getSequenceSize().set(valuesNum);
            parameters.getSequenceIndex().set(0);
        }

        lexer.reset(pos);
        long lastInsertId = 0;
        long returnedLastInsertId = 0;
        boolean isFirstTimeExecute = true;
        // reset prepared flag during inner execution
        executionContext.setIsExecutingPreparedStmt(false);
        do {
            ByteString values = parseValues(lexer);
            if (values == null || values.isEmpty()) {
                break;
            }

            ByteBuffer buffer =
                ByteBuffer.allocate(insertPrefix.length() + values.length() + duplicateSuffix.length() + 1);
            buffer.put(insertPrefix.getBytes());
            buffer.put(values.getBytes());
            buffer.put((byte) ' ');
            buffer.put(duplicateSuffix.getBytes());

            ByteString nextSQL = new ByteString(buffer.array(), 0, buffer.position(), sql.getCharset());
            affectRows += innerExecute(nextSQL, executionContext, isFirstTimeExecute);
            isFirstTimeExecute = false;

            // Last insert id must be in the first batch.
            // As though sequence is assigned once, inserting into single db still needs extra care.
            if (lastInsertId == 0) {
                lastInsertId = executionContext.getConnection().getLastInsertId();
            }
            if (returnedLastInsertId == 0) {
                returnedLastInsertId = executionContext.getConnection().getReturnedLastInsertId();
            }
        } while (true);

        executionContext.getConnection().setLastInsertId(lastInsertId);
        executionContext.getConnection().setReturnedLastInsertId(returnedLastInsertId);

        AffectRowCursor arc = new AffectRowCursor(affectRows);
        return new ResultCursor(arc);
    }

    /**
     * Add this to see if it's in optimization in jstack.
     */
    private int innerExecute(ByteString sql, ExecutionContext executionContext, boolean isFirstTimeExecute) {
        executionContext.setSql(sql);
        if (executionContext.getParams() != null) {
            executionContext.getParams().clear();
        }

        ResultCursor resultCursor;
        if (isFirstTimeExecute) {
            /*
             * The first batch should check if it's needed to modify transaction policy.
             * When inserting into broadcast table, XA transaction may be needed.
             * Following execution should just execute without modifying transaction policy.
             */
            resultCursor = executeFirstQuery.apply(sql);
        } else {
            resultCursor = executeOtherQuery.apply(sql);
        }

        if (resultCursor instanceof MultiResultCursor) {
            return collectAffectNum((MultiResultCursor) resultCursor);
        }

        Row row = resultCursor.getCursor().next();
        if (row != null) {
            return row.getInteger(0);
        }
        return 0;
    }

    private int collectAffectNum(MultiResultCursor resultCursor) {
        int rs = 0;
        for (ResultCursor cursor : resultCursor.getResultCursors()) {
            if (cursor.getCursor() != null) {
                rs += cursor.getCursor().next().getInteger(0);
            }
        }
        return rs;
    }

    /**
     * hint + insert into tb values
     */
    private ByteString parsePrefix(Lexer lexer) {
        String comments = parseComments(lexer);
        if (comments == null) {
            return null;
        }

        if (lexer.token() != Token.REPLACE && lexer.token() != Token.INSERT) {
            return null;
        }

        // There may be comments after INSERT
        comments = parseComments(lexer);
        if (comments == null) {
            return null;
        }

        if (lexer.identifierEquals("IGNORE")) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.INTO) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.IDENTIFIER) {
            lexer.nextToken();

            if (lexer.token() == Token.DOT) {
                lexer.nextToken();

                if (lexer.token() == Token.IDENTIFIER) {
                    lexer.nextToken();
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }

        if (lexer.token() == Token.LPAREN) {
            while (lexer.token() != Token.RPAREN) {
                lexer.nextToken();
                switch (lexer.token()) {
                case IDENTIFIER:
                case RPAREN:
                case COMMA:
                    break;
                default:
                    return null;
                }
            }
            lexer.nextToken();
        }

        if (lexer.token() != Token.VALUES && !lexer.identifierEquals("VALUE")) {
            return null;
        }

        return lexer.text.slice(0, lexer.pos());
    }

    private String parseComments(Lexer lexer) {
        int startPos = lexer.pos();
        int endPos = lexer.pos();

        do {
            lexer.nextToken();

            if (lexer.token() == Token.COMMENT ||
                lexer.token() == Token.LINE_COMMENT ||
                lexer.token() == Token.MULTI_LINE_COMMENT) {
                endPos = lexer.pos();

            } else if (lexer.token() == Token.SLASH) {

                int i = lexer.pos();
                if (lexer.charAt(i) == '!') {
                    for (i++; ; i++) {
                        char ch = lexer.charAt(i);
                        if (ch == 26) {
                            return null;
                        } else if (ch == '*' && lexer.charAt(i + 1) == '/') {
                            lexer.reset(i + 2);
                            endPos = i + 2;
                            break;
                        }
                    }
                } else {
                    return null;
                }
            } else {
                break;
            }

        } while (true);

        return lexer.subString(startPos, endPos - startPos);
    }

    /**
     * Count how many values in this sql. 0 stands for error.
     */
    private int skipValues(Lexer lexer) {
        int valuesNum = 0;

        lexer.nextToken();
        while (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            for (int lparenNum = 1; lparenNum > 0; ) {
                switch (lexer.token()) {
                case LPAREN:
                    lparenNum++;
                    break;
                case RPAREN:
                    lparenNum--;
                    break;
                case EOF:
                case COMMENT:
                case MULTI_LINE_COMMENT:
                case LINE_COMMENT:
                    /*
                     * No need to split insert for prepared statement, because
                     * {@link com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor#visit(com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement)}
                     * will compress values part for bulk insert
                     */
                case QUES:
                    return 0;
                default:
                    break;
                }

                if (lparenNum > 0) {
                    lexer.nextToken();
                }
            }

            lexer.nextToken();

            valuesNum++;

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();

                if (lexer.token() != Token.LPAREN) {
                    return 0;
                }
            }
        }

        return valuesNum;
    }

    /**
     * Return next batch of values.
     */
    private ByteString parseValues(Lexer lexer) {
        int curValuesNum = 0;

        int startPos = lexer.pos(), endPos = startPos;

        lexer.nextToken();
        while (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            for (int lparenNum = 1; lparenNum > 0; ) {
                switch (lexer.token()) {
                case LPAREN:
                    lparenNum++;
                    break;
                case RPAREN:
                    lparenNum--;
                    break;
                case EOF:
                case COMMENT:
                case MULTI_LINE_COMMENT:
                case LINE_COMMENT:
                    return null;
                default:
                    break;
                }

                if (lparenNum > 0) {
                    lexer.nextToken();
                }
            }

            endPos = lexer.pos();
            lexer.nextToken();

            curValuesNum++;
            if (curValuesNum >= sqlSplitEachValues) {
                break;
            }

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();

                if (lexer.token() != Token.LPAREN) {
                    return null;
                }
            }
        }

        return lexer.text.slice(startPos, endPos);
    }

    /**
     * ON DUPLICATE KEY UPDATE ...
     */
    private ByteString parseSuffix(Lexer lexer) {
        if (lexer.token() == Token.ON) {
            int startPos = lexer.pos() - 2;

            lexer.nextToken();

            if (!lexer.identifierEquals("DUPLICATE")) {
                return null;
            }
            lexer.nextToken();

            if (lexer.token() != Token.KEY) {
                return null;
            }
            lexer.nextToken();

            if (lexer.token() != Token.UPDATE) {
                return null;
            }

            return lexer.text.slice(startPos, lexer.text.length());
        } else if (lexer.token() == Token.SEMI) {
            lexer.nextToken();

            if (lexer.token() != Token.EOF) {
                return null;
            }
        } else if (lexer.token() != Token.EOF) {
            return null;
        }

        return ByteString.EMPTY;
    }
}

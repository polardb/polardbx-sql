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
package com.alibaba.polardbx.druid.sql.dialect.mysql.parser;

import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLSizeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSampling;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnnestTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlIndexHintImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateTableSource;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLExprParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLSelectListCache;
import com.alibaba.polardbx.druid.sql.parser.SQLSelectParser;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.util.ArrayList;
import java.util.List;

public class MySqlSelectParser extends SQLSelectParser {

    protected boolean returningFlag = false;
    protected MySqlUpdateStatement updateStmt;

    public MySqlSelectParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public MySqlSelectParser(SQLExprParser exprParser, SQLSelectListCache selectListCache) {
        super(exprParser, selectListCache);
    }

    public MySqlSelectParser(ByteString sql) {
        this(new MySqlExprParser(sql));
    }

    public void parseFrom(SQLSelectQueryBlock queryBlock) {
        if (lexer.token() == Token.EOF
            || lexer.token() == Token.SEMI
            || lexer.token() == Token.ORDER
            || lexer.token() == Token.RPAREN
            || lexer.token() == Token.UNION
        ) {
            return;
        }

        if (lexer.token() != Token.FROM) {
            for (SQLSelectItem item : queryBlock.getSelectList()) {
                SQLExpr expr = item.getExpr();
                if (expr instanceof SQLAggregateExpr) {
                    throw new ParserException(
                        "syntax error, expect " + Token.FROM + ", actual " + lexer.token() + ", " + lexer.info());
                }
            }
            return;
        }

        lexer.nextTokenIdent();

        while (lexer.token() == Token.HINT) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.UPDATE) { // returning syntax: SELECT FROM UPDATE
            updateStmt = this.parseUpdateStatment();
            List<SQLExpr> returnning = updateStmt.getReturning();
            for (SQLSelectItem item : queryBlock.getSelectList()) {
                SQLExpr itemExpr = item.getExpr();
                itemExpr.setParent(updateStmt);
                returnning.add(itemExpr);
            }
            returningFlag = true;
            return;
        }

        SQLTableSource from = parseTableSource(queryBlock);
        queryBlock.setFrom(from);
    }

    @Override
    public SQLSelectQuery query(SQLObject parent, boolean acceptUnion) {
        List<SQLCommentHint> hints = null;
        if (lexer.token() == Token.HINT) {
            hints = this.exprParser.parseHints();
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            SQLSelectQuery select = query();
            select.setBracket(true);
            accept(Token.RPAREN);

            return queryRest(select, acceptUnion);
        }

        if (lexer.token() == Token.VALUES) {
            return valuesQuery(acceptUnion);
        }

        MySqlSelectQueryBlock queryBlock = new MySqlSelectQueryBlock();
        queryBlock.setParent(parent);

        class QueryHintHandler implements Lexer.CommentHandler {
            private MySqlSelectQueryBlock queryBlock;
            private Lexer lexer;

            QueryHintHandler(MySqlSelectQueryBlock queryBlock, Lexer lexer) {
                this.queryBlock = queryBlock;
                this.lexer = lexer;
            }

            @Override
            public boolean handle(Token lastToken, String comment) {
                if (lexer.isEnabled(SQLParserFeature.TDDLHint)
                    && (comment.startsWith("+ TDDL")
                    || comment.startsWith("+TDDL")
                    || comment.startsWith("!TDDL")
                    || comment.startsWith("TDDL"))) {
                    SQLCommentHint hint = new TDDLHint(comment);

                    if (lexer.getCommentCount() > 0) {
                        hint.addBeforeComment(lexer.getComments());
                    }

                    queryBlock.getHints().add(hint);

                    lexer.nextToken();
                }
                return false;
            }
        }

        this.lexer.setCommentHandler(new QueryHintHandler(queryBlock, this.lexer));

        if (lexer.hasComment() && lexer.isKeepComments()) {
            queryBlock.addBeforeComment(lexer.readAndResetComments());
        }

        if (lexer.token() == Token.SELECT) {
            if (selectListCache != null) {
                selectListCache.match(lexer, queryBlock);
            }
        }

        if (lexer.token() == Token.SELECT) {
            lexer.nextTokenValue();

            for (; ; ) {
                if (lexer.token() == Token.HINT) {
                    this.exprParser.parseHints(queryBlock.getHints());
                } else {
                    break;
                }
            }

            while (true) {
                Token token = lexer.token();
                if (token == (Token.DISTINCT)) {
                    queryBlock.setDistionOption(SQLSetQuantifier.DISTINCT);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.DISTINCTROW)) {
                    queryBlock.setDistionOption(SQLSetQuantifier.DISTINCTROW);
                    lexer.nextToken();
                } else if (token == (Token.ALL)) {
                    queryBlock.setDistionOption(SQLSetQuantifier.ALL);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.HIGH_PRIORITY)) {
                    queryBlock.setHignPriority(true);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.STRAIGHT_JOIN)) {
                    queryBlock.setStraightJoin(true);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.SQL_SMALL_RESULT)) {
                    queryBlock.setSmallResult(true);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.SQL_BIG_RESULT)) {
                    queryBlock.setBigResult(true);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.SQL_BUFFER_RESULT)) {
                    queryBlock.setBufferResult(true);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.SQL_CACHE)) {
                    queryBlock.setCache(true);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.SQL_NO_CACHE)) {
                    queryBlock.setCache(false);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.SQL_CALC_FOUND_ROWS)) {
                    queryBlock.setCalcFoundRows(true);
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.TOP)) {
                    Lexer.SavePoint mark = lexer.mark();

                    lexer.nextToken();
                    if (lexer.token() == Token.LITERAL_INT) {
                        SQLLimit limit = new SQLLimit(lexer.integerValue().intValue());
                        queryBlock.setLimit(limit);
                        lexer.nextToken();
                    } else if (lexer.token() == Token.DOT) {
                        lexer.reset(mark);
                        break;
                    }
                } else {
                    break;
                }
            }

            parseSelectList(queryBlock);

            if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
                lexer.nextToken();
                accept(Token.PARTITION);
                SQLName partition = this.exprParser.name();
                queryBlock.setForcePartition(partition);
            }

            parseInto(queryBlock);
        }

        parseFrom(queryBlock);

        parseWhere(queryBlock);

        parseHierachical(queryBlock);

        if (lexer.token() == Token.GROUP || lexer.token() == Token.HAVING) {
            parseGroupBy(queryBlock);
        }

        if (lexer.identifierEquals(FnvHash.Constants.WINDOW)) {
            parseWindow(queryBlock);
        }

        if (lexer.token() == Token.ORDER) {
            queryBlock.setOrderBy(this.exprParser.parseOrderBy());
        }

        if (lexer.token() == Token.LIMIT) {
            queryBlock.setLimit(this.exprParser.parseLimit());
        }

        if (lexer.token() == Token.FETCH) {
            final Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.identifierEquals(FnvHash.Constants.NEXT)) {
                lexer.nextToken();
                SQLExpr rows = this.exprParser.primary();
                queryBlock.setLimit(
                    new SQLLimit(rows));
                acceptIdentifier("ROWS");
                acceptIdentifier("ONLY");
            } else if (lexer.isEnabled(SQLParserFeature.DrdsMisc) && lexer.identifierEquals(FnvHash.Constants.FIRST)) {
                // 'FETCH FIRST [<n>] [ROW|ROWS] ONLY' clause will be translated to 'LIMIT <n>'
                lexer.nextToken();
                if (lexer.token() == Token.ROW || lexer.identifierEquals("ROWS")) {
                    queryBlock.setFirst(new SQLIntegerExpr(1));
                    lexer.nextToken();
                } else {
                    SQLExpr first = this.exprParser.primary();
                    queryBlock.setFirst(first);
                    if (lexer.token() == Token.ROW || lexer.identifierEquals("ROWS")) {
                        lexer.nextToken();
                    } else {
                        throw new ParserException("syntax error, expect ROW or ROWS, actual " + lexer.info());
                    }
                }
                acceptIdentifier("ONLY");
            } else {
                lexer.reset(mark);
            }
        }

        if (lexer.token() == Token.PROCEDURE) {
            lexer.nextToken();
            throw new ParserException("TODO. " + lexer.info());
        }

        if (lexer.token() == Token.INTO) {
            parseInto(queryBlock);
        }

        if (lexer.token() == Token.FOR) {
            lexer.nextToken();
            accept(Token.UPDATE);

            queryBlock.setForUpdate(true);

            if (lexer.identifierEquals(FnvHash.Constants.NO_WAIT) || lexer.identifierEquals(FnvHash.Constants.NOWAIT)) {
                lexer.nextToken();
                queryBlock.setNoWait(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.WAIT)) {
                lexer.nextToken();
                SQLExpr waitTime = this.exprParser.primary();
                queryBlock.setWaitTime(waitTime);
            }
        }

        if (lexer.token() == Token.LOCK) {
            lexer.nextToken();
            accept(Token.IN);
            acceptIdentifier("SHARE");
            acceptIdentifier("MODE");
            queryBlock.setLockInShareMode(true);
        }

        if (hints != null) {
            queryBlock.setHints(hints);
        }

        return queryRest(queryBlock, acceptUnion);
    }

    public SQLTableSource parseTableSource() {
        return parseTableSource(null);
    }

    public SQLTableSource parseTableSource(SQLObject parent) {
        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            List hints = null;
            if (lexer.token() == Token.HINT) {
                hints = new ArrayList();
                this.exprParser.parseHints(hints);
            }

            SQLTableSource tableSource;
            if (lexer.token() == Token.SELECT || lexer.token() == Token.WITH) {
                SQLSelect select = select();

                accept(Token.RPAREN);

                SQLSelectQueryBlock innerQuery = select.getQueryBlock();

                boolean noOrderByAndLimit = innerQuery instanceof SQLSelectQueryBlock
                    && ((SQLSelectQueryBlock) innerQuery).getOrderBy() == null
                    && ((SQLSelectQueryBlock) select.getQuery()).getLimit() == null;

                if (lexer.token() == Token.LIMIT) {
                    SQLLimit limit = this.exprParser.parseLimit();
                    if (parent != null && parent instanceof SQLSelectQueryBlock) {
                        ((SQLSelectQueryBlock) parent).setLimit(limit);
                    }
                    if (parent == null && noOrderByAndLimit) {
                        innerQuery.setLimit(limit);
                    }
                } else if (lexer.token() == Token.ORDER) {
                    SQLOrderBy orderBy = this.exprParser.parseOrderBy();
                    if (parent != null && parent instanceof SQLSelectQueryBlock) {
                        ((SQLSelectQueryBlock) parent).setOrderBy(orderBy);
                    }
                    if (parent == null && noOrderByAndLimit) {
                        innerQuery.setOrderBy(orderBy);
                    }
                }

                SQLSelectQuery query = queryRest(select.getQuery(), false);
                if (query instanceof SQLUnionQuery && select.getWithSubQuery() == null) {
                    select.getQuery().setBracket(true);
                    tableSource = new SQLUnionQueryTableSource((SQLUnionQuery) query);
                } else {
                    tableSource = new SQLSubqueryTableSource(select);
                }

                if (hints != null) {
                    tableSource.getHints().addAll(hints);
                }

            } else if (lexer.token() == Token.LPAREN) {
                tableSource = parseTableSource();
                if (lexer.token() != Token.RPAREN && tableSource instanceof SQLSubqueryTableSource) {
                    SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) tableSource;
                    SQLSelect select = sqlSubqueryTableSource.getSelect();

                    SQLSelectQuery query = queryRest(select.getQuery(), true);
                    if (query instanceof SQLUnionQuery && select.getWithSubQuery() == null) {
                        select.getQuery().setBracket(true);
                        tableSource = new SQLUnionQueryTableSource((SQLUnionQuery) query);
                    } else {
                        tableSource = new SQLSubqueryTableSource(select);
                    }

                    if (hints != null) {
                        tableSource.getHints().addAll(hints);
                    }
                } else if (lexer.token() != Token.RPAREN && tableSource instanceof SQLUnionQueryTableSource) {
                    SQLUnionQueryTableSource unionQueryTableSource = (SQLUnionQueryTableSource) tableSource;
                    SQLUnionQuery unionQuery = unionQueryTableSource.getUnion();

                    // Deal xx union xx order by or limit.
                    if (lexer.token() == Token.LIMIT) {
                        boolean hasNonOrderByOrLimit = unionQuery.getOrderBy() == null && unionQuery.getLimit() == null;

                        if (hasNonOrderByOrLimit) {
                            SQLLimit limit = this.exprParser.parseLimit();
                            unionQuery.setLimit(limit);
                        }
                    } else if (lexer.token() == Token.ORDER) {
                        boolean hasNonOrderByOrLimit = unionQuery.getOrderBy() == null && unionQuery.getLimit() == null;

                        if (hasNonOrderByOrLimit) {
                            SQLOrderBy orderBy = this.exprParser.parseOrderBy();
                            unionQuery.setOrderBy(orderBy);
                            if (lexer.token() == Token.LIMIT) {
                                SQLLimit limit = this.exprParser.parseLimit();
                                unionQuery.setLimit(limit);
                            }
                        }
                    }

                    SQLSelectQuery query = queryRest(unionQuery, true);
                    if (query instanceof SQLUnionQuery) {
                        unionQuery.setBracket(true);
                        tableSource = new SQLUnionQueryTableSource((SQLUnionQuery) query);
                    } else {
                        tableSource = new SQLSubqueryTableSource(unionQuery);
                    }

                    if (hints != null) {
                        tableSource.getHints().addAll(hints);
                    }
                }
                accept(Token.RPAREN);
            } else {
                tableSource = parseTableSource();
                accept(Token.RPAREN);
                if (tableSource instanceof SQLValuesTableSource) {
                    ((SQLValuesTableSource) tableSource).setBracket(true);
                    if (lexer.token() == Token.AS) {
                        lexer.nextToken();
                        String alias = lexer.stringVal();
                        lexer.nextToken();
                        tableSource.setAlias(alias);
                        if (lexer.token() == Token.LPAREN) {
                            accept(Token.LPAREN);
                            SQLValuesTableSource values = (SQLValuesTableSource) tableSource;
                            this.exprParser.names(values.getColumns(), tableSource);
                            accept(Token.RPAREN);
                        }
                    }
                }

            }

            return parseTableSourceRest(tableSource);
        } else if (lexer.token() == Token.LBRACE) {
            accept(Token.LBRACE);
            acceptIdentifier("OJ");

            SQLTableSource tableSrc = parseTableSource();

            accept(Token.RBRACE);

            tableSrc = parseTableSourceRest(tableSrc);

            if (lexer.hasComment() && lexer.isKeepComments()) {
                tableSrc.addAfterComment(lexer.readAndResetComments());
            }

            return tableSrc;
        }

        if (lexer.token() == Token.VALUES) {
            return parseValues();
        }

        if (lexer.token() == Token.UPDATE) {
            SQLTableSource tableSource = new MySqlUpdateTableSource(parseUpdateStatment());
            return parseTableSourceRest(tableSource);
        }

        if (lexer.token() == Token.SELECT) {
            throw new ParserException("TODO. " + lexer.info());
        }

        if (lexer.identifierEquals(FnvHash.Constants.UNNEST)) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                SQLUnnestTableSource unnest = new SQLUnnestTableSource();
                this.exprParser.exprList(unnest.getItems(), unnest);
                accept(Token.RPAREN);

                if (lexer.token() == Token.WITH) {
                    lexer.nextToken();
                    acceptIdentifier("ORDINALITY");
                    unnest.setOrdinality(true);
                }

                String alias = this.tableAlias();
                unnest.setAlias(alias);

                if (lexer.token() == Token.LPAREN) {
                    lexer.nextToken();
                    this.exprParser.names(unnest.getColumns(), unnest);
                    accept(Token.RPAREN);
                }

                SQLTableSource tableSrc = parseTableSourceRest(unnest);
                return tableSrc;
            } else {
                lexer.reset(mark);
            }
        }

        SQLExprTableSource tableReference = new SQLExprTableSource();

        parseTableSourceQueryTableExpr(tableReference);

        SQLTableSource tableSrc = parseTableSourceRest(tableReference);

        if (lexer.hasComment() && lexer.isKeepComments()) {
            tableSrc.addAfterComment(lexer.readAndResetComments());
        }

        return tableSrc;
    }

    protected MySqlUpdateStatement parseUpdateStatment() {
        MySqlUpdateStatement update = new MySqlUpdateStatement();

        lexer.nextToken();

        if (lexer.identifierEquals(FnvHash.Constants.LOW_PRIORITY)) {
            lexer.nextToken();
            update.setLowPriority(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.IGNORE)) {
            lexer.nextToken();
            update.setIgnore(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.COMMIT_ON_SUCCESS)) {
            lexer.nextToken();
            update.setCommitOnSuccess(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.ROLLBACK_ON_FAIL)) {
            lexer.nextToken();
            update.setRollBackOnFail(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.QUEUE_ON_PK)) {
            lexer.nextToken();
            update.setQueryOnPk(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.TARGET_AFFECT_ROW)) {
            lexer.nextToken();
            SQLExpr targetAffectRow = this.exprParser.expr();
            update.setTargetAffectRow(targetAffectRow);
        }

        if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
            lexer.nextToken();

            if (lexer.token() == Token.ALL) {
                lexer.nextToken();
                acceptIdentifier("PARTITIONS");
                update.setForceAllPartitions(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)) {
                lexer.nextToken();
                update.setForceAllPartitions(true);
            } else if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                SQLName partition = this.exprParser.name();
                update.setForcePartition(partition);
            } else {
                throw new ParserException("TODO. " + lexer.info());
            }
        }

        while (lexer.token() == Token.HINT) {
            this.exprParser.parseHints(update.getHints());
        }

        SQLSelectParser selectParser = this.exprParser.createSelectParser();
        SQLTableSource updateTableSource = selectParser.parseTableSource();
        update.setTableSource(updateTableSource);

        accept(Token.SET);

        for (; ; ) {
            SQLUpdateSetItem item = this.exprParser.parseUpdateSetItem();
            update.addItem(item);

            if (lexer.token() != Token.COMMA) {
                break;
            }

            lexer.nextToken();
        }

        if (lexer.token() == (Token.WHERE)) {
            lexer.nextToken();
            update.setWhere(this.exprParser.expr());
        }

        update.setOrderBy(this.exprParser.parseOrderBy());
        update.setLimit(this.exprParser.parseLimit());

        return update;
    }

    protected void parseInto(SQLSelectQueryBlock queryBlock) {
        if (lexer.token() != Token.INTO) {
            return;
        }

        lexer.nextToken();

        if (lexer.identifierEquals(FnvHash.Constants.OUTFILE)) {
            lexer.nextToken();

            MySqlOutFileExpr outFile = new MySqlOutFileExpr();
            outFile.setFile(expr());

            queryBlock.setInto(outFile);
            if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
                lexer.nextToken();
                accept(Token.SET);
                outFile.setCharset(expr().toString());
            }
            if (lexer.identifierEquals(FnvHash.Constants.FIELDS) || lexer.identifierEquals(FnvHash.Constants.COLUMNS)) {
                lexer.nextToken();

                if (lexer.identifierEquals(FnvHash.Constants.TERMINATED)) {
                    lexer.nextToken();
                    accept(Token.BY);
                    outFile.setColumnsTerminatedBy(expr());
                }

                if (lexer.identifierEquals(FnvHash.Constants.OPTIONALLY)) {
                    lexer.nextToken();
                    outFile.setColumnsEnclosedOptionally(true);
                }

                if (lexer.identifierEquals(FnvHash.Constants.ENCLOSED)) {
                    lexer.nextToken();
                    accept(Token.BY);
                    outFile.setColumnsEnclosedBy(expr());
                }

                if (lexer.identifierEquals(FnvHash.Constants.ESCAPED)) {
                    lexer.nextToken();
                    accept(Token.BY);
                    outFile.setColumnsEscaped(expr());
                }
            }

            if (lexer.identifierEquals(FnvHash.Constants.LINES)) {
                lexer.nextToken();

                if (lexer.identifierEquals(FnvHash.Constants.STARTING)) {
                    lexer.nextToken();
                    accept(Token.BY);
                    outFile.setLinesStartingBy(expr());
                }
                if (lexer.identifierEquals(FnvHash.Constants.TERMINATED)) {
                    lexer.nextToken();
                    accept(Token.BY);
                    outFile.setLinesTerminatedBy(expr());
                }
            }
            if (lexer.identifierEquals(FnvHash.Constants.STATISTICS)) {
                lexer.nextToken();
                outFile.setStatistics(true);
            }
        } else {
            // Why we need this?
            // output of select a, b into @x, @y from test; is select a, b into (@x, @y) from test
            // TODO maybe modify output visitor is better
            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
            }
            SQLExpr intoExpr = this.exprParser.name();
            if (lexer.token() == Token.COMMA) {
                SQLListExpr list = new SQLListExpr();
                list.addItem(intoExpr);

                while (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    SQLName name = this.exprParser.name();
                    list.addItem(name);
                }

                intoExpr = list;
            }
            if (lexer.token() == Token.RPAREN) {
                lexer.nextToken();
            }
            queryBlock.setInto(intoExpr);
        }
    }

    protected SQLTableSource primaryTableSourceRest(SQLTableSource tableSource) {
        parsePartitionAndAsOf(tableSource);

        parseIndexHintList(tableSource);

        return tableSource;
    }

    public SQLTableSource parseTableSourceRest(SQLTableSource tableSource) {
        if (lexer.identifierEquals(FnvHash.Constants.TABLESAMPLE) && tableSource instanceof SQLExprTableSource) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();

            SQLTableSampling sampling = new SQLTableSampling();

            if (lexer.identifierEquals(FnvHash.Constants.BERNOULLI)) {
                lexer.nextToken();
                sampling.setBernoulli(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.SYSTEM)) {
                lexer.nextToken();
                sampling.setSystem(true);
            }

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();

                if (lexer.identifierEquals(FnvHash.Constants.BUCKET)) {
                    lexer.nextToken();
                    SQLExpr bucket = this.exprParser.primary();
                    sampling.setBucket(bucket);

                    if (lexer.token() == Token.OUT) {
                        lexer.nextToken();
                        accept(Token.OF);
                        SQLExpr outOf = this.exprParser.primary();
                        sampling.setOutOf(outOf);
                    }

                    if (lexer.token() == Token.ON) {
                        lexer.nextToken();
                        SQLExpr on = this.exprParser.expr();
                        sampling.setOn(on);
                    }
                }

                if (lexer.token() == Token.LITERAL_INT || lexer.token() == Token.LITERAL_FLOAT) {
                    SQLExpr val = this.exprParser.primary();

                    if (lexer.identifierEquals(FnvHash.Constants.ROWS)) {
                        lexer.nextToken();
                        sampling.setRows(val);
                    } else if (lexer.token() == Token.RPAREN) {
                        sampling.setRows(val);
                    } else {
                        acceptIdentifier("PERCENT");
                        sampling.setPercent(val);
                    }
                }

                if (lexer.token() == Token.IDENTIFIER) {
                    String strVal = lexer.stringVal();
                    char first = strVal.charAt(0);
                    char last = strVal.charAt(strVal.length() - 1);
                    if (last >= 'a' && last <= 'z') {
                        last -= 32; // to upper
                    }

                    boolean match = false;
                    if ((first == '.' || (first >= '0' && first <= '9'))) {
                        switch (last) {
                        case 'B':
                        case 'K':
                        case 'M':
                        case 'G':
                        case 'T':
                        case 'P':
                            match = true;
                            break;
                        default:
                            break;
                        }
                    }
                    SQLSizeExpr size = new SQLSizeExpr(strVal.substring(0, strVal.length() - 2), last);
                    sampling.setByteLength(size);
                    lexer.nextToken();
                }

                final SQLExprTableSource table = (SQLExprTableSource) tableSource;
                table.setSampling(sampling);

                accept(Token.RPAREN);
            } else {
                lexer.reset(mark);
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.USING)) {
            return tableSource;
        }

        //table_factor: {
        //    tbl_name [{PARTITION (partition_names) | AS OF expr}]
        //        [[AS] alias] [index_hint_list]
        //  | table_subquery [AS] alias
        //  | ( table_references )
        //}
        parsePartitionAndAsOf(tableSource);

        parseIndexHintList(tableSource);

        return super.parseTableSourceRest(tableSource);
    }

    private void parseIndexHintList(SQLTableSource tableSource) {
        if (lexer.token() == Token.USE) {
            lexer.nextToken();
            MySqlUseIndexHint hint = new MySqlUseIndexHint();
            parseIndexHint(hint);
            tableSource.getHints().add(hint);
            parseIndexHintList(tableSource);
        }

        if (lexer.identifierEquals(FnvHash.Constants.IGNORE)) {
            lexer.nextToken();
            MySqlIgnoreIndexHint hint = new MySqlIgnoreIndexHint();
            parseIndexHint(hint);
            tableSource.getHints().add(hint);
            parseIndexHintList(tableSource);
        }

        if (lexer.identifierEquals(FnvHash.Constants.FORCE)) {
            lexer.nextToken();
            MySqlForceIndexHint hint = new MySqlForceIndexHint();
            parseIndexHint(hint);
            tableSource.getHints().add(hint);
            parseIndexHintList(tableSource);
        }
    }

    private void parseIndexHint(MySqlIndexHintImpl hint) {
        if (lexer.token() == Token.INDEX) {
            lexer.nextToken();
        } else {
            accept(Token.KEY);
        }

        if (lexer.token() == Token.FOR) {
            lexer.nextToken();

            if (lexer.token() == Token.JOIN) {
                lexer.nextToken();
                hint.setOption(MySqlIndexHint.Option.JOIN);
            } else if (lexer.token() == Token.ORDER) {
                lexer.nextToken();
                accept(Token.BY);
                hint.setOption(MySqlIndexHint.Option.ORDER_BY);
            } else {
                accept(Token.GROUP);
                accept(Token.BY);
                hint.setOption(MySqlIndexHint.Option.GROUP_BY);
            }
        }

        accept(Token.LPAREN);
        while (lexer.token() != Token.RPAREN && lexer.token() != Token.EOF) {
            if (lexer.token() == Token.PRIMARY) {
                lexer.nextToken();
                hint.getIndexList().add(new SQLIdentifierExpr("PRIMARY"));
            } else {
                if (lexer.token() == Token.LITERAL_CHARS) {
                    // Use literal char is not support in MySQL.
                    throw new ParserException(
                        "syntax error, not support string literal in index hint, " + lexer.info());
                }
                SQLName name = this.exprParser.name();
                name.setParent(hint);
                hint.getIndexList().add(name);
            }
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            } else {
                break;
            }
        }
        accept(Token.RPAREN);
    }

    public SQLUnionQuery unionRest(SQLUnionQuery union) {
        if (lexer.token() == Token.LIMIT) {
            union.setLimit(this.exprParser.parseLimit());
        }
        return super.unionRest(union);
    }

    public MySqlExprParser getExprParser() {
        return (MySqlExprParser) exprParser;
    }

    /**
     * tbl_name [{PARTITION (partition_names) | AS OF expr}] [[AS] alias] [index_hint_list]
     */
    private void parsePartitionAndAsOf(SQLTableSource tableSource) {
        while (true) {
            switch (lexer.token()) {
            case PARTITION:
                lexer.nextToken();
                // 兼容jsqlparser 和presto
                if (lexer.token() == Token.ON) {
                    tableSource.setAlias("partition");
                } else {
                    accept(Token.LPAREN);
                    this.exprParser.names(((SQLExprTableSource) tableSource).getPartitions(), tableSource);
                    accept(Token.RPAREN);
                }
                continue;
            case AS:
                if (parseAsOf(tableSource)) {
                    continue;
                }
            }
            break;
        }
    }

    public boolean parseAsOf(SQLTableSource tableSource) {
        Lexer.SavePoint savePoint = lexer.mark();

        if (lexer.token() == Token.AS) {
            lexer.nextToken();
            if (lexer.token() != Token.OF) {
                lexer.reset(savePoint);
                return false;
            }
            lexer.nextToken();

            // table_factor: {
            //    tbl_name [{PARTITION (partition_names) | AS OF expr}]
            //        [[AS] alias] [index_hint_list]
            //  | table_subquery [AS] alias
            //  | ( table_references )
            //}

            // Cannot set flashback timestamp twice;
            // FIXME: Syntax like below should be validated here
            //    tbl_name [{PARTITION (partition_names) | AS OF expr}]
            //        [[AS] alias] [index_hint_list]
            // but, for now, we handle [{PARTITION (partition_names) | AS OF expr}] and [index_hint_list] in
            // MySqlSelectParser.parseTableSourceRest() before [[AS] alias] is handled in super.parseTableSourceRest().
            // So that, for now, we do not reject clause like "FROM t AS a AS OF ..." or "FROM t a AS OF ..."
            if (null != tableSource.getFlashback()
                || tableSource instanceof SQLSubqueryTableSource
                || tableSource instanceof SQLValuesTableSource
                || tableSource instanceof SQLUnionQueryTableSource
//                || (null != tableSource.getAlias() && !tableSource.getAlias() .isEmpty())
            ) {
                lexer.reset(savePoint);
                setErrorEndPos(lexer.pos());
                printError(lexer.token());
            }

            //支持as of tso expr; expr 不做限定，可以是tso，可以是表达式。
            //不同于下面as of timestamp 和 as of 限定了后面表达式类型;
            if (lexer.identifierEquals("TSO")) {
                lexer.nextToken();
                SQLExpr expr = this.exprParser.expr();
                tableSource.setFlashback(expr);
                tableSource.setFlashbackWithTso(true);
                return true;
            }

            SQLExpr expr = this.exprParser.expr();
            if (expr instanceof SQLTimestampExpr) {
                // FROM t AS OF TIMESTAMP '2021-12-10 17:44:00'
                SQLTimestampExpr tsExpr = (SQLTimestampExpr) (expr);
                tableSource.setFlashback(tsExpr);
            } else if (expr instanceof SQLVariantRefExpr) {
                // Timestamp value was parameterized
                // FROM t AS OF ?
                SQLVariantRefExpr tsExpr = (SQLVariantRefExpr) (expr);
                tableSource.setFlashback(tsExpr);
            } else {
                throw new ParserException("SELECT ... AS OF expect timestamp expression");
            }
            return true;
        }

        return false;
    }
}

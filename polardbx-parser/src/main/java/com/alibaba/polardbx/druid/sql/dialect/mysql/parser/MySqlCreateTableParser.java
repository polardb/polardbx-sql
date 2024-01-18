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
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByList;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByUdfHash;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByValue;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByList;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByUdfHash;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLForeignKeyConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableLike;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement.TableSpaceOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExtPartition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByValue;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLCreateTableParser;
import com.alibaba.polardbx.druid.sql.parser.SQLExprParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.druid.util.MySqlUtils;
import com.alibaba.polardbx.druid.util.StringUtils;

public class MySqlCreateTableParser extends SQLCreateTableParser {

    public MySqlCreateTableParser(ByteString sql) {
        super(new MySqlExprParser(sql));
    }

    public MySqlCreateTableParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public SQLCreateTableStatement parseCreateTable() {
        return parseCreateTable(true);
    }

    @Override
    public MySqlExprParser getExprParser() {
        return (MySqlExprParser) exprParser;
    }

    public MySqlCreateTableStatement parseCreateTable(boolean acceptCreate) {
        MySqlCreateTableStatement stmt = new MySqlCreateTableStatement();
        if (acceptCreate) {
            if (lexer.hasComment() && lexer.isKeepComments()) {
                stmt.addBeforeComment(lexer.readAndResetComments());
            }
            accept(Token.CREATE);
        }

        if (lexer.identifierEquals("TEMPORARY")) {
            lexer.nextToken();
            stmt.setType(SQLCreateTableStatement.Type.GLOBAL_TEMPORARY);
        } else if (lexer.identifierEquals("SHADOW")) {
            lexer.nextToken();
            stmt.setType(SQLCreateTableStatement.Type.SHADOW);
        } else if (isEnabled(SQLParserFeature.DrdsMisc)) {
            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                stmt.setPrefixPartition(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.BROADCAST)) {
                lexer.nextToken();
                stmt.setPrefixBroadcast(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.SINGLE)) {
                lexer.nextToken();
                stmt.setPrefixSingle(true);
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.DIMENSION)) {
            lexer.nextToken();
            stmt.setDimension(true);
        }

        if (lexer.token() == Token.HINT) {
            this.exprParser.parseHints(stmt.getHints());
        }

        if (lexer.identifierEquals(FnvHash.Constants.EXTERNAL)) {
            lexer.nextToken();
            stmt.setExternal(true);
        }

        accept(Token.TABLE);

        if (lexer.token() == Token.IF || lexer.identifierEquals("IF")) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);

            stmt.setIfNotExiists(true);
        }

        stmt.setName(this.exprParser.name());

        if (lexer.token() == Token.LIKE) {
            lexer.nextToken();
            SQLName name = this.exprParser.name();
            stmt.setLike(name);
        }

        boolean tableAS = false;
        if (lexer.token() == Token.AS) {
            lexer.nextToken();
            tableAS = true;
            if (lexer.token() == Token.IDENTIFIER) {
                SQLName name = this.exprParser.name();
                stmt.setAsTable(name);
            }
        }

        if (lexer.token() == Token.WITH && tableAS) {
            SQLSelect query = new MySqlSelectParser(this.exprParser).select();
            stmt.setSelect(query);
        } else if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();

            if (lexer.token() == Token.SELECT) {
                SQLSelect query = new MySqlSelectParser(this.exprParser).select();
                stmt.setSelect(query);
            } else {
                for (; ; ) {
                    SQLColumnDefinition column = null;

                    boolean global = false;
                    if (lexer.identifierEquals(FnvHash.Constants.GLOBAL)) {
                        final Lexer.SavePoint mark = lexer.mark();
                        lexer.nextToken();
                        if (lexer.token() == Token.INDEX || lexer.token() == Token.KEY
                            || lexer.token() == Token.UNIQUE) {
                            global = true;
                        } else {
                            lexer.reset(mark);
                        }
                    }

                    boolean local = false;
                    if (lexer.identifierEquals(FnvHash.Constants.LOCAL)) {
                        final Lexer.SavePoint mark = lexer.mark();
                        lexer.nextToken();
                        if (lexer.token() == Token.INDEX || lexer.token() == Token.KEY
                            || lexer.token() == Token.UNIQUE) {
                            local = true;
                        } else if (lexer.token() == Token.FULLTEXT) {
                            lexer.nextToken();

                            if (lexer.token() == Token.KEY) {
                                MySqlKey fulltextKey = new MySqlKey();
                                this.exprParser.parseIndex(fulltextKey.getIndexDefinition());
                                fulltextKey.setIndexType("FULLTEXT");
                                fulltextKey.setParent(stmt);
                                stmt.getTableElementList().add(fulltextKey);

                                while (lexer.token() == Token.HINT) {
                                    lexer.nextToken();
                                }

                                if (lexer.token() == Token.RPAREN) {
                                    break;
                                } else if (lexer.token() == Token.COMMA) {
                                    lexer.nextToken();
                                    continue;
                                }
                            } else if (lexer.token() == Token.INDEX) {
                                MySqlTableIndex idx = new MySqlTableIndex();
                                this.exprParser.parseIndex(idx.getIndexDefinition());
                                idx.setIndexType("FULLTEXT");
                                idx.setParent(stmt);
                                stmt.getTableElementList().add(idx);

                                if (lexer.token() == Token.RPAREN) {
                                    break;
                                } else if (lexer.token() == Token.COMMA) {
                                    lexer.nextToken();
                                    continue;
                                }
                            } else if (lexer.token() == Token.IDENTIFIER && MySqlUtils.isBuiltinDataType(
                                lexer.stringVal())) {
                                lexer.reset(mark);
                            } else {
                                MySqlTableIndex idx = new MySqlTableIndex();
                                this.exprParser.parseIndex(idx.getIndexDefinition());
                                idx.setIndexType("FULLTEXT");
                                idx.setParent(stmt);
                                stmt.getTableElementList().add(idx);

                                if (lexer.token() == Token.RPAREN) {
                                    break;
                                } else if (lexer.token() == Token.COMMA) {
                                    lexer.nextToken();
                                    continue;
                                }
                            }

                        } else if (lexer.identifierEquals(FnvHash.Constants.SPATIAL)) {
                            lexer.nextToken();
                            if (lexer.token() == Token.INDEX || lexer.token() == Token.KEY ||
                                lexer.token() != Token.IDENTIFIER || !MySqlUtils.isBuiltinDataType(lexer.stringVal())) {
                                MySqlTableIndex idx = new MySqlTableIndex();
                                this.exprParser.parseIndex(idx.getIndexDefinition());
                                idx.setIndexType("SPATIAL");
                                idx.setParent(stmt);
                                stmt.getTableElementList().add(idx);

                                if (lexer.token() == Token.RPAREN) {
                                    break;
                                } else if (lexer.token() == Token.COMMA) {
                                    lexer.nextToken();
                                    continue;
                                }
                            } else {
                                lexer.reset(mark);
                            }
                        } else {
                            lexer.reset(mark);
                        }
                    }

                    if (lexer.token() == Token.FULLTEXT) {
                        Lexer.SavePoint mark = lexer.mark();
                        lexer.nextToken();

                        if (lexer.token() == Token.KEY) {
                            MySqlKey fulltextKey = new MySqlKey();
                            this.exprParser.parseIndex(fulltextKey.getIndexDefinition());
                            fulltextKey.setIndexType("FULLTEXT");
                            fulltextKey.setParent(stmt);
                            stmt.getTableElementList().add(fulltextKey);

                            while (lexer.token() == Token.HINT) {
                                lexer.nextToken();
                            }

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else if (lexer.token() == Token.INDEX) {
                            MySqlTableIndex idx = new MySqlTableIndex();
                            this.exprParser.parseIndex(idx.getIndexDefinition());
                            idx.setIndexType("FULLTEXT");
                            idx.setParent(stmt);
                            stmt.getTableElementList().add(idx);

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else if (lexer.token() == Token.IDENTIFIER && MySqlUtils.isBuiltinDataType(
                            lexer.stringVal())) {
                            lexer.reset(mark);
                        } else {
                            MySqlTableIndex idx = new MySqlTableIndex();
                            this.exprParser.parseIndex(idx.getIndexDefinition());
                            idx.setIndexType("FULLTEXT");
                            idx.setParent(stmt);
                            stmt.getTableElementList().add(idx);

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        }

                    } else if (lexer.identifierEquals(FnvHash.Constants.SPATIAL)) {
                        Lexer.SavePoint mark = lexer.mark();
                        lexer.nextToken();
                        if (lexer.token() == Token.INDEX || lexer.token() == Token.KEY ||
                            lexer.token() != Token.IDENTIFIER || !MySqlUtils.isBuiltinDataType(lexer.stringVal())) {
                            MySqlTableIndex idx = new MySqlTableIndex();
                            this.exprParser.parseIndex(idx.getIndexDefinition());
                            idx.setIndexType("SPATIAL");
                            idx.setParent(stmt);
                            stmt.getTableElementList().add(idx);

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else {
                            lexer.reset(mark);
                        }
                    }

                    if (lexer.identifierEquals(FnvHash.Constants.ANN)) {
                        Lexer.SavePoint mark = lexer.mark();
                        lexer.nextToken();
                        if (lexer.token() == Token.INDEX || lexer.token() == Token.KEY) {
                            MySqlTableIndex idx = new MySqlTableIndex();
                            this.exprParser.parseIndex(idx.getIndexDefinition());
                            idx.setIndexType("ANN");
                            idx.setParent(stmt);
                            stmt.getTableElementList().add(idx);

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else {
                            lexer.reset(mark);
                        }
                    }
                    if (lexer.identifierEquals(FnvHash.Constants.CLUSTERED)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.KEY) {
                            MySqlKey clsKey = new MySqlKey();
                            this.exprParser.parseIndex(clsKey.getIndexDefinition());
                            clsKey.setIndexType("CLUSTERED");
                            clsKey.setParent(stmt);
                            stmt.getTableElementList().add(clsKey);

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else if (lexer.token() == Token.INDEX) {
                            MySqlTableIndex idx = new MySqlTableIndex();
                            this.exprParser.parseIndex(idx.getIndexDefinition());
                            idx.setClustered(true);
                            idx.setParent(stmt);
                            stmt.getTableElementList().add(idx);

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else if (lexer.token() == Token.UNIQUE) {
                            MySqlUnique unique = this.getExprParser().parseUnique();
                            unique.setClustered(true);
                            unique.setParent(stmt);
                            stmt.getTableElementList().add(unique);
                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        }
                    } else if (lexer.identifierEquals(FnvHash.Constants.CLUSTERING)) {
                        lexer.nextToken();
                        if (lexer.token() == Token.KEY) {
                            MySqlKey clsKey = new MySqlKey();
                            this.exprParser.parseIndex(clsKey.getIndexDefinition());
                            clsKey.setIndexType("CLUSTERING");
                            clsKey.setParent(stmt);
                            stmt.getTableElementList().add(clsKey);

                            if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else if (lexer.token() == Token.INDEX) {
                            MySqlTableIndex idx = new MySqlTableIndex();
                            this.exprParser.parseIndex(idx.getIndexDefinition());
                            idx.setIndexType("CLUSTERING");
                            idx.setParent(stmt);
                            stmt.getTableElementList().add(idx);

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        }
                    } else if (lexer.token() == Token.IDENTIFIER //
                        || lexer.token() == Token.LITERAL_CHARS) {
                        column = this.exprParser.parseColumn();
                        column.setParent(stmt);
                        stmt.getTableElementList().add(column);

                        if (lexer.isKeepComments() && lexer.hasComment()) {
                            column.addAfterComment(lexer.readAndResetComments());
                        }
                    } else if (lexer.token() == Token.CONSTRAINT //
                        || lexer.token() == Token.PRIMARY //
                        || lexer.token() == Token.UNIQUE) {
                        SQLTableConstraint constraint = this.parseConstraint();
                        constraint.setParent(stmt);

                        if (constraint instanceof MySqlUnique) {
                            MySqlUnique unique = (MySqlUnique) constraint;
                            if (global) {
                                unique.setGlobal(true);
                            }
                            if (local) {
                                unique.setLocal(true);
                            }
                        }

                        stmt.getTableElementList().add(constraint);
                    } else if (lexer.token() == (Token.INDEX)) {
                        MySqlTableIndex idx = new MySqlTableIndex();
                        this.exprParser.parseIndex(idx.getIndexDefinition());

                        if (global) {
                            idx.getIndexDefinition().setGlobal(true);
                        }
                        if (local) {
                            idx.getIndexDefinition().setLocal(true);
                        }

                        idx.setParent(stmt);
                        stmt.getTableElementList().add(idx);
                    } else if (lexer.token() == (Token.KEY)) {
                        Lexer.SavePoint savePoint = lexer.mark();
                        lexer.nextToken();

                        boolean isColumn = false;
                        if (lexer.identifierEquals(FnvHash.Constants.VARCHAR)) {
                            isColumn = true;
                        }
                        lexer.reset(savePoint);

                        if (isColumn) {
                            column = this.exprParser.parseColumn();
                            stmt.getTableElementList().add(column);
                        } else {
                            final SQLTableConstraint constraint = parseConstraint();
                            if (constraint instanceof MySqlKey) {
                                final MySqlKey key = (MySqlKey) constraint;
                                if (global) {
                                    key.getIndexDefinition().setGlobal(true);
                                }
                                if (local) {
                                    key.getIndexDefinition().setLocal(true);
                                }
                            }
                            stmt.getTableElementList().add(constraint);
                        }
                    } else if (lexer.token() == (Token.PRIMARY)) {
                        SQLTableConstraint pk = parseConstraint();
                        pk.setParent(stmt);
                        stmt.getTableElementList().add(pk);
                    } else if (lexer.token() == (Token.FOREIGN)) {
                        SQLForeignKeyConstraint fk = this.getExprParser().parseForeignKey();
                        fk.setParent(stmt);
                        stmt.getTableElementList().add(fk);
                    } else if (lexer.token() == Token.CHECK) {
                        SQLCheck check = this.exprParser.parseCheck();
                        stmt.getTableElementList().add(check);
                    } else if (lexer.token() == Token.LIKE) {
                        lexer.nextToken();
                        SQLTableLike tableLike = new SQLTableLike();
                        tableLike.setTable(new SQLExprTableSource(this.exprParser.name()));
                        tableLike.setParent(stmt);
                        stmt.getTableElementList().add(tableLike);

                        if (lexer.identifierEquals(FnvHash.Constants.INCLUDING)) {
                            lexer.nextToken();
                            acceptIdentifier("PROPERTIES");
                            tableLike.setIncludeProperties(true);
                        } else if (lexer.identifierEquals(FnvHash.Constants.EXCLUDING)) {
                            lexer.nextToken();
                            acceptIdentifier("PROPERTIES");
                            tableLike.setExcludeProperties(true);
                        }
                    } else {
                        column = this.exprParser.parseColumn();
                        stmt.getTableElementList().add(column);
                    }

                    if (lexer.token() != Token.COMMA) {
                        break;
                    } else {
                        lexer.nextToken();
                        if (lexer.isKeepComments() && lexer.hasComment() && column != null) {
                            column.addAfterComment(lexer.readAndResetComments());
                        }
                    }
                }
            }

            accept(Token.RPAREN);

//            if (lexer.token() == Token.HINT && lexer.stringVal().charAt(0) == '!') {
//                lexer.nextToken();
//            }

            if (lexer.token() == (Token.HINT)) {
                this.exprParser.parseHints(stmt.getOptionHints());
            }
        }

        for (; ; ) {
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            if (lexer.identifierEquals(FnvHash.Constants.ENGINE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                SQLExpr expr = null;
                if (lexer.token() == Token.MERGE) {
                    expr = new SQLIdentifierExpr(lexer.stringVal());
                    lexer.nextToken();
                } else {
                    expr = this.exprParser.expr();
                }
                stmt.addOption("ENGINE", expr);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.ARCHIVE_MODE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                SQLExpr expr = new SQLIdentifierExpr(lexer.stringVal());
                lexer.nextToken();
                stmt.addOption("ARCHIVE_MODE", expr);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.BLOCK_SIZE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                SQLExpr expr = null;
                if (lexer.token() == Token.MERGE) {
                    expr = new SQLIdentifierExpr(lexer.stringVal());
                    lexer.nextToken();
                } else {
                    expr = this.exprParser.integerExpr();
                }
                stmt.addOption("BLOCK_SIZE", expr);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.REPLICA_NUM)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                SQLExpr expr = this.exprParser.integerExpr();
                stmt.addOption("REPLICA_NUM", expr);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.TABLET_SIZE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                SQLExpr expr = this.exprParser.integerExpr();
                stmt.addOption("TABLET_SIZE", expr);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.PCTFREE)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                SQLExpr expr = this.exprParser.integerExpr();
                stmt.addOption("PCTFREE", expr);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.USE_BLOOM_FILTER)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                SQLExpr expr = this.exprParser.primary();
                stmt.addOption("USE_BLOOM_FILTER", expr);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.AUTO_INCREMENT)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("AUTO_INCREMENT", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("AVG_ROW_LENGTH")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("AVG_ROW_LENGTH", this.exprParser.expr());
                continue;
            }

            if (parseTableOptionCharsetOrCollate(stmt)) {
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.CHECKSUM)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("CHECKSUM", this.exprParser.expr());
                continue;
            }

            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.setComment(this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.CONNECTION)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("CONNECTION", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.DATA)) {
                lexer.nextToken();
                acceptIdentifier("DIRECTORY");
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("DATA DIRECTORY", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("DELAY_KEY_WRITE")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("DELAY_KEY_WRITE", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("FULLTEXT_DICT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("FULLTEXT_DICT", this.exprParser.charExpr());
                continue;
            }

            if (lexer.token() == Token.INDEX) {
                lexer.nextToken();
                acceptIdentifier("DIRECTORY");
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("INDEX DIRECTORY", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("INSERT_METHOD")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("INSERT_METHOD", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("KEY_BLOCK_SIZE")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("KEY_BLOCK_SIZE", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.MAX_ROWS)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("MAX_ROWS", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.MIN_ROWS)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("MIN_ROWS", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.PACK_KEYS)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("PACK_KEYS", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.PASSWORD)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("PASSWORD", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("ROW_FORMAT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLExpr expr = new SQLIdentifierExpr(lexer.stringVal());
                lexer.nextToken();
                stmt.addOption("ROW_FORMAT", expr);
                continue;
            }

            if (lexer.identifierEquals("STATS_AUTO_RECALC")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.addOption("STATS_AUTO_RECALC", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("STATS_PERSISTENT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.addOption("STATS_PERSISTENT", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("STATS_SAMPLE_PAGES")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.addOption("STATS_SAMPLE_PAGES", this.exprParser.expr());
                continue;
            }

            if (lexer.token() == Token.UNION) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                accept(Token.LPAREN);
                SQLListExpr list = new SQLListExpr();
                this.exprParser.exprList(list.getItems(), list);
                stmt.addOption("UNION", list);
                accept(Token.RPAREN);
                continue;
            }

            if (lexer.token() == Token.TABLESPACE) {
                lexer.nextToken();

                TableSpaceOption option = new TableSpaceOption();
                option.setName(this.exprParser.name());

                if (lexer.identifierEquals("STORAGE")) {
                    lexer.nextToken();
                    option.setStorage(this.exprParser.name());
                }

                stmt.addOption("TABLESPACE", option);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.LOCALITY)) {
                lexer.nextToken();
                acceptIf(Token.EQ);

                stmt.setLocality(this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.TABLEGROUP)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLName tableGroup = this.exprParser.name();
                stmt.setTableGroup(tableGroup);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.JOINGROUP)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                SQLName joinGroup = this.exprParser.name();
                stmt.setJoinGroup(joinGroup);
                continue;
            }

            if (lexer.identifierEquals("AUTO_SPLIT")) {
                lexer.nextToken();
                acceptIf(Token.EQ);
                SQLExpr autoSplit = this.exprParser.expr();
                stmt.setAutoSplit(autoSplit);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.TYPE)) {
                lexer.nextToken();
                accept(Token.EQ);
                stmt.addOption("TYPE", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("INDEX_ALL")) {
                lexer.nextToken();
                accept(Token.EQ);
                if (lexer.token() == Token.LITERAL_CHARS) {
                    if ("Y".equalsIgnoreCase(lexer.stringVal())) {
                        lexer.nextToken();
                        stmt.addOption("INDEX_ALL", new SQLCharExpr("Y"));
                    } else if ("N".equalsIgnoreCase(lexer.stringVal())) {
                        lexer.nextToken();
                        stmt.addOption("INDEX_ALL", new SQLCharExpr("N"));
                    } else {
                        throw new ParserException("INDEX_ALL accept parameter ['Y' or 'N'] only.");
                    }
                }
                continue;
            }

            if (lexer.identifierEquals("RT_INDEX_ALL")) {
                lexer.nextToken();
                accept(Token.EQ);
                if (lexer.token() == Token.LITERAL_CHARS) {
                    if ("Y".equalsIgnoreCase(lexer.stringVal())) {
                        lexer.nextToken();
                        stmt.addOption("RT_INDEX_ALL", new SQLCharExpr("Y"));
                    } else if ("N".equalsIgnoreCase(lexer.stringVal())) {
                        lexer.nextToken();
                        stmt.addOption("RT_INDEX_ALL", new SQLCharExpr("N"));
                    } else {
                        throw new ParserException("RT_INDEX_ALL accepts parameter ['Y' or 'N'] only.");
                    }
                }

                continue;
            }
            if (lexer.identifierEquals(FnvHash.Constants.ARCHIVE)) {
                lexer.nextToken();
                accept(Token.BY);
                acceptIdentifier("OSS");
                stmt.setArchiveBy(new SQLIdentifierExpr("OSS"));
                continue;
            }

            if (lexer.identifierEquals("STORAGE_TYPE")) {
                lexer.nextToken();
                accept(Token.EQ);
                stmt.addOption("STORAGE_TYPE", this.exprParser.charExpr());
                continue;
            }

            if (lexer.identifierEquals("STORAGE_POLICY")) {
                lexer.nextToken();
                accept(Token.EQ);

                if (lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.LITERAL_ALIAS) {
                    String policy = StringUtils.removeNameQuotes(lexer.stringVal());
                    stmt.addOption("STORAGE_POLICY", new SQLCharExpr(policy));
                    lexer.nextToken();
                } else {
                    throw new ParserException("syntax error. " + lexer.info());
                }

                continue;
            }

            if (lexer.identifierEquals("HOT_PARTITION_COUNT")) {
                lexer.nextToken();
                accept(Token.EQ);
                try {
                    stmt.addOption("HOT_PARTITION_COUNT", this.exprParser.integerExpr());
                } catch (Exception e) {
                    throw new ParserException("only integer number is supported for hot_partition_count");
                }
                continue;
            }

            if (lexer.identifierEquals("TABLE_PROPERTIES")) {
                lexer.nextToken();
                accept(Token.EQ);
                stmt.addOption("TABLE_PROPERTIES", exprParser.charExpr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.ENCRYPTION)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("ENCRYPTION", this.exprParser.expr());
                continue;
            } else if (lexer.identifierEquals(FnvHash.Constants.COMPRESSION)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("COMPRESSION", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("BLOCK_FORMAT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.addOption("BLOCK_FORMAT", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.CLUSTERED)) {
                lexer.nextToken();
                accept(Token.BY);
                accept(Token.LPAREN);
                for (; ; ) {
                    SQLSelectOrderByItem item = this.exprParser.parseSelectOrderByItem();
                    stmt.addClusteredByItem(item);
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
                continue;
            }

            if (lexer.token() == Token.PARTITION) {
                SQLPartitionBy partitionClause = parsePartitionBy();
                stmt.setPartitioning(partitionClause);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.LOCAL)) {
                SQLPartitionBy localPartitionClause = parseLocalPartitionBy();
                stmt.setLocalPartitioning(localPartitionClause);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.BROADCAST)) {
                lexer.nextToken();
                stmt.setBroadCast(true);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.SINGLE)) {
                lexer.nextToken();
                stmt.setSingle(true);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.DISTRIBUTE) || lexer.identifierEquals(
                FnvHash.Constants.DISTRIBUTED)) {
                lexer.nextToken();
                accept(Token.BY);
                if (lexer.identifierEquals(FnvHash.Constants.HASH)) {
                    lexer.nextToken();
                    accept(Token.LPAREN);
                    for (; ; ) {
                        SQLName name = this.exprParser.name();
                        stmt.getDistributeBy().add(name);
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    accept(Token.RPAREN);
                    stmt.setDistributeByType(new SQLIdentifierExpr("HASH"));
                } else if (lexer.identifierEquals(FnvHash.Constants.BROADCAST)) {
                    lexer.nextToken();
                    stmt.setDistributeByType(new SQLIdentifierExpr("BROADCAST"));
                }
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.DBPARTITION)) {
                lexer.nextToken();
                accept(Token.BY);
                SQLExpr dbPartitoinBy = this.exprParser.primary();
                stmt.setDbPartitionBy(dbPartitoinBy);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.DBPARTITIONS)) {
                lexer.nextToken();
                SQLExpr dbPartitoins = this.exprParser.primary();
                stmt.setDbPartitions(dbPartitoins);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.TBPARTITION)) {
                lexer.nextToken();
                accept(Token.BY);
                SQLExpr expr = this.exprParser.expr();
                if (lexer.identifierEquals(FnvHash.Constants.STARTWITH)) {
                    lexer.nextToken();
                    SQLExpr start = this.exprParser.primary();
                    acceptIdentifier("ENDWITH");
                    SQLExpr end = this.exprParser.primary();
                    expr = new SQLBetweenExpr(expr, start, end);
                }
                stmt.setTablePartitionBy(expr);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.TBPARTITIONS)) {
                lexer.nextToken();
                SQLExpr tbPartitions = this.exprParser.primary();
                stmt.setTablePartitions(tbPartitions);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.EXTPARTITION)) {
                lexer.nextToken();
                accept(Token.LPAREN);

                MySqlExtPartition partitionDef = new MySqlExtPartition();

                for (; ; ) {
                    MySqlExtPartition.Item item = new MySqlExtPartition.Item();

                    if (lexer.identifierEquals(FnvHash.Constants.DBPARTITION)) {
                        lexer.nextToken();
                        SQLName name = this.exprParser.name();
                        item.setDbPartition(name);
                        accept(Token.BY);
                        SQLExpr value = this.exprParser.primary();
                        item.setDbPartitionBy(value);
                    }

                    if (lexer.identifierEquals(FnvHash.Constants.TBPARTITION)) {
                        lexer.nextToken();
                        SQLName name = this.exprParser.name();
                        item.setTbPartition(name);
                        accept(Token.BY);
                        SQLExpr value = this.exprParser.primary();
                        item.setTbPartitionBy(value);
                    }

                    item.setParent(partitionDef);
                    partitionDef.getItems().add(item);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    } else {
                        break;
                    }
                }
                accept(Token.RPAREN);
                stmt.setExPartition(partitionDef);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.OPTIONS)) {
                lexer.nextToken();
                accept(Token.LPAREN);

                stmt.putAttribute("ads.options", Boolean.TRUE);
                for (; ; ) {
                    String name = lexer.stringVal();
                    lexer.nextToken();
                    accept(Token.EQ);
                    SQLExpr value = this.exprParser.primary();
                    stmt.addOption(name, value);
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }

                accept(Token.RPAREN);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.STORED)) {
                lexer.nextToken();
                accept(Token.BY);
                SQLName name = this.exprParser.name();
                stmt.setStoredBy(name);
            }

            if (lexer.token() == Token.WITH) {
                lexer.nextToken();
                accept(Token.LPAREN);

                for (; ; ) {
                    String name = lexer.stringVal();
                    lexer.nextToken();
                    accept(Token.EQ);
                    SQLName value = this.exprParser.name();
                    stmt.getWith().put(name, value);
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }

                accept(Token.RPAREN);
                continue;
            }

            if (lexer.token() == (Token.HINT)) {
                this.exprParser.parseHints(stmt.getOptionHints());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.ROUTE)) {
                lexer.nextToken();

                accept(Token.BY);
                acceptIdentifier("HASH");
                accept(Token.LPAREN);

                for (; ; ) {
                    stmt.getRouteBy().add(this.getExprParser().identifier());
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }

                accept(Token.RPAREN);

                acceptIdentifier("EFFECTED");
                accept(Token.BY);
                accept(Token.LPAREN);
                stmt.setEffectedBy(this.getExprParser().identifier());
                accept(Token.RPAREN);

                continue;
            }

            break;
        }

        if (lexer.token() == (Token.ON)) {
            throw new ParserException("TODO. " + lexer.info());
        }

        if (lexer.token() == Token.REPLACE) {
            lexer.nextToken();
            stmt.setReplace(true);
        } else if (lexer.identifierEquals("IGNORE")) {
            lexer.nextToken();
            stmt.setIgnore(true);
        }

        if (lexer.token() == (Token.AS)) {
            lexer.nextToken();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                SQLSelect query = new MySqlSelectParser(this.exprParser).select();
                stmt.setSelect(query);
                accept(Token.RPAREN);
            } else if (lexer.token() == Token.WITH) {
                SQLSelect query = new MySqlSelectParser(this.exprParser).select();
                stmt.setSelect(query);
            }
        }

        SQLCommentHint hint = null;
        if (lexer.token() == Token.HINT) {
            hint = this.exprParser.parseHint();
        }

        if (lexer.token() == (Token.SELECT)) {
            SQLSelect query = new MySqlSelectParser(this.exprParser).select();
            if (hint != null) {
                query.setHeadHint(hint);
            }
            stmt.setSelect(query);

            if (lexer.token() == Token.WITH) {
                lexer.nextToken();
                if (lexer.identifierEquals(FnvHash.Constants.NO)) {
                    lexer.nextToken();
                    acceptIdentifier("DATA");
                    stmt.setWithData(false);
                } else {
                    acceptIdentifier("DATA");
                    stmt.setWithData(true);
                }
            }
        }

        while (lexer.token() == (Token.HINT)) {
            this.exprParser.parseHints(stmt.getOptionHints());
        }
        return stmt;
    }

    public SQLPartitionBy parseLocalPartitionBy() {
        lexer.nextToken();
        accept(Token.PARTITION);
        accept(Token.BY);
        acceptIdentifier("RANGE");

        SQLPartitionByRange partitionClause = new SQLPartitionByRange();

        accept(Token.LPAREN);
        partitionClause.addColumn(this.exprParser.name());
        accept(Token.RPAREN);

        if (lexer.identifierEquals(FnvHash.Constants.STARTWITH)) {
            lexer.nextToken();
            partitionClause.setStartWith(exprParser.expr());
        }

        partitionClause.setInterval(getExprParser().parseInterval());

        if (lexer.identifierEquals("EXPIRE")) {
            acceptIdentifier("EXPIRE");
            acceptIdentifier("AFTER");
            partitionClause.setExpireAfter((SQLIntegerExpr) exprParser.expr());
        }

        if (lexer.identifierEquals("PRE")) {
            acceptIdentifier("PRE");
            acceptIdentifier("ALLOCATE");
            partitionClause.setPreAllocate((SQLIntegerExpr) exprParser.expr());
        }

        if (lexer.identifierEquals("PIVOTDATE")) {
            acceptIdentifier("PIVOTDATE");
            partitionClause.setPivotDateExpr(exprParser.expr());
        }

        if (lexer.token() == Token.DISABLE) {
            lexer.nextToken();
            acceptIdentifier("SCHEDULE");
            partitionClause.setDisableSchedule(true);
        }

        return partitionClause;
    }

    public SQLPartitionBy parsePartitionBy() {
        return parsePartitionBy(false);
    }

    public SQLPartitionBy parsePartitionBy(boolean forTableGroup) {
        lexer.nextToken();
        accept(Token.BY);

        SQLPartitionBy partitionClause;

        boolean linera = false;
        if (lexer.identifierEquals(FnvHash.Constants.LINEAR)) {
            lexer.nextToken();
            linera = true;
        }

        if (lexer.token() == Token.KEY) {
            MySqlPartitionByKey clause = new MySqlPartitionByKey();
            clause.setForTableGroup(forTableGroup);
            lexer.nextToken();

            if (linera) {
                clause.setLinear(true);
            }

            if (lexer.identifierEquals(FnvHash.Constants.ALGORITHM)) {
                lexer.nextToken();
                accept(Token.EQ);
                clause.setAlgorithm(lexer.integerValue().shortValue());
                lexer.nextToken();
            }

            accept(Token.LPAREN);
            for (; ; ) {
                if (forTableGroup) {
                    clause.addColumnDefinition(this.exprParser.parseColumn(true));
                } else {
                    boolean scanEnd = false;
                    if (lexer.token() == Token.RPAREN) {
                        scanEnd = true;
                    }
                    if (!scanEnd) {
                        SQLExpr partColExpr = this.exprParser.expr();
                        if (partColExpr != null) {
                            clause.addColumn(partColExpr);
                        }
                    }
                }
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }
            accept(Token.RPAREN);

            partitionClause = clause;

            partitionClauseRest(clause);
        } else if (lexer.identifierEquals("HASH") || lexer.identifierEquals("UNI_HASH")) {
            SQLPartitionByHash clause = new SQLPartitionByHash();
            clause.setForTableGroup(forTableGroup);

            if (lexer.identifierEquals("UNI_HASH")) {
                clause.setUnique(true);
            }

            lexer.nextToken();

            if (linera) {
                clause.setLinear(true);
            }

            if (lexer.token() == Token.KEY) {
                lexer.nextToken();
                clause.setKey(true);
            }

            accept(Token.LPAREN);
            for (; ; ) {
                if (forTableGroup) {
                    clause.addColumnDefinition(this.exprParser.parseColumn(true));
                } else {
                    clause.addColumn(this.exprParser.expr());
                }
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            accept(Token.RPAREN);
            partitionClause = clause;

            partitionClauseRest(clause);

        } else if (lexer.identifierEquals("UDF_HASH")) {
            SQLPartitionByUdfHash clause = new SQLPartitionByUdfHash();
            lexer.nextToken();
            accept(Token.LPAREN);

//            this.exprParser.exprList(clause.getColumns(), clause);
            for (; ; ) {
                if (forTableGroup) {
                    clause.addColumnDefinition(this.exprParser.parseColumn(true));
                } else {
                    clause.addColumn(this.exprParser.expr());
                }
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            accept(Token.RPAREN);
            clause.setForTableGroup(forTableGroup);
            partitionClause = clause;
            partitionClauseRest(clause);
        } else if (lexer.identifierEquals("RANGE")) {
            SQLPartitionByRange clause = partitionByRange(forTableGroup);
            partitionClause = clause;
            clause.setForTableGroup(forTableGroup);

            partitionClauseRest(clause);

        } else if (lexer.identifierEquals("VALUE")) {
            SQLPartitionByValue clause = partitionByValue();
            partitionClause = clause;
            clause.setForTableGroup(forTableGroup);

            partitionClauseRest(clause);

        } else if (lexer.identifierEquals("LIST")) {
            lexer.nextToken();
            SQLPartitionByList clause = new SQLPartitionByList();
            clause.setForTableGroup(forTableGroup);

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                if (forTableGroup) {
                    clause.addColumnDefinition(this.exprParser.parseColumn(true));
                } else {
                    clause.addColumn(this.exprParser.expr());
                }
                accept(Token.RPAREN);
            } else {
                acceptIdentifier("COLUMNS");
                clause.setColumns(true);
                accept(Token.LPAREN);
                for (; ; ) {
                    if (forTableGroup) {
                        clause.addColumnDefinition(this.exprParser.parseColumn(true));
                    } else {
                        clause.addColumn(this.exprParser.expr());
                    }
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
            }
            partitionClause = clause;

            partitionClauseRest(clause);
        } else if (lexer.token() == Token.IDENTIFIER) {
            SQLPartitionByRange clause = partitionByRange(forTableGroup);
            partitionClause = clause;
            clause.setForTableGroup(forTableGroup);

            partitionClauseRest(clause);
        } else {
            throw new ParserException("TODO. " + lexer.info());
        }

        if (lexer.identifierEquals(FnvHash.Constants.LIFECYCLE)) {
            lexer.nextToken();
            partitionClause.setLifecycle((SQLIntegerExpr) exprParser.expr());
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            for (; ; ) {
                SQLPartition partitionDef = this.getExprParser().parsePartition();

                partitionClause.addPartition(partitionDef);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                } else {
                    break;
                }
            }
            accept(Token.RPAREN);
        }
        return partitionClause;
    }

    protected SQLPartitionByValue partitionByValue() {
        SQLPartitionByValue clause = new SQLPartitionByValue();
        if (lexer.identifierEquals(FnvHash.Constants.VALUE)) {
            lexer.nextToken();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                clause.addColumn(this.exprParser.expr());
                accept(Token.RPAREN);
            }
        }
        return clause;
    }

    protected SQLPartitionByRange partitionByRange(boolean forTableGroup) {
        SQLPartitionByRange clause = new SQLPartitionByRange();
        if (lexer.identifierEquals(FnvHash.Constants.RANGE)) {
            lexer.nextToken();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                if (forTableGroup) {
                    clause.addColumnDefinition(this.exprParser.parseColumn(true));
                } else {
                    clause.addColumn(this.exprParser.expr());
                }
                accept(Token.RPAREN);
            } else {
                acceptIdentifier("COLUMNS");
                clause.setColumns(true);
                accept(Token.LPAREN);
                for (; ; ) {
                    if (forTableGroup) {
                        clause.addColumnDefinition(this.exprParser.parseColumn(true));
                    } else {
                        clause.addColumn(this.exprParser.expr());
                    }
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
            }
        } else {
            SQLExpr expr = this.exprParser.expr();
            if (lexer.identifierEquals(FnvHash.Constants.STARTWITH)) {
                lexer.nextToken();
                SQLExpr start = this.exprParser.primary();
                acceptIdentifier("ENDWITH");
                SQLExpr end = this.exprParser.primary();
                expr = new SQLBetweenExpr(expr, start, end);
            }
            clause.setInterval(expr);
        }

        return clause;
    }

    protected void partitionClauseRest(SQLPartitionBy clause) {
        if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)
            || lexer.identifierEquals(FnvHash.Constants.TBPARTITIONS)
            || lexer.identifierEquals(FnvHash.Constants.DBPARTITIONS)) {
            lexer.nextToken();
            SQLIntegerExpr countExpr = this.exprParser.integerExpr();
            clause.setPartitionsCount(countExpr);
        }

        if (lexer.token() == Token.PARTITION) {
            lexer.nextToken();

            if (lexer.identifierEquals("NUM")) {
                lexer.nextToken();
            }

            clause.setPartitionsCount(this.exprParser.expr());

            clause.putAttribute("ads.partition", Boolean.TRUE);
        }

        if (lexer.identifierEquals(FnvHash.Constants.LIFECYCLE)) {
            lexer.nextToken();
            clause.setLifecycle((SQLIntegerExpr) exprParser.expr());
        }

        boolean foundPartSubBySpecTemp = false;
        boolean foundPartSubByDef = false;
        if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION)) {
            lexer.nextToken();
            accept(Token.BY);

            SQLSubPartitionBy subPartitionByClause = null;
            foundPartSubBySpecTemp = true;
            foundPartSubByDef = true;
            boolean linear = false;
            if (lexer.identifierEquals("LINEAR")) {
                lexer.nextToken();
                linear = true;
            }

            if (lexer.token() == Token.KEY) {
                MySqlSubPartitionByKey subPartitionKey = new MySqlSubPartitionByKey();
                lexer.nextToken();

                if (linear) {
                    clause.setLinear(true);
                }

                if (lexer.identifierEquals(FnvHash.Constants.ALGORITHM)) {
                    lexer.nextToken();
                    accept(Token.EQ);
                    subPartitionKey.setAlgorithm(lexer.integerValue().shortValue());
                    lexer.nextToken();
                }

                accept(Token.LPAREN);
                for (; ; ) {
                    if (clause.isForTableGroup()) {
                        subPartitionKey.addColumnDefinition(this.exprParser.parseColumn(true));
                    } else {
                        subPartitionKey.addColumn(this.exprParser.name());
                    }
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);

                subPartitionByClause = subPartitionKey;

            } else if (lexer.identifierEquals("VALUE")) {
                MySqlSubPartitionByValue subPartitionByValue = new MySqlSubPartitionByValue();
                lexer.nextToken();
                accept(Token.LPAREN);
                for (; ; ) {
                    if (clause.isForTableGroup()) {
                        subPartitionByValue.addColumnDefinition(this.exprParser.parseColumn(true));
                    } else {
                        subPartitionByValue.addColumn(this.exprParser.name());
                    }
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);

                subPartitionByClause = subPartitionByValue;

            } else if (lexer.identifierEquals("HASH")) {
                lexer.nextToken();
                SQLSubPartitionByHash subPartitionHash = new SQLSubPartitionByHash();
                if (linear) {
                    clause.setLinear(true);
                }

                if (lexer.token() == Token.KEY) {
                    lexer.nextToken();
                    subPartitionHash.setKey(true);
                }

                accept(Token.LPAREN);
                //subPartitionHash.setExpr(this.exprParser.expr());
                for (; ; ) {
                    if (clause.isForTableGroup()) {
                        subPartitionHash.addColumnDefinition(this.exprParser.parseColumn(true));
                    } else {
                        subPartitionHash.addColumn(this.exprParser.expr());
                    }
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
                subPartitionByClause = subPartitionHash;

            } else if (lexer.identifierEquals("UDF_HASH")) {
                lexer.nextToken();
                SQLSubPartitionByUdfHash subPartitioUdfHash = new SQLSubPartitionByUdfHash();

                accept(Token.LPAREN);
                //subPartitionHash.setExpr(this.exprParser.expr());
                for (; ; ) {
                    if (clause.isForTableGroup()) {
                        subPartitioUdfHash.addColumnDefinition(this.exprParser.parseColumn(true));
                    } else {
                        subPartitioUdfHash.addColumn(this.exprParser.expr());
                    }
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
                subPartitionByClause = subPartitioUdfHash;

            } else if (lexer.identifierEquals("LIST")) {
                lexer.nextToken();
                SQLSubPartitionByList list = new SQLSubPartitionByList();
                if (lexer.identifierEquals("COLUMNS")) {
                    acceptIdentifier("COLUMNS");
                    list.setColumns(true);
                }
                accept(Token.LPAREN);
                for (; ; ) {
                    if (clause.isForTableGroup()) {
                        list.addColumnDefinition(this.exprParser.parseColumn(true));
                    } else {
                        list.addColumn(this.exprParser.expr());
                    }
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
                subPartitionByClause = list;
            } else if (lexer.identifierEquals(FnvHash.Constants.RANGE)) {
                lexer.nextToken();
                SQLSubPartitionByRange range = new SQLSubPartitionByRange();
                if (lexer.identifierEquals("COLUMNS")) {
                    acceptIdentifier("COLUMNS");
                    range.setColumns(true);
                }
                accept(Token.LPAREN);
                for (; ; ) {
                    if (clause.isForTableGroup()) {
                        range.addColumnDefinition(this.exprParser.parseColumn(true));
                    } else {
                        range.addColumn(this.exprParser.expr());
                    }
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
                subPartitionByClause = range;
            }

            boolean needParseSubPartSpecDef = false;

            boolean foundSubPartTempKeyWords = false;
            if (foundPartSubBySpecTemp) {
                if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITIONS)) {
                    /**
                     * parse for subpartition-template:
                     * subpartition by xxx(c1)
                     * ->[SUBPARTITIONS cnt]
                     * ...
                     */
                    lexer.nextToken();
                    Number intValue = lexer.integerValue();
                    SQLIntegerExpr numExpr = new SQLIntegerExpr(intValue);
                    subPartitionByClause.setSubPartitionsCount(numExpr);
                    lexer.nextToken();
                }

                /**
                 * parse for subpartition-template:
                 * subpartition by xxx(c1) [SUBPARTITIONS cnt]
                 * ->[SUBPARTITION TEMPLATE]
                 * ...
                 */
                if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION)) {
                    lexer.nextToken();
                    if (lexer.identifierEquals("TEMPLATE")) {
                        acceptIdentifier("TEMPLATE");
                        foundSubPartTempKeyWords = true;
                    }
                }

                /**
                 * parse for subpartition-template:
                 * subpartition by xxx(c1) [SUBPARTITIONS cnt]
                 * [subpartition template]
                 * ->(
                 *      subpartition sp1 ...
                 *
                 * or
                 *
                 * parse for subpartition-template:
                 * subpartition by xxx(c1) [SUBPARTITIONS cnt]
                 * [subpartition template]
                 * ->(
                 *      partition p1 ...
                 */
                if (lexer.token() == Token.LPAREN) {
                    Lexer.SavePoint sp = lexer.mark();
                    lexer.nextToken();
                    if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION)) {
                        /**
                         * parse for subpartition-template:
                         * subpartition by xxx(c1) [subpartitions cnt]
                         * [subpartition template]
                         * ( subpartition sp1 ..., subpartition spk ... )
                         */
                        needParseSubPartSpecDef = true;
                    } else {
                        /**
                         * parse for non-subpartition-template:
                         * subpartition by xxx(c1) [subpartitions cnt]
                         * [subpartition template]
                         * ( partition p1 (
                         *   subpartition sp1 ...
                         *   ),
                         *   partition sp2 (
                         *   ...
                         *   ),...
                         * )
                         */
                        lexer.reset(sp);
                    }
                }
            }

            if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITIONS)) {
                /**
                 * parse for subpartition-template:
                 * subpartition by xxx(c1)
                 * subpartitions cnt
                 * ...
                 */
                lexer.nextToken();
                Number intValue = lexer.integerValue();
                SQLIntegerExpr numExpr = new SQLIntegerExpr(intValue);
                subPartitionByClause.setSubPartitionsCount(numExpr);
                lexer.nextToken();
            } else if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)) {
                if (foundPartSubByDef) {
                    throw new ParserException("syntax error, 'partitions' after 'subpartition by' is not allowed");
                }
                // ignore adb syntax
//                // ADB
//                lexer.nextToken();
//                subPartitionByClause.setSubPartitionsCount((SQLIntegerExpr) exprParser.expr());
//                subPartitionByClause.getAttributes().put("adb.partitons", true);
            }

            if (needParseSubPartSpecDef) {
                /**
                 * parse for subpartition-template:
                 * subpartition by xxx(c1) [subpartition cnt]
                 * [subpartition template]
                 * ( subpartition sp1 ..., subpartition spk ... )
                 */
                for (; ; ) {
                    acceptIdentifier("SUBPARTITION");
                    SQLSubPartition subPartition = this.getExprParser().parseSubPartition();
                    subPartitionByClause.getSubPartitionTemplate().add(subPartition);
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
            }

            if (lexer.identifierEquals(FnvHash.Constants.LIFECYCLE)) {
                lexer.nextToken();
                subPartitionByClause.setLifecycle((SQLIntegerExpr) exprParser.expr());
            }

            if (subPartitionByClause != null) {
                subPartitionByClause.setLinear(linear);

                subPartitionByClause.setForTableGroup(clause.isForTableGroup());
                clause.setSubPartitionBy(subPartitionByClause);
            }
        }
    }

    private boolean parseTableOptionCharsetOrCollate(MySqlCreateTableStatement stmt) {
        if (lexer.token() == Token.DEFAULT) {
            lexer.nextToken();
        }

        if (lexer.identifierEquals("CHARACTER")) {
            lexer.nextToken();
            accept(Token.SET);
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLExpr charset;
            if (lexer.token() == Token.IDENTIFIER) {
                charset = new SQLIdentifierExpr(lexer.stringVal());
                lexer.nextToken();
            } else if (lexer.token() == Token.LITERAL_CHARS) {
                charset = new SQLCharExpr(lexer.stringVal());
                lexer.nextToken();
            } else {
                charset = this.exprParser.primary();
            }
            stmt.addOption("CHARACTER SET", charset);
            return true;
        }

        if (lexer.identifierEquals("CHARSET")) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLExpr charset;
            if (lexer.token() == Token.IDENTIFIER) {
                charset = new SQLIdentifierExpr(lexer.stringVal());
                lexer.nextToken();
            } else if (lexer.token() == Token.LITERAL_CHARS) {
                charset = new SQLCharExpr(lexer.stringVal());
                lexer.nextToken();
            } else {
                charset = this.exprParser.primary();
            }
            stmt.addOption("CHARSET", charset);
            return true;
        }

        if (lexer.identifierEquals("COLLATE")) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            stmt.addOption("COLLATE", this.exprParser.expr());
            return true;
        }

        return false;
    }

    protected SQLTableConstraint parseConstraint() {
        SQLName name = null;
        boolean hasConstaint = false;
        if (lexer.token() == (Token.CONSTRAINT)) {
            hasConstaint = true;
            lexer.nextToken();
        }

        if (lexer.token() == Token.IDENTIFIER) {
            name = this.exprParser.name();
        }

        SQLTableConstraint constraint = null;

        if (lexer.token() == (Token.KEY)) {
            MySqlKey key = new MySqlKey();
            this.exprParser.parseIndex(key.getIndexDefinition());
            key.setHasConstraint(hasConstaint);

            if (name != null) {
                key.setName(name);
            }

            constraint = key;
        } else if (lexer.token() == Token.PRIMARY) {
            MySqlPrimaryKey pk = this.getExprParser().parsePrimaryKey();
            if (name != null) {
                pk.setName(name);
            }
            pk.setHasConstraint(hasConstaint);
            constraint = pk;
        } else if (lexer.token() == Token.UNIQUE) {
            MySqlUnique uk = this.getExprParser().parseUnique();
            // should not use CONSTRAINT [symbol] for index name if index_name already specified
            if (name != null && uk.getName() == null) {
                uk.setName(name);
            }

            uk.setHasConstraint(hasConstaint);

            constraint = uk;
        } else if (lexer.token() == Token.FOREIGN) {
            MysqlForeignKey fk = this.getExprParser().parseForeignKey();
            fk.setName(name);
            fk.setHasConstraint(hasConstaint);
            constraint = fk;
        } else if (lexer.token() == Token.CHECK) {
            lexer.nextToken();
            SQLCheck check = new SQLCheck();
            check.setName(name);
            SQLExpr expr = this.exprParser.expr();
            check.setExpr(expr);
            constraint = check;
        }

        if (constraint != null) {
            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                SQLExpr comment = this.exprParser.primary();
                constraint.setComment(comment);
            }

            return constraint;
        }

        throw new ParserException("TODO. " + lexer.info());
    }
}

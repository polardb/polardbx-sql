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

package com.alibaba.polardbx.optimizer.parse.custruct;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.eagleeye.EagleeyeHelper;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLHint;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint.Argument;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint.Function;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLGroupingSetExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlIndexHintImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithSomethingVisitor;
import com.alibaba.polardbx.optimizer.exception.SqlParserException;
import com.alibaba.polardbx.optimizer.hint.operator.HintArgKey;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.parse.FastSqlParserException;
import com.alibaba.polardbx.optimizer.parse.ParserStaticConstant;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameterKey;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.OutFileParams;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlColumnDeclaration.ColumnNull;
import org.apache.calcite.sql.SqlColumnDeclaration.SpecialIndex;
import org.apache.calcite.sql.SqlColumnDeclaration.Storage;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlReferenceDefinition;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Fast SQL 各个节点到Calcite的映射工具
 *
 * @author hongxi.chx on 2017/11/21.
 * @since 5.0.0
 */
public final class FastSqlConstructUtils {

    public static SqlDataTypeSpec convertDataType(SQLColumnDefinition columnDefinition, ContextParameters context,
                                                  ExecutionContext ec) {
        final SQLDataTypeImpl sqlDataType = (SQLDataTypeImpl) columnDefinition.getDataType();

        final SqlIdentifier typeName = new SqlIdentifier(sqlDataType.getName(), SqlParserPos.ZERO);
        final boolean unsigned = sqlDataType.isUnsigned();
        final boolean zerofill = sqlDataType.isZerofill();
        /** for text only */
        boolean binary = false;
        SqlLiteral length = null;
        SqlLiteral decimals = null;
        SqlLiteral charSet = null;
        SqlLiteral collation = null;
        List<SqlLiteral> collectionVals = null;
        SqlLiteral fsp = null;

        if (null != columnDefinition.getCharsetExpr()) {
            final SqlNode charSetNode = FastSqlConstructUtils.convertToSqlNode(columnDefinition.getCharsetExpr(),
                context, ec);
            charSet = SqlLiteral.createCharString(RelUtils.lastStringValue(charSetNode), SqlParserPos.ZERO);
        }

        if (null != columnDefinition.getCollateExpr()) {
            final SqlNode collateNode = FastSqlConstructUtils.convertToSqlNode(columnDefinition.getCollateExpr(),
                context, ec);
            collation = SqlLiteral.createCharString(RelUtils.lastStringValue(collateNode), SqlParserPos.ZERO);
        }

        if (sqlDataType instanceof SQLCharacterDataType) {
            final SQLCharacterDataType charDataType = (SQLCharacterDataType) sqlDataType;

            binary = charDataType.isHasBinary();

            if (null != charDataType.getCharSetName()) {
                charSet = SqlLiteral.createCharString(charDataType.getCharSetName(), SqlParserPos.ZERO);
            }

            if (null != charDataType.getCollate()) {
                collation = SqlLiteral.createCharString(charDataType.getCollate(), SqlParserPos.ZERO);
            }
        }

        final SqlDataTypeSpec.DrdsTypeName dataType =
            SqlDataTypeSpec.DrdsTypeName.from(sqlDataType.getName().toUpperCase());
        if (null != dataType && GeneralUtil.isNotEmpty(sqlDataType.getArguments())) {
            if (dataType.isA(SqlDataTypeSpec.DrdsTypeName.TYPE_WITH_LENGTH) ||
                dataType.isA(SqlDataTypeSpec.DrdsTypeName.TYPE_WITH_LENGTH_STRING)) {
                length =
                    (SqlLiteral) FastSqlConstructUtils.convertToSqlNode(sqlDataType.getArguments().get(0), context, ec);

                if (dataType.isA(SqlDataTypeSpec.DrdsTypeName.TYPE_WITH_LENGTH_DECIMALS)
                    && sqlDataType.getArguments().size() == 2) {
                    decimals = (SqlLiteral) FastSqlConstructUtils.convertToSqlNode(sqlDataType.getArguments().get(1),
                        context, ec);
                }
            } else if (dataType.isA(SqlDataTypeSpec.DrdsTypeName.TYPE_WITH_FSP)) {
                fsp =
                    (SqlLiteral) FastSqlConstructUtils.convertToSqlNode(sqlDataType.getArguments().get(0), context, ec);
            } else if (dataType == SqlDataTypeSpec.DrdsTypeName.ENUM || dataType == SqlDataTypeSpec.DrdsTypeName.SET) {
                final ImmutableList.Builder<SqlLiteral> builder = ImmutableList.builder();

                for (SQLExpr arg : sqlDataType.getArguments()) {
                    final SqlLiteral value = (SqlLiteral) FastSqlConstructUtils.convertToSqlNode(arg, context, ec);
                    builder.add(value);
                }

                collectionVals = builder.build();
            }
        }

        return new SqlDataTypeSpec(SqlParserPos.ZERO,
            typeName,
            unsigned,
            zerofill,
            binary,
            length,
            decimals,
            charSet,
            collation,
            SqlUtil.wrapSqlNodeList(collectionVals),
            fsp);
    }

    public static SequenceBean initSequenceBean(SqlColumnDeclaration sqlColumnDeclaration) {
        SequenceBean sequence = new SequenceBean();
        sequence.setType(sqlColumnDeclaration.getAutoIncrementType());
        sequence.setUnitCount(sqlColumnDeclaration.getUnitCount());
        sequence.setUnitIndex(sqlColumnDeclaration.getUnitIndex());
        sequence.setInnerStep(sqlColumnDeclaration.getInnerStep());
        return sequence;
    }

    public static SequenceBean convertTableElements(SqlNodeList sqlNodeList, List<SQLTableElement> tableElementList,
                                                    ContextParameters context, ExecutionContext ec) {
        if (GeneralUtil.isEmpty(tableElementList)) {
            return null;
        }

        SequenceBean sequence = null;
        for (int i = 0; i < tableElementList.size(); ++i) {
            final SQLTableElement sqlTableElement = tableElementList.get(i);
            if (sqlTableElement instanceof SQLColumnDefinition) {
                final SqlColumnDeclaration sqlColumnDeclaration =
                    FastSqlConstructUtils.convertColumnDefinition((SQLColumnDefinition) sqlTableElement,
                        context, ec);
                sqlNodeList.add(sqlColumnDeclaration);

                if (sqlColumnDeclaration.isAutoIncrement()) {
                    sequence = initSequenceBean(sqlColumnDeclaration);
                }
            }
        }
        return sequence;
    }

    public static SqlNodeList constructSelectList(List<SQLSelectItem> selectList, ContextParameters context,
                                                  ExecutionContext ec) {
        if (selectList == null) {
            return null;
        }
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        // select
        List<SqlNode> columnNodes = new ArrayList<>(selectList.size());
        SqlNodeList selectListTemp = new SqlNodeList(columnNodes, SqlParserPos.ZERO);
        for (int i = 0, size = selectList.size(); i < size; ++i) {
            SQLSelectItem selectItem = selectList.get(i);
            if (selectItem.getClass() == SQLSelectItem.class) {
                visitor.visit(selectItem);
            } else {
                selectItem.accept(visitor);
            }
            SqlNode sqlNode = visitor.getSqlNode();

            selectListTemp.add(sqlNode);

        }
        return selectListTemp;
    }

    public static OutFileParams constructOutFile(SQLTableSource intoFile, ContextParameters context,
                                                 ExecutionContext ec) {
        if (null == intoFile) {
            return null;
        }
        OutFileParams outFileParams = null;
        SQLExpr intoFileExpr = ((SQLExprTableSource) intoFile).getExpr();
        if (intoFileExpr instanceof MySqlOutFileExpr) {
            outFileParams = new OutFileParams();
            MySqlOutFileExpr outFileExpr = (MySqlOutFileExpr) intoFileExpr;

            outFileParams.setCharset(outFileExpr.getCharset() == null ? "utf-8" : outFileExpr.getCharset());
            if (outFileExpr.getFile() == null) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.PARSER_ERROR,
                    "File name cannot be null.");
            }
            outFileParams.setFileName(getText(context, outFileExpr.getFile()));

            String text;
            // fields: escaped by 的默认值为'\\', enclosed by 的默认值为'', terminated by 的默认值为'\t'
            // lines: starting by 的默认值为'', terminated by 的默认值为'\n'
            text = getText(context, outFileExpr.getColumnsEnclosedBy());
            if (text != null && text.length() > 1) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.PARSER_ERROR,
                    "Enclosed by should be a single character.");
            }
            if ("".equals(text) || null == text) {
                outFileParams.setFieldEnclose(null);
            } else {
                byte[] temp = text.getBytes();
                outFileParams.setFieldEnclose(temp[0]);
            }
            outFileParams.setFieldEnclosedOptionally((outFileExpr.isColumnsEnclosedOptionally()));

            text = getText(context, outFileExpr.getColumnsEscaped());
            if (text != null && text.length() > 1) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.PARSER_ERROR,
                    "Escaped by should be a single character.");
            }
            if ("".equals(text)) {
                outFileParams.setFieldEscape(null);
            } else if (null == text) {
                outFileParams.setFieldEscape((byte) '\\');
            } else {
                byte[] temp = text.getBytes();
                outFileParams.setFieldEscape(temp[0]);
            }

            text = getText(context, outFileExpr.getColumnsTerminatedBy());
            outFileParams.setFieldTerminatedBy(null == text ? "\t" : text);

            text = getText(context, outFileExpr.getLinesStartingBy());
            outFileParams.setLinesStartingBy(null == text ? "" : text);

            text = getText(context, outFileExpr.getLinesTerminatedBy());
            outFileParams.setLineTerminatedBy(null == text ? "\n" : text);

            // set statistics
            outFileParams.setStatistics(outFileExpr.getStatistics());
        }
        return outFileParams;
    }

    private static String getText(ContextParameters context, SQLExpr expr) {
        if (null == expr) {
            return null;
        }
        if (expr instanceof SQLVariantRefExpr) {
            List<String> params = context.getParameter(ContextParameterKey.PARAMS);
            return params.get(((SQLVariantRefExpr) expr).getIndex());
        } else if (expr instanceof SQLCharExpr) {
            return ((SQLCharExpr) expr).getText();
        }
        throw new FastSqlParserException(FastSqlParserException.ExceptionType.PARSER_ERROR,
            "Parameters maybe incorrect, e.g. use 'test.txt' rather than test.txt ");
    }

    public static SqlNode constructFrom(SQLTableSource from, ContextParameters context, ExecutionContext ec) {
        if (from == null) {
            return null;
        }
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        Class<?> clazz = from.getClass();
        if (clazz == SQLJoinTableSource.class) {
            visitor.visit((SQLJoinTableSource) from);
        } else if (clazz == SQLExprTableSource.class) {
            visitor.visit((SQLExprTableSource) from);
        } else if (clazz == SQLSubqueryTableSource.class) {
            visitor.visit((SQLSubqueryTableSource) from);
        } else {
            from.accept(visitor);
        }
        return visitor.getSqlNode();
    }

    public static SqlNode constructForceIndex(SQLTableSource from, ContextParameters context, ExecutionContext ec) {
        if (from == null) {
            return null;
        }
        List<SQLHint> hints = from.getHints();
        if (hints == null) {
            return null;
        }
        SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
        for (int i = 0; i < hints.size(); i++) {
            SQLHint sqlHint = hints.get(i);
            if (sqlHint instanceof MySqlIndexHintImpl) {
                SqlNode sqlNode = convertToSqlNode(sqlHint, context, ec);
                sqlNodeList.add(sqlNode);
            }
        }
        return sqlNodeList;
    }

    public static SqlNode constructWhere(SQLExpr where, ContextParameters context, ExecutionContext ec) {
        if (where == null) {
            return null;
        }
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        visitor.visitExpr(where);
        return visitor.getSqlNode();
    }

    public static SqlNodeList constructGroupBy(SQLSelectGroupByClause group, ContextParameters context,
                                               ExecutionContext ec) {
        if (group == null) {
            return null;
        }
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        int itemSize = group.getItems().size();
        if (itemSize > 0) {
            List<SqlNode> groupByNodes = new ArrayList<>(itemSize);
            for (int i = 0; i < itemSize; ++i) {
                List<SQLExpr> items = group.getItems();
                SQLExpr sqlExpr = items.get(i);
                SqlNode sqlNode;
                if (sqlExpr instanceof SQLGroupingSetExpr) {
                    List<SQLExpr> parameters = ((SQLGroupingSetExpr) sqlExpr).getParameters();
                    List<SqlNode> paraNodes = new ArrayList<>(parameters.size());
                    for (SQLExpr parameter : parameters) {
                        SqlNode paraNode = visitor.convertToSqlNode(parameter);
                        paraNodes.add(paraNode);
                    }
                    sqlNode = SqlStdOperatorTable.GROUPING_SETS.createCall(SqlParserPos.ZERO, paraNodes);
                } else {
                    sqlNode = visitor.convertToSqlNode(sqlExpr);
                }
                groupByNodes.add(sqlNode);
            }
            if (group.isWithCube()) {
                SqlNode cube = SqlStdOperatorTable.CUBE.createCall(SqlParserPos.ZERO, groupByNodes);
                return new SqlNodeList(ImmutableList.of(cube), SqlParserPos.ZERO);
            } else if (group.isWithRollUp()) {
                SqlNode rollup = SqlStdOperatorTable.ROLLUP.createCall(SqlParserPos.ZERO, groupByNodes);
                return new SqlNodeList(ImmutableList.of(rollup), SqlParserPos.ZERO);
            }
            return new SqlNodeList(groupByNodes, SqlParserPos.ZERO);
        }
        return null;
    }

    public static SqlNode constructHaving(SQLSelectGroupByClause group, ContextParameters context,
                                          ExecutionContext ec) {
        if (group == null) {
            return null;
        }
        SQLExpr having = group.getHaving();
        if (having == null) {
            return null;
        }
        context.putParameter(ContextParameterKey.DO_HAVING, null);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        SqlNode sqlNode = visitor.convertToSqlNode(having);
        context.removeParameter(ContextParameterKey.DO_HAVING);
        return sqlNode;
    }

    public static SqlNodeList constructOrderBy(SQLOrderBy orderBy, ContextParameters context, ExecutionContext ec) {
        if (orderBy == null) {
            return null;
        }
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        visitor.visit(orderBy);
        return (SqlNodeList) visitor.getSqlNode();
    }

    public static SqlNodeList constructLimit(SQLLimit limit, ContextParameters context, ExecutionContext ec) {
        SqlNodeList sqlNode = (SqlNodeList) convertToSqlNode(limit, context, ec);
        return sqlNode;
    }

    public static SqlNode convertToSqlNode(SQLObject ast, ContextParameters context, ExecutionContext ec) {
        if (ast == null) {
            return null;
        }
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        ast.accept(visitor);
        return visitor.getSqlNode();
    }

    public static SqlNodeList constructForUpdate(boolean forUpdate, ContextParameters context) {
        if (forUpdate) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Not supported select for update");
        }
        return null;
    }

    public static SqlNodeList constructKeywords(MySqlSelectQueryBlock x, ContextParameters context) {
        SqlNodeList keywordList = null;
        List<SqlNode> keywordNodes = new ArrayList<SqlNode>(5);
        int option = x.getDistionOption();
        if (option != 0) {
            if (option == SQLSetQuantifier.DISTINCT
                || option == SQLSetQuantifier.DISTINCTROW) {
                keywordNodes.add(SqlSelectKeyword.DISTINCT.symbol(SqlParserPos.ZERO));
            }
            keywordList = new SqlNodeList(keywordNodes, SqlParserPos.ZERO);
        }
        return keywordList;
    }

    public static SqlTypeName getTypeNameOfParam(List<?> params, int index) {
        if (params.size() <= index) {
            throw new SqlParserException("The size of params is incorrect.");
        }

        Object param = params.get(index);
        if (param instanceof Integer || param instanceof Long) {
            return SqlTypeName.BIGINT;
        } else if (param instanceof BigInteger) {
            return SqlTypeName.BIGINT_UNSIGNED;
        } else if (param instanceof BigDecimal) {
            return SqlTypeName.DECIMAL;
        } else {
            return SqlTypeName.CHAR;
        }
    }

    public static SqlNodeList constructKeywords(MySqlInsertStatement x) {
        List<SqlNode> keywordNodes = new ArrayList<SqlNode>();
        if (x.isLowPriority()) {
            keywordNodes.add(SqlDmlKeyword.LOW_PRIORITY.symbol(SqlParserPos.ZERO));
        }
        if (x.isDelayed()) {
            keywordNodes.add(SqlDmlKeyword.DELAYED.symbol(SqlParserPos.ZERO));
        }
        if (x.isHighPriority()) {
            keywordNodes.add(SqlDmlKeyword.HIGH_PRIORITY.symbol(SqlParserPos.ZERO));
        }
        if (x.isIgnore()) {     // must be after low/delayed/high
            keywordNodes.add(SqlDmlKeyword.IGNORE.symbol(SqlParserPos.ZERO));
        }
        if (x.isOverwrite()) {
            keywordNodes.add(SqlDmlKeyword.OVERWRITE.symbol(SqlParserPos.ZERO));
        }
        return new SqlNodeList(keywordNodes, SqlParserPos.ZERO);
    }

    public static SqlNodeList constructKeywords(SQLReplaceStatement x) {
        List<SqlNode> keywordNodes = new ArrayList<>();
        if (x.isLowPriority()) {
            keywordNodes.add(SqlDmlKeyword.LOW_PRIORITY.symbol(SqlParserPos.ZERO));
        }
        if (x.isDelayed()) {
            keywordNodes.add(SqlDmlKeyword.DELAYED.symbol(SqlParserPos.ZERO));
        }
        return new SqlNodeList(keywordNodes, SqlParserPos.ZERO);
    }

    public static SqlNodeList constructKeywords(MySqlUpdateStatement x) {
        List<SqlNode> keywordNodes = new ArrayList<>();
        if (x.isLowPriority()) {
            keywordNodes.add(SqlDmlKeyword.LOW_PRIORITY.symbol(SqlParserPos.ZERO));
        }
        if (x.isIgnore()) {
            keywordNodes.add(SqlDmlKeyword.IGNORE.symbol(SqlParserPos.ZERO));
        }
        return new SqlNodeList(keywordNodes, SqlParserPos.ZERO);
    }

    public static SqlNodeList constructKeywords(MySqlDeleteStatement x) {
        List<SqlNode> keywordNodes = new ArrayList<>();
        if (x.isLowPriority()) {
            keywordNodes.add(SqlDmlKeyword.LOW_PRIORITY.symbol(SqlParserPos.ZERO));
        }
        if (x.isQuick()) {
            keywordNodes.add(SqlDmlKeyword.QUICK.symbol(SqlParserPos.ZERO));
        }
        if (x.isIgnore()) {
            keywordNodes.add(SqlDmlKeyword.IGNORE.symbol(SqlParserPos.ZERO));
        }
        return new SqlNodeList(keywordNodes, SqlParserPos.ZERO);
    }

    /**
     * If there are multiple VALUES, and all values in VALUES CLAUSE are literal,
     * convert the value clauses to a single value clause.
     */
    public static List<ValuesClause> convertToSingleValuesIfNeed(List<ValuesClause> valuesClauseList, int columnCount,
                                                                 ContextParameters context) {
        if (valuesClauseList.size() <= 1) {
            if (valuesClauseList.isEmpty() || (columnCount > 0
                && valuesClauseList.get(0).getValues().size() != columnCount)) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.PARSER_ERROR,
                    "Column count doesn't match value count at row " + 1);
            }
            return valuesClauseList;
        }

        Map<SQLVariantRefExpr, ColumnMeta> bindMapTypes = context != null ?
            (Map<SQLVariantRefExpr, ColumnMeta>) context.getParameter(ContextParameterKey.BIND_TYPE_PARAMS) : null;
        List<Object> params = context != null ? context.getParameter(ContextParameterKey.PARAMS) : null;

        // If they are all literals
        for (Ord<ValuesClause> o : Ord.zip(valuesClauseList)) {
            final ValuesClause clause = o.getValue();
            if ((columnCount > 0 && clause.getValues().size() != columnCount)) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.PARSER_ERROR,
                    "Column count doesn't match value count at row " + o.getKey());
            }

            for (SQLExpr expr : clause.getValues()) {
                if (expr instanceof SQLVariantRefExpr) {
                    if (((SQLVariantRefExpr) expr).getName().equals("?")) {
                        int index = ((SQLVariantRefExpr) expr).getIndex();
                        boolean existCast = false;
                        boolean allowInferType =
                            bindMapTypes != null && (CollectionUtils.isNotEmpty(params) && byte[].class.isInstance(
                                params.get(index)));
                        if (allowInferType) {
                            ColumnMeta columnMeta = bindMapTypes.get(expr);
                            if (columnMeta != null) {
                                existCast = DataTypeUtil.isStringType(columnMeta.getDataType()) ||
                                    DataTypeUtil.isBinaryType(columnMeta.getDataType());
                            }
                        }
                        if (!existCast) {
                            continue;
                        }
                    }
                }
                return valuesClauseList;
            }
        }

        // Return only the first values clause.
        return ImmutableList.of(valuesClauseList.get(0));
    }

    public static SqlNode getLimitOffsetForUpdateOrDelete(SqlNodeList limitNodes) {
        if (limitNodes == null) {
            return null;
        }

        if (limitNodes.size() == 2) {
            SqlNode offset = limitNodes.get(0);
            //参数化返回SqlDynamicParam
            if ((offset instanceof SqlDynamicParam)) {
                return limitNodes.get(0);
            }

            //是数字0；直接返回空，去除limit offset；原因：update limit ?; fastSql 解成 limit 0,?;
            if ((offset instanceof SqlLiteral) && ((SqlLiteral) offset).getValueAs(Integer.class) == 0) {
                return null;
            }

            return limitNodes.get(0);
        }

        return null;
    }

    /**
     * For delete and update, support limit m, n
     */
    public static SqlNode getLimitForUpdateOrDelete(SqlNodeList limitNodes, ExecutionContext ec) {
        if (limitNodes == null) {
            return null;
        }

        if (limitNodes.size() == 1) {
            return limitNodes.get(0);
        }

        if (limitNodes.size() == 2) {
            SqlNode offset = limitNodes.get(0);

            if ((offset instanceof SqlDynamicParam) && ec.getParams() != null) {
                return limitNodes.get(1);
            }
            if (!(offset instanceof SqlLiteral) || ((SqlLiteral) offset).getValueAs(Integer.class) > 0) {
                throw new UnsupportedOperationException("Does not support UPDATE/DELETE statement with offset.");
            }

            return limitNodes.get(1);
        }

        throw new UnsupportedOperationException("UPDATE/DELETE statement with illegal limit.");
    }

    public static SqlNodeList /**/convertHints(List<SQLCommentHint> hints, ContextParameters context,
                                               ExecutionContext ec) {
        if (hints == null) {
            return null;
        }

        List<SqlNode> nodes = new ArrayList<SqlNode>(hints.size());

        for (SQLCommentHint hint : hints) {
            if (hint instanceof TDDLHint) {
                nodes.add(convertTDDLHint((TDDLHint) hint, context, ec));
            }
        }

        return new SqlNodeList(nodes, SqlParserPos.ZERO);

    }

    public static OptimizerHint convertHints(List<SQLCommentHint> hints, OptimizerHint hintContext) {
        for (SQLCommentHint hint : hints) {
            if (!(hint instanceof TDDLHint)) {
                if (hintContext == null) {
                    hintContext = new OptimizerHint();
                }
                hintContext.addHint(hint.getText().toUpperCase());
            }
        }
        return hintContext;
    }

    private static SqlNodeList convertTDDLHint(TDDLHint hint, ContextParameters context, ExecutionContext ec) {

        List<TDDLHint.Function> functions = hint.getFunctions();
        List<SqlNode> funNodes = new ArrayList<SqlNode>(functions.size());

        /**
         * group hints by first param of each construct
         */
        List<SqlNode> group = new ArrayList<>();
        switch (hint.getType()) {
        case Function:
            for (TDDLHint.Function function : functions) {
                function = getNewFunction(function);
                final HintType hintType = HintType.of(function.getName());
                if (group.size() > 0 && HintType.CONSTRUCT == hintType) {
                    funNodes.add(new SqlNodeList(group, SqlParserPos.ZERO));
                    group = new ArrayList<>();
                    context.setUseHint(true);
                }

                final SqlNode funNode = buildHintFunctionNode(context, function, hintType, ec);

                if (hintType == HintType.CMD_INDEX) {
                    // independent group for CMD_INDEX
                    funNodes.add(SqlNodeList.of(funNode));
                    // do not set using outline hint so that this query will be plan cached
                } else {
                    group.add(funNode);
                }
            }
            break;
        case JSON:
            SqlNode[] argNodes = new SqlNode[1];
            SqlCharStringLiteral charString = SqlCharStringLiteral.createCharString(hint.getJson(),
                SqlParserPos.ZERO);
            argNodes[0] = charString;
            SqlNode funNode = new SqlBasicCall(
                new SqlUnresolvedFunction(new SqlIdentifier(ParserStaticConstant.CDB_JSON_HINT, SqlParserPos.ZERO),
                    null,
                    null,
                    null,
                    null,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION),
                argNodes,
                SqlParserPos.ZERO);
            group.add(funNode);
            break;
        case Unknown:
        default:
            break;
        } // end of switch

        if (group.size() > 0) {
            SqlNode groupNode = new SqlNodeList(group, SqlParserPos.ZERO);
            funNodes.add(groupNode);
            context.setUseHint(true);

        }

        return new SqlNodeList(funNodes, SqlParserPos.ZERO);
    }

    private static SqlNode buildHintFunctionNode(ContextParameters context, Function function, HintType hintType,
                                                 ExecutionContext ec) {
        List<Argument> arguments = function.getArguments();

        final String qbNameKey = HintArgKey.PUSHDOWN_HINT.get(5).getName();
        String qbNameValue = null;

        SqlNode[] argNodes = new SqlNode[arguments.size()];
        for (int i = 0; i < arguments.size(); i++) {
            Argument argument = arguments.get(i);
            SqlNode argName = convertToSqlNode(argument.getName(), context, ec);
            SqlNode argValue = convertToSqlNode(argument.getValue(), context, ec);

            List<SqlNode> arg = new ArrayList<SqlNode>();
            if (argName != null) {
                arg.add(argName);
            }
            if (argValue != null) {
                arg.add(argValue);
            }

            SqlNode argNode = null;
            if (arg.size() == 2) {
                argNode = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, arg);
            } else if (arg.size() == 1) {
                argNode = argValue;
            }

            final String key = argName == null ? null : RelUtils.stringValue(argName);
            if (HintType.PUSHDOWN == hintType && TStringUtil.equalsIgnoreCase(qbNameKey, key)) {
                qbNameValue = RelUtils.stringValue(argValue);
            }

            argNodes[i] = argNode;
        }

        if (HintType.PUSHDOWN == hintType && TStringUtil.isBlank(qbNameValue)) {
            qbNameValue = HintUtil.gerenateQbName();
            SqlNode[] newArgNodes = new SqlNode[argNodes.length + 1];
            for (int i = 0; i < argNodes.length; i++) {
                newArgNodes[i] = argNodes[i];
            }
            newArgNodes[argNodes.length] = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                ImmutableList.of(new SqlIdentifier(qbNameKey, SqlParserPos.ZERO),
                    SqlLiteral.createCharString(qbNameValue, SqlParserPos.ZERO)));
            argNodes = newArgNodes;
        }

        if (context.isTestMode()) {
            if (HintType.CMD_INDEX == hintType) {
                String index = ((SqlIdentifier) argNodes[1]).getLastName();
                SqlNode[] newArgNodes = new SqlNode[argNodes.length];
                newArgNodes[0] = new SqlIdentifier(HintUtil.findTestTableName(
                    ((SqlIdentifier) argNodes[0]).getLastName(), true, context.getTb2TestNames()),
                    SqlParserPos.ZERO);
                if (index.equalsIgnoreCase("PRIMARY")) {
                    newArgNodes[1] = argNodes[1];
                } else {
                    newArgNodes[1] = new SqlIdentifier(EagleeyeHelper.rebuildTableName(index, true),
                        SqlParserPos.ZERO);
                }

                argNodes = newArgNodes;
            } else if (HintType.CMD_BKA_JOIN == hintType ||
                HintType.CMD_NL_JOIN == hintType ||
                HintType.CMD_SORT_MERGE_JOIN == hintType ||
                HintType.CMD_HASH_JOIN == hintType ||
                HintType.CMD_MATERIALIZED_SEMI_JOIN == hintType ||
                HintType.CMD_SEMI_HASH_JOIN == hintType ||
                HintType.CMD_SEMI_NL_JOIN == hintType ||
                HintType.CMD_ANTI_NL_JOIN == hintType ||
                HintType.CMD_SEMI_SORT_MERGE_JOIN == hintType ||
                HintType.CMD_NO_JOIN == hintType ||
                HintType.CMD_SEMI_BKA_JOIN == hintType) {
                SqlNode[] newArgNodes = new SqlNode[argNodes.length];
                for (int i = 0; i < argNodes.length; i++) {
                    newArgNodes[i] = argNodes[i];
                }
                if (argNodes.length >= 1) {
                    newArgNodes[0] = operandsToSet(argNodes[0], context);
                }
                if (argNodes.length >= 2) {
                    newArgNodes[1] = operandsToSet(argNodes[1], context);
                }
                argNodes = newArgNodes;
            }

        }

        return new SqlBasicCall(
            new SqlUnresolvedFunction(new SqlIdentifier(function.getName(), SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION),
            argNodes,
            SqlParserPos.ZERO);
    }

    private static SqlNode operandsToSet(SqlNode sqlNode, ContextParameters context) {
        if (sqlNode.toString().equalsIgnoreCase("any")) {
            // just pass
            return sqlNode;
        } else if (sqlNode instanceof SqlIdentifier) {
            return new SqlIdentifier(
                HintUtil.findTestTableName(sqlNode.toString(), true, context.getTb2TestNames()), SqlParserPos.ZERO);
        } else if (sqlNode instanceof SqlBasicCall && sqlNode.getKind() == SqlKind.ROW) {
            SqlNode[] sqlNodes = ((SqlBasicCall) sqlNode).getOperands();
            SqlNode[] newArgNodes = new SqlNode[sqlNodes.length];
            for (int i = 0; i < sqlNodes.length; i++) {
                newArgNodes[i] = operandsToSet(sqlNodes[i], context);
            }
            return new SqlBasicCall(
                ((SqlBasicCall) sqlNode).getOperator(),
                newArgNodes,
                SqlParserPos.ZERO);
        }
        return sqlNode;
    }

    public static List<SqlIndexColumnName> constructIndexColumnNames(List<SQLSelectOrderByItem> columns) {
        if (null == columns) {
            return null;
        }

        final List<SqlIndexColumnName> result = new ArrayList<>();
        for (SQLSelectOrderByItem column : columns) {
            if (column.getExpr() instanceof SQLIdentifierExpr) {
                SqlIdentifier columnName = new SqlIdentifier(((SQLIdentifierExpr) column.getExpr()).normalizedName(),
                    SqlParserPos.ZERO);
                result.add(new SqlIndexColumnName(SqlParserPos.ZERO, columnName, null, null));
            } else if (column.getExpr() instanceof SQLMethodInvokeExpr) {
                final SQLMethodInvokeExpr columnCall = (SQLMethodInvokeExpr) column.getExpr();
                final SqlIdentifier columnName = new SqlIdentifier(SQLUtils.normalizeNoTrim(columnCall.getMethodName()),
                    SqlParserPos.ZERO);

                SqlLiteral length = null;
                Boolean asc = null;
                if (columnCall.getArguments() != null) {
                    for (SQLExpr arg : columnCall.getArguments()) {
                        if (arg instanceof SQLIntegerExpr) {
                            length = SqlLiteral.createExactNumeric(((SQLIntegerExpr) arg).getNumber().toString(),
                                SqlParserPos.ZERO);
                        } else if (arg instanceof SqlIdentifier) {
                            final SqlIdentifier argIdentifier = (SqlIdentifier) arg;
                            if ("DESC".equalsIgnoreCase(argIdentifier.getLastName())) {
                                asc = false;
                            } else if ("ASC".equalsIgnoreCase(argIdentifier.getLastName())) {
                                asc = true;
                            }
                        }
                    }
                }

                if (null != column.getType()) {
                    switch (column.getType()) {
                    case ASC:
                        asc = true;
                        break;
                    case DESC:
                        asc = false;
                        break;
                    default:
                        break;
                    }
                }

                result.add(new SqlIndexColumnName(SqlParserPos.ZERO, columnName, length, asc));
            }
        }

        return result;
    }

    public static List<SqlIndexColumnName> constructIndexCoveringNames(List<SQLName> covering) {
        if (null == covering) {
            return null;
        }

        final List<SqlIndexColumnName> result = new ArrayList<>();
        for (SQLName column : covering) {
            if (column instanceof SQLIdentifierExpr) {
                SqlIdentifier columnName = new SqlIdentifier(((SQLIdentifierExpr) column).normalizedName(),
                    SqlParserPos.ZERO);
                result.add(new SqlIndexColumnName(SqlParserPos.ZERO, columnName, null, null));
            }
        }

        return result;
    }

    private static TDDLHint.Function getNewFunction(TDDLHint.Function function) {
        return HintParser.instance.convertMethod(function);
    }

    public static SqlColumnDeclaration convertColumnDefinition(SQLColumnDefinition sqlTableElement,
                                                               ContextParameters context, ExecutionContext ec) {
        final SQLColumnDefinition tableColumn = sqlTableElement;
        SqlIdentifier tableSourceSqlNode = (SqlIdentifier) convertToSqlNode(tableColumn.getName(), context, ec);

        SqlDataTypeSpec sqlDataTypeSpec = FastSqlConstructUtils.convertDataType(tableColumn, context, ec);

        ColumnNull columnNull = null;
        SpecialIndex specialIndex = null;
        SqlReferenceDefinition referenceDefinition = null;
        final List<SQLColumnConstraint> constraints = tableColumn.getConstraints();
        if (constraints != null && constraints.size() > 0) {
            for (int j = 0; j < constraints.size(); j++) {
                final SQLColumnConstraint sqlColumnConstraint = constraints.get(j);
                if (sqlColumnConstraint instanceof SQLNotNullConstraint) {
                    sqlDataTypeSpec = sqlDataTypeSpec.withNullable(false);
                    columnNull = ColumnNull.NOTNULL;
                } else if (sqlColumnConstraint instanceof SQLNullConstraint) {
                    sqlDataTypeSpec = sqlDataTypeSpec.withNullable(true);
                    columnNull = ColumnNull.NULL;
                } else if (sqlColumnConstraint instanceof SQLColumnPrimaryKey) {
                    specialIndex = SpecialIndex.PRIMARY;
                } else if (sqlColumnConstraint instanceof SQLColumnUniqueKey) {
                    specialIndex = SpecialIndex.UNIQUE;
                } else if (sqlColumnConstraint instanceof SQLColumnReference) {
                    referenceDefinition = (SqlReferenceDefinition) convertToSqlNode(sqlColumnConstraint, context, ec);
                }
            }
        }

        SqlLiteral defaultValue = null;
        SqlCall defualtExpr = null;
        if (tableColumn.getDefaultExpr() != null) {
            if (tableColumn.getDefaultExpr() instanceof SQLBinaryOpExpr) {
                // Only collate will contact after some expr.
                // It's hard to fix it in fastSql so we deal it here.
                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) tableColumn.getDefaultExpr();
                if (binaryOpExpr.getOperator().name().equalsIgnoreCase("COLLATE")) {
                    tableColumn.setDefaultExpr(binaryOpExpr.getLeft());
                    tableColumn.setCollateExpr(binaryOpExpr.getRight());
                }
            }
            if (tableColumn.getDefaultExpr() instanceof SQLLiteralExpr) {
                defaultValue = (SqlLiteral) convertToSqlNode(tableColumn.getDefaultExpr(), context, ec);
            } else if (tableColumn.getDefaultExpr() instanceof SQLCurrentTimeExpr && !InstanceVersion.isMYSQL80()) {
                final SQLCurrentTimeExpr currentTimeExpr = (SQLCurrentTimeExpr) tableColumn.getDefaultExpr();
                defaultValue = SqlLiteral.createSymbol(currentTimeExpr.getType(), SqlParserPos.ZERO);
            } else {
                defualtExpr = (SqlCall) convertToSqlNode(tableColumn.getDefaultExpr(), context, ec);
            }
        }

        boolean onUpdateCurrentTimestamp = false;
        if (tableColumn.getOnUpdate() != null &&
            (tableColumn.getOnUpdate() instanceof SQLCurrentTimeExpr || (
                tableColumn.getOnUpdate() instanceof SQLMethodInvokeExpr
                    && ((SQLMethodInvokeExpr) tableColumn.getOnUpdate()).getMethodName()
                    .equalsIgnoreCase("current_timestamp")))) {
            onUpdateCurrentTimestamp = true;
        }

        final SqlLiteral comment = (SqlLiteral) convertToSqlNode(tableColumn.getComment(), context, ec);

        Storage storage = null;
        if (null != tableColumn.getStorage()) {
            final SqlIdentifier sqlStorage = (SqlIdentifier) convertToSqlNode(tableColumn.getStorage(), context, ec);
            storage = Storage.valueOf(sqlStorage.getLastName());
        }

        int unitCount = SequenceAttribute.UNDEFINED_UNIT_COUNT;
        int unitIndex = SequenceAttribute.UNDEFINED_UNIT_INDEX;
        int innerStep = SequenceAttribute.UNDEFINED_INNER_STEP;
        SequenceAttribute.Type autoIncrementType = null;
        if (tableColumn.isAutoIncrement()) {
            if (tableColumn.getUnitCount() != null) {
                if (tableColumn.getUnitCount() instanceof SQLIntegerExpr) {
                    unitCount = ((SQLIntegerExpr) tableColumn.getUnitCount()).getNumber().intValue();
                } else if (tableColumn.getUnitCount() instanceof SQLNumberExpr) {
                    unitCount = ((SQLNumberExpr) tableColumn.getUnitCount()).getNumber().intValue();
                }

            }

            if (tableColumn.getUnitIndex() != null) {
                if (tableColumn.getUnitIndex() instanceof SQLIntegerExpr) {
                    unitIndex = ((SQLIntegerExpr) tableColumn.getUnitIndex()).getNumber().intValue();
                } else if (tableColumn.getUnitIndex() instanceof SQLNumberExpr) {
                    unitIndex = ((SQLNumberExpr) tableColumn.getUnitIndex()).getNumber().intValue();
                }

            }
            if (tableColumn.getStep() != null) {
                if (tableColumn.getStep() instanceof SQLIntegerExpr) {
                    innerStep = ((SQLIntegerExpr) tableColumn.getStep()).getNumber().intValue();
                } else if (tableColumn.getStep() instanceof SQLNumberExpr) {
                    innerStep = ((SQLNumberExpr) tableColumn.getStep()).getNumber().intValue();
                }
            }

            autoIncrementType = SequenceBean.convertAutoIncrementType(tableColumn.getSequenceType());
        }

        boolean generatedAlways = false;
        boolean generatedAlwaysLogical = false;
        SqlCall generatedAlwaysExpr = null;

        if (tableColumn.getGeneratedAlawsAs() == null && tableColumn.isLogical()) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.PARSER_ERROR,
                String.format("Keyword LOGICAL can only be used for generated column, which [%s] is not",
                    tableColumn.getColumnName()));
        }

        if (tableColumn.getGeneratedAlawsAs() != null) {
            FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
            tableColumn.getGeneratedAlawsAs().accept(visitor);
            // Wrap with GEN_COL_WRAPPER_FUNC so that it will not be pushed down
            generatedAlwaysExpr =
                new SqlBasicCall(SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC, new SqlNode[] {visitor.getSqlNode()},
                    SqlParserPos.ZERO);
            generatedAlways = true;
            generatedAlwaysLogical = tableColumn.isLogical();

            if (defaultValue != null || defualtExpr != null) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.PARSER_ERROR,
                    String.format("Can not assign default value for generated column [%s].",
                        tableColumn.getColumnName()));
            }
        }

        return (SqlColumnDeclaration) SqlDdlNodes.column(SqlParserPos.ZERO,
            tableSourceSqlNode,
            sqlDataTypeSpec,
            columnNull,
            defaultValue,
            defualtExpr,
            tableColumn.isAutoIncrement(),
            specialIndex,
            comment,
            null,
            storage,
            referenceDefinition,
            onUpdateCurrentTimestamp,
            autoIncrementType,
            unitCount,
            unitIndex,
            innerStep,
            generatedAlways,
            generatedAlwaysLogical,
            generatedAlwaysExpr);
    }

    public static boolean collectSourceTable(SqlNode source, List<SqlNode> outTargetTables, List<SqlNode> outAliases,
                                             Map<SqlNode, List<SqlIdentifier>> subQueryTableMap,
                                             boolean withTableAlias, ExecutionContext ec) {
        if (null == source) {
            return withTableAlias;
        }

        if (source instanceof SqlIdentifier) {
            // UPDATE t1
            // DELETE t1 FROM t1
            outTargetTables.add(source);
            outAliases.add(source);
        } else if (source instanceof SqlBasicCall && source.getKind() == SqlKind.AS) {
            // UPDATE t1 a
            // UPDATE t1 AS a
            // DELETE t1 FROM t AS t1
            // DELETE FROM t1 USING t AS t1

            final SqlNode left = ((SqlBasicCall) source).operands[0];
            outTargetTables.add(left);
            outAliases.add(source);
            withTableAlias = true;

            // UPDATE t1 JOIN (SELECT t2.id FROM t2) b
            // UPDATE t1 JOIN (SELECT t2.id FROM t2) AS b
            // DELETE t1 FROM t1 JOIN (SELECT t2.id FROM t2) b
            // DELETE FROM t1 USING t1 JOIN (SELECT t2.id FROM t2) AS b
            if (left instanceof SqlSelect) {
                // Get tables in subquery
                final List<SqlIdentifier> tables = new ArrayList<>();
                left.accept(new ReplaceTableNameWithSomethingVisitor(DefaultSchema.getSchemaName(), ec) {
                    @Override
                    protected SqlNode buildSth(SqlNode sqlNode) {
                        tables.add((SqlIdentifier) sqlNode);
                        return sqlNode;
                    }
                });

                subQueryTableMap.put(left, tables);
            }
        } else if (source instanceof SqlJoin) {
            // UPDATE t1 JOIN t2
            // UPDATE t AS t1 JOIN t AS t2
            // DELETE t1 FROM t AS t1 JOIN t AS t2
            // DELETE FROM t1 USING t AS t1 JOIN t AS t2
            final SqlJoin join = (SqlJoin) source;
            withTableAlias |=
                collectSourceTable(join.getLeft(), outTargetTables, outAliases, subQueryTableMap, withTableAlias, ec);
            withTableAlias |=
                collectSourceTable(join.getRight(), outTargetTables, outAliases, subQueryTableMap, withTableAlias, ec);
        }

        return withTableAlias;
    }

    public static SqlUpdate collectTableInfo(SqlUpdate update, ExecutionContext ec) {

        final List<SqlNode> targetTables = new ArrayList<>();
        final List<SqlNode> aliases = new ArrayList<>();
        final Map<SqlNode, List<SqlIdentifier>> subQueryTableMap = new HashMap<>();

        boolean withAlias =
            collectSourceTable(update.getTargetTable(), targetTables, aliases, subQueryTableMap, false, ec);

        if (update.getCondition() instanceof SqlCall) {

            final TableFinder tableFinder = new TableFinder(DefaultSchema.getSchemaName(), ec);
            update.getCondition().accept(tableFinder);

            subQueryTableMap.putAll(tableFinder.getSubQueryTableMap());
        }

        if (update.getOrderList() != null && update.getOrderList().size() > 0) {
            final TableFinder tableFinder = new TableFinder(DefaultSchema.getSchemaName(), ec);
            update.getOrderList().accept(tableFinder);

            subQueryTableMap.putAll(tableFinder.getSubQueryTableMap());
        }

        return update.initTableInfo(new SqlNodeList(targetTables, SqlParserPos.ZERO),
            new SqlNodeList(aliases, SqlParserPos.ZERO),
            subQueryTableMap);
    }

    public static SqlUpdate constructUpdate(SqlNodeList keywords, SqlNode targetTable, SqlNodeList targetColumnList,
                                            SqlNodeList sourceExpressList, SqlNode condition, SqlIdentifier alias,
                                            SqlNodeList orderBySqlNode, SqlNode offset, SqlNode limit,
                                            SqlNodeList hints, OptimizerHint hintContext, ExecutionContext ec) {
        return collectTableInfo(new SqlUpdate(SqlParserPos.ZERO,
            targetTable,
            targetColumnList,
            sourceExpressList,
            condition,
            null,
            alias,
            orderBySqlNode,
            offset,
            limit,
            keywords,
            hints,
            hintContext), ec);
    }

    public static SqlDelete collectTableInfo(SqlDelete delete, ExecutionContext ec) {

        final List<SqlNode> sources = new ArrayList<>();
        final List<SqlNode> aliases = new ArrayList<>();
        final Map<SqlNode, List<SqlIdentifier>> subQueryTableMap = new HashMap<>();
        final SqlNode sourceTableNode =
            Optional.ofNullable(delete.getSourceTableNode()).orElse(delete.getTargetTable());

        boolean withAlias = collectSourceTable(sourceTableNode, sources, aliases, subQueryTableMap, false, ec);

        if (delete.getCondition() instanceof SqlCall) {

            final TableFinder tableFinder = new TableFinder(DefaultSchema.getSchemaName(), ec);
            delete.getCondition().accept(tableFinder);

            subQueryTableMap.putAll(tableFinder.getSubQueryTableMap());
        }

        if (delete.getOrderList() != null && delete.getOrderList().size() > 0) {
            final TableFinder tableFinder = new TableFinder(DefaultSchema.getSchemaName(), ec);
            delete.getOrderList().accept(tableFinder);

            subQueryTableMap.putAll(tableFinder.getSubQueryTableMap());
        }

        return delete.initTableInfo(new SqlNodeList(sources, SqlParserPos.ZERO),
            new SqlNodeList(aliases, SqlParserPos.ZERO),
            subQueryTableMap,
            withAlias);
    }

    private static class TableFinder extends ReplaceTableNameWithSomethingVisitor {
        private final Map<SqlNode, List<SqlIdentifier>> subQueryTableMap = new HashMap<>();
        private final Deque<SqlNode> subQueryStack = new ArrayDeque<>();

        protected TableFinder(String defaultSchemaName, ExecutionContext ec) {
            super(defaultSchemaName, ec);
        }

        @Override
        public SqlNode visit(SqlCall call) {

            if (call.isA(SqlKind.QUERY)) {
                subQueryStack.push(call);
                try {
                    return super.visit(call);
                } finally {
                    subQueryStack.pop();
                }
            }

            return super.visit(call);
        }

        @Override
        protected SqlNode buildSth(SqlNode sqlNode) {
            subQueryTableMap.computeIfAbsent(subQueryStack.peek(), k -> new ArrayList<>()).add((SqlIdentifier) sqlNode);
            return sqlNode;
        }

        public Map<SqlNode, List<SqlIdentifier>> getSubQueryTableMap() {
            return subQueryTableMap;
        }
    }

    public static SqlDelete constructDelete(SqlNodeList keywords, List<SqlNode> targetTables, SqlNode targetTable,
                                            SqlNode from,
                                            SqlNode using, SqlIdentifier alias, SqlNode condition,
                                            SqlNodeList orderBySqlNode, SqlNode offset,
                                            SqlNode limit, SqlNodeList hints, ExecutionContext ec) {
        return collectTableInfo(new SqlDelete(SqlParserPos.ZERO,
            targetTable,
            condition,
            null,
            alias,
            from,
            using,
            new SqlNodeList(targetTables, SqlParserPos.ZERO),
            orderBySqlNode,
            offset,
            limit,
            keywords,
            hints), ec);
    }

    private static class HintParser {
        static HintParser instance = new HintParser();
        private HashMap<String, String> convertMethodHint = new HashMap<>();

        private HintParser() {
            convertMethodHint.put("NODE_IN", "NODE");
            convertMethodHint.put("FORBID_EXECUTE_DML_ALL",
                "cmd_extra");///*+TDDL({'extra':{'ALLOW_TEMPORARY_TABLE'='TRUE','CHOOSE_INDEX'='TRUE'}})*/
            convertMethodHint.put("PARTITIONS", "NODE");
        }

        public TDDLHint.Function convertMethod(TDDLHint.Function function) {
            if (function == null) {
                return null;
            }
            String name = function.getName().toUpperCase();
            String newName = convertMethodHint.get(name);
            List<TDDLHint.Argument> arguments = function.getArguments();

            if (ConnectionParams.SUPPORTED_PARAMS.containsKey(name)) {
                TDDLHint.Function newFunction = new TDDLHint.Function("cmd_extra");
                for (int i = 0; i < arguments.size(); i++) {
                    TDDLHint.Argument newParam =
                        new TDDLHint.Argument(new MySqlCharExpr(name.toUpperCase()), arguments.get(i).getValue());
                    newFunction.getArguments().add(newParam);
                }
                return newFunction;
            } else if (convertMethodHint.containsKey(name)) {
                switch (name) {
                case "NODE_IN":
                    TDDLHint.Function newFunction = new TDDLHint.Function(newName);
                    for (int i = 0; i < arguments.size(); i++) {
                        newFunction.getArguments().add(arguments.get(i));
                    }
                    return newFunction;
                case "FORBID_EXECUTE_DML_ALL":
                    TDDLHint.Function newFunctionExtra = new TDDLHint.Function(newName);
                    for (int i = 0; i < arguments.size(); i++) {
                        newFunctionExtra.getArguments().add(arguments.get(i));
                    }
                    return newFunctionExtra;
                case "PARTITIONS":

                    break;
                }
            }
            return function;

        }

        public String escapeAlias(String alias) {
            if (alias == null || alias.length() == 0) {
                return alias;
            }
            int aliasLength = alias.length();
            String s = CalciteUtils.removeFirstAndLastChar(alias, '\'');
            if (s.length() == aliasLength) {
                alias = CalciteUtils.removeFirstAndLastChar(alias, '\"');
            }
            char[] chars = alias.toCharArray();
            StringBuilder result = new StringBuilder();
            if (alias.length() == 1) {
                return alias;
            }
            for (int i = 0; i < chars.length; ++i) {
                if (i + 1 == chars.length) {
                    result.append(chars[i]);
                    continue;
                }
                char curr = chars[i];
                char nextChar = chars[i + 1];
                switch (curr) {
                case '\\':
                    ++i;
                    result.append(nextChar);
                    break;
                case '\'':
                    ++i;
                    result.append(nextChar);
                    break;
                case '"':
                    ++i;
                    result.append(nextChar);
                    break;
                default:
                    result.append(curr);

                }
            }
            return result.toString();
        }
    }

    public static SqlNode convertPartitionBy(SQLPartitionBy partitionBy, ContextParameters context,
                                             ExecutionContext ec) {
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        partitionBy.accept(visitor);
        return visitor.getSqlNode();
    }
}

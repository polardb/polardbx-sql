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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.schema.RootSchemaFactory;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlJavaTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushModifyRule;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.TableInfoNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.externalize.RelDrdsJsonWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.sql.SqlKind.ASCENDING;
import static org.apache.calcite.sql.SqlKind.DESCENDING;
import static org.apache.calcite.sql.SqlKind.IS_NOT_NULL;
import static org.apache.calcite.sql.SqlKind.IS_NULL;

public class RelUtils {

    /**
     * 使用synchronized来解决并发问题
     */
    public static Double getRowCount(RelNode relNode) {
        RelMetadataQuery relMetadataQuery = relNode.getCluster().getMetadataQuery();
        synchronized (relMetadataQuery) {
            return relMetadataQuery.getRowCount(relNode);
        }
    }

    /**
     * 将SQLNode转换为对应的Sql语句
     */
    public static String toNativeSql(final SqlNode sqlNode, final DbType dbType) {
        return sqlNode.toSqlString(dbType.getDialect().getCalciteSqlDialect()).getSql();
    }

    /**
     * 将SQLNode转换为对应的Sql语句
     */
    public static BytesSql toNativeBytesSql(final SqlNode sqlNode, final DbType dbType) {
        if (sqlNode.isA(SqlKind.DDL)) {
            String sql = toNativeSql(sqlNode, dbType);
            return BytesSql.getBytesSql(sql);
        }
        return sqlNode.toBytesSql(dbType.getDialect().getCalciteSqlDialect(), true);
    }

    public static BytesSql toNativeBytesSql(final SqlNode sqlNode) {
        if (sqlNode.isA(SqlKind.DDL)) {
            String sql = toNativeSql(sqlNode);
            return BytesSql.getBytesSql(sql);
        }
        return sqlNode.toBytesSql(DbType.MYSQL.getDialect().getCalciteSqlDialect(), true);
    }

    public static String toNativeSql(final SqlNode sqlNode) {
        DbType dbType = DbType.MYSQL;
        return sqlNode.toSqlString(dbType.getDialect().getCalciteSqlDialect()).getSql();
    }

    public static String toNativeSqlLine(final SqlNode sqlNode) {
        DbType dbType = DbType.MYSQL;
        return TStringUtil.replace(sqlNode.toSqlString(dbType.getDialect().getCalciteSqlDialect()).getSql(),
            "\n",
            " ");
    }

    /**
     * 通过反射修改RelNode的rowType
     */
    public static void changeRowType(RelNode relNode, RelDataType relDataType) {
        try {
            Field rowTypeField = AbstractRelNode.class.getDeclaredField("rowType");
            rowTypeField.setAccessible(true);
            rowTypeField.set(relNode, relDataType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从数据类型 如 int(11) 中提取数据类型名称 int 全部转换为大写
     */
    public static String getDataType(String typeName) {
        String type = StringUtils.upperCase(typeName);
        if (StringUtils.indexOf(type, "(") != -1) {
            int index = StringUtils.indexOf(type, "(");
            type = StringUtils.substring(type, 0, index);
        }
        type = Util.replace(type, "UNSIGNED", "").trim();
        return type;
    }

    public static SqlNodeList toSqlNodeList(List<String> elements) {
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
        for (String element : elements) {
            nodeList.add(new SqlIdentifier(element, SqlParserPos.ZERO));
        }

        return nodeList;
    }

    public static String toString(SqlExplainLevel sqlExplainLevel, RelNode rel, Map<Integer, ParameterContext> params) {
        RelDrdsWriter relWriter = new RelDrdsWriter(sqlExplainLevel, params);
        rel.explainForDisplay(relWriter);
        return relWriter.asString();
    }

    public static String toString(RelNode rel, Map<Integer, ParameterContext> params,
                                  Function<RexNode, Object> funcEvaluator, ExecutionContext executionContext) {
        RelDrdsWriter relWriter =
            new RelDrdsWriter(null, executionContext.getSqlExplainLevel(), false, params, funcEvaluator, null,
                executionContext,
                executionContext.getCalcitePlanOptimizerTrace().orElse(null));
        rel.explainForDisplay(relWriter);
        return relWriter.asString();
    }

    /**
     * Get detail information from RelNode tree with extra info functions.
     */
    public static List<Object[]> toStringWithExtraInfo(RelNode rel, Map<Integer, ParameterContext> params,
                                                       Function<RexNode, Object> funcEvaluator,
                                                       ExecutionContext executionContext,
                                                       Function<RelNode, String> extraInfoBuilder) {
        RelDrdsWriter relWriter = new RelDrdsWriter(
            null, executionContext.getSqlExplainLevel(), false,
            params, funcEvaluator, extraInfoBuilder, executionContext);
        rel.explainForDisplay(relWriter);

        List<Object[]> planWithExtraInfo = new ArrayList<>();

        String planString = relWriter.asString();
        String[] planRows = StringUtils.split(planString, "\r\n");

        List<Object> extraInfos = relWriter.getExtraInfos();
        if (planRows.length != extraInfos.size()) {
            // in case the RelNode does not match the extra information.
            for (String planRow : planRows) {
                planWithExtraInfo.add(new Object[] {planRow, null});
            }
        } else {
            int extraInfoIndex = extraInfos.size() - 1;

            for (String planRow : StringUtils.split(planString, "\r\n")) {
                Object extraInfo = extraInfos.get(extraInfoIndex--);
                planWithExtraInfo.add(new Object[] {planRow, extraInfo});
            }
        }

        return planWithExtraInfo;
    }

    public static String toJsonString(RelNode rel, Map<Integer, ParameterContext> params,
                                      Function<RexNode, Object> funcEvaluator, ExecutionContext executionContext) {
        RelDrdsJsonWriter relWriter = new RelDrdsJsonWriter(executionContext.getSqlExplainLevel()
            , params, funcEvaluator, executionContext, executionContext.getCalcitePlanOptimizerTrace().orElse(null));
        rel.explainForDisplay(relWriter);
        return relWriter.asString();
    }

    public static String toString(RelNode plan) {
        return toString(CalcitePlanOptimizerTrace.DEFAULT_LEVEL, plan, null);
    }

    public static String toString(ExecutionPlan executionPlan) {
        return toString(CalcitePlanOptimizerTrace.DEFAULT_LEVEL, executionPlan.getPlan(), null);
    }

    public static String toString(ExecutionPlan executionPlan, Map<Integer, ParameterContext> params) {
        return toString(CalcitePlanOptimizerTrace.DEFAULT_LEVEL, executionPlan.getPlan(), params);
    }

    public static String stringValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            return ((SqlCharStringLiteral) sqlNode).getNlsString().getValue();

        }
        return sqlNode.toString();
    }

    public static Boolean booleanValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlCharStringLiteral || sqlNode instanceof SqlIdentifier) {
            Boolean r = BooleanUtils.toBoolean(stringValue(sqlNode));
            if (r == null) {
                return false;
            }
            return r;
        } else if (sqlNode instanceof SqlNumericLiteral) {
            int i = ((SqlNumericLiteral) sqlNode).intValue(false);
            if (i != 0 && i != 1) {
                throw new IllegalArgumentException("Variable 'tx_read_only' can't be set to the value of " + i);
            }
            return i == 1;
        }

        return (Boolean) ((SqlLiteral) sqlNode).getValue();
    }

    public static Integer integerValue(SqlLiteral sqlNode) {
        return sqlNode.intValue(false);
    }

    public static Long longValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlLiteral) {
            return ((SqlLiteral) sqlNode).longValue(false);
        }

        return Long.valueOf(sqlNode.toString());
    }

    public static List<String> stringListValue(SqlNode row) {
        List<String> result = new LinkedList<>();

        if (row.getKind() == SqlKind.ROW) {
            for (SqlNode operand : ((SqlBasicCall) row).getOperands()) {
                result.add(stringValue(operand));
            }
        } else if (row.getKind() == SqlKind.LITERAL) {
            result.add(stringValue(row));
        }

        return result;
    }

    public static Map<String, Set<String>> getCommonTopology(String schemaName, String tableName) {
        Map<String, Set<String>> topology = new HashMap<String, Set<String>>();
        Matrix matrix = OptimizerContext.getContext(schemaName).getMatrix();
        for (Group group : matrix.getGroups()) {
            Set<String> bcastTables = new HashSet<String>(1);
            bcastTables.add(tableName);
            topology.put(group.getName(), bcastTables);
        }
        return topology;
    }

    /**
     * 替换为可用的 Order By 列
     */
    public static SqlNode convertColumnName(SqlNode node, Map<String, String> projectMap) {
        /**
         * 首先,从 Project 中查找是否有别名
         */
        String aliasName = projectMap.get(node.toString());
        if (aliasName != null) {
            return new SqlIdentifier(aliasName, node.getParserPosition());
        }

        /**
         * 没有别名, 如果是 SqlIdentifier,则将其表名删除
         */
        if (node instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) node;
            return new SqlIdentifier(Util.last(identifier.names), node.getParserPosition());
        }

        /**
         * 如果是表达式,则将表达式的 String 作为 Order By 的列
         * <p>
         * 调用 SqlUtil.deriveAliasFromSqlNode 方法, Project
         * 中生成列名时同样调用该方法,这样可以确保Order By 的列名与 Project 输出的列名相同
         */
        if (node instanceof SqlCall) {
            SqlKind kind = node.getKind();
            SqlNodeList operands = new SqlNodeList(node.getParserPosition());
            SqlCall call = (SqlCall) node;
            if (ImmutableList.of(IS_NULL, IS_NOT_NULL, DESCENDING, ASCENDING).contains(kind)) {
                for (SqlNode op : call.getOperandList()) {
                    operands.add(convertColumnName(op, projectMap));
                }
                return ((SqlCall) node).getOperator().createCall(operands);
            } else {
                return new SqlIdentifier(SqlUtil.deriveAliasFromSqlNode(node), node.getParserPosition());
            }
        }

        return node;
    }

    public static RelDataType buildRowType(List<RelDataTypeField> fields, RelOptCluster cluster) {
        final List<RelDataType> dataTypes = new ArrayList<>();
        final List<String> fieldNames = fields.stream().map(s -> {
            dataTypes.add(s.getType());
            return s.getName();
        }).collect(Collectors.toList());

        return cluster.getTypeFactory().createStructType(dataTypes, fieldNames);
    }

    public static Map<String, Integer> getColumnIndexMap(RelOptTable table, RelNode parent, BitSet targetBitSet) {
        if (parent instanceof LogicalInsert) {
            final RelDataType srcRowType = ((LogicalInsert) parent).getInsertRowType();
            final Map<String, Integer> beforeColumnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            Ord.zip(srcRowType.getFieldNames()).stream().filter(o -> targetBitSet.get(o.i))
                .forEach(o -> beforeColumnIndexMap.put(o.e, o.i));
            return beforeColumnIndexMap;
        } else {
            return RelUtils.getTableColumnNames(table, RelUtils.getRelInput(parent), targetBitSet);
        }
    }

    /**
     * Mapping form column name to index in rowType of source rel
     *
     * @param table Target table
     * @param input Source rel
     * @param targetBitSet Which column of input rel belongs to target table(instead of source subquery)
     * @return Column index map
     */
    public static Map<String, Integer> getTableColumnNames(RelOptTable table, RelNode input, BitSet targetBitSet) {
        final Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (input instanceof LogicalDynamicValues) {
            final RelDataType srcRowType = input.getRowType();
            Ord.zip(srcRowType.getFieldNames()).stream().filter(o -> targetBitSet.get(o.i))
                .forEach(o -> columnIndexMap.put(o.e, o.i));
            return columnIndexMap;
        }

        final List<Set<RelColumnOrigin>> inputColumnNames = input.getCluster()
            .getMetadataQuery()
            .getColumnOriginNames(input);
        Ord.zip(inputColumnNames)
            .stream()
            .filter(ord -> targetBitSet.get(ord.i))
            .filter(ord -> ord.e.size() == 1)
            .filter(ord -> !ord.e.iterator().next().isDerived())
            .filter(ord -> sameTable(ord.e.iterator().next().getOriginTable(), table))
            .map(ord -> Pair.of(ord.e.iterator().next().getColumnName(), ord.i))
            .forEach(p -> columnIndexMap.putIfAbsent(p.left, p.right));
        return columnIndexMap;
    }

    private static boolean sameTable(RelOptTable currentTable, RelOptTable expectTable) {
        final Pair<String, String> current = RelUtils.getQualifiedTableName(currentTable);
        final Pair<String, String> expect = RelUtils.getQualifiedTableName(expectTable);

        return current.left.equalsIgnoreCase(expect.left) && current.right.equalsIgnoreCase(expect.right);
    }

    /**
     * <pre>
     * DELETE yy FROM xx AS yy FORCE INDEX(PRIMARY)
     * WHERE (pk0, pk1, ...) IN ((, ...), ...)
     * </pre>
     */
    public static SqlDelete buildDeleteWithInCondition(String targetTableName, List<String> columnNames,
                                                       String indexName, final AtomicInteger paramIndex,
                                                       ExecutionContext ec) {
        final SqlNode condition = buildInCondition(columnNames, paramIndex);

        final SqlIdentifier targetTableNode = new SqlIdentifier(targetTableName, SqlParserPos.ZERO);
        final SqlIdentifier targetAlias = new SqlIdentifier(targetTableName, SqlParserPos.ZERO);
        final SqlCall from = wrapWithAlias(targetTableNode, targetTableName, indexName);

        final SqlSelect deleteSourceSelect = new SqlSelect(SqlParserPos.ZERO,
            null,
            SqlNodeList.EMPTY,
            from,
            condition,
            null,
            null,
            null,
            null,
            null,
            null);

        return FastSqlConstructUtils.collectTableInfo(new SqlDelete(SqlParserPos.ZERO,
            targetTableNode,
            condition,
            deleteSourceSelect,
            targetAlias,
            from,
            null,
            new SqlNodeList(ImmutableList.of(targetAlias), SqlParserPos.ZERO),
            null,
            null,
            null,
            null), ec);
    }

    /**
     * <pre>
     * UPDATE ? AS xx FORCE INDEX(PRIMARY) SET xx.a = ?, ...
     * WHERE pk0 <=> ? AND pk1 <=> ? AND ...
     * </pre>
     */
    public static SqlUpdate buildUpdateWithAndCondition(String targetTableName, List<? extends SqlNode> targetColumns,
                                                        List<? extends SqlNode> sourceExpressions,
                                                        List<String> columnNames, SqlNodeList keywords,
                                                        String indexName,
                                                        AtomicInteger paramIndex, ExecutionContext ec) {
        final SqlNode condition = buildAndCondition(columnNames, paramIndex);

        final SqlNode targetTableNode = buildTargetNode();
        final SqlBasicCall tableReference = wrapWithAlias(targetTableNode, targetTableName, indexName);

        final SqlSelect sourceSelect = new SqlSelect(SqlParserPos.ZERO,
            null,
            SqlNodeList.EMPTY,
            tableReference,
            condition,
            null,
            null,
            null,
            null,
            null,
            null);

        return FastSqlConstructUtils.collectTableInfo(new SqlUpdate(SqlParserPos.ZERO,
            tableReference.operands[0],
            new SqlNodeList(targetColumns, SqlParserPos.ZERO),
            new SqlNodeList(sourceExpressions, SqlParserPos.ZERO),
            condition,
            sourceSelect,
            (SqlIdentifier) tableReference.operands[1],
            null,
            null,
            keywords,
            null), ec);
    }

    public static SqlUpdate buildUpdateWithAndNotDistinctCondition(String targetTableName,
                                                                   List<? extends SqlNode> targetColumns,
                                                                   List<? extends SqlNode> sourceExpressions,
                                                                   List<String> columnNames, SqlNodeList keywords,
                                                                   String indexName, AtomicInteger paramIndex,
                                                                   ExecutionContext ec) {
        final SqlNode condition = buildAndNotDistinctCondition(columnNames, paramIndex);

        final SqlNode targetTableNode = buildTargetNode();
        final SqlBasicCall tableReference = wrapWithAlias(targetTableNode, targetTableName, indexName);

        final SqlSelect sourceSelect = new SqlSelect(SqlParserPos.ZERO,
            null,
            SqlNodeList.EMPTY,
            tableReference,
            condition,
            null,
            null,
            null,
            null,
            null,
            null);

        return FastSqlConstructUtils.collectTableInfo(new SqlUpdate(SqlParserPos.ZERO,
            tableReference.operands[0],
            new SqlNodeList(targetColumns, SqlParserPos.ZERO),
            new SqlNodeList(sourceExpressions, SqlParserPos.ZERO),
            condition,
            sourceSelect,
            (SqlIdentifier) tableReference.operands[1],
            null,
            null,
            keywords,
            null), ec);
    }

    /**
     * Wrap table node with alias
     *
     * @param table table node
     * @param alias alias
     * @param indexName Local index name for FORCE INDEX
     * @return table node with alias
     */
    private static SqlBasicCall wrapWithAlias(SqlNode table, String alias, String indexName) {
        final SqlIdentifier aliasNode = new SqlIdentifier(alias, SqlParserPos.ZERO);
        final SqlBasicCall from = new SqlBasicCall(SqlStdOperatorTable.AS,
            new SqlNode[] {table, aliasNode},
            SqlParserPos.ZERO);

        if (TStringUtil.isNotBlank(indexName)) {
            aliasNode.indexNode = new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", SqlParserPos.ZERO),
                null,
                new SqlNodeList(ImmutableList.of(SqlLiteral.createCharString(indexName, SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                SqlParserPos.ZERO);
        }
        return from;
    }

    /**
     * Build IN condition like
     * <pre>
     * c IN ( ?, ...)
     * or
     * (c0, c1, ...) IN (( ?, ...), ...)
     * </pre>
     *
     * @param columns column names
     * @param paramIndex param index
     * @return IN condition
     */
    private static SqlNode buildInCondition(List<String> columns, AtomicInteger paramIndex) {
        // IN condition
        final SqlIdentifier[] columnNodes = new SqlIdentifier[columns.size()];

        SqlNode condition = null;
        if (columns.size() == 1) {
            final SqlNode[] paramNodes = new SqlNode[columns.size()];
            IntStream.range(0, columns.size()).forEach(i -> {
                columnNodes[i] = new SqlIdentifier(columns.get(i), SqlParserPos.ZERO);
                paramNodes[i] = new SqlDynamicParam(paramIndex.getAndIncrement(), SqlParserPos.ZERO);
            });
            final SqlNode paramRow = new SqlBasicCall(SqlStdOperatorTable.ROW, paramNodes, SqlParserPos.ZERO);

            condition = new SqlBasicCall(SqlStdOperatorTable.IN,
                new SqlNode[] {columnNodes[0], paramRow},
                SqlParserPos.ZERO);
        } else {
            final SqlNode columnRow = new SqlBasicCall(SqlStdOperatorTable.ROW, columnNodes, SqlParserPos.ZERO);

            final SqlNode[] paramNodes = new SqlNode[columns.size()];
            IntStream.range(0, columns.size()).forEach(i -> {
                columnNodes[i] = new SqlIdentifier(columns.get(i), SqlParserPos.ZERO);
                paramNodes[i] = new SqlDynamicParam(paramIndex.getAndIncrement(), SqlParserPos.ZERO);
            });

            final SqlNode[] paramRowArray = new SqlNode[1];
            paramRowArray[0] = new SqlBasicCall(SqlStdOperatorTable.ROW, paramNodes, SqlParserPos.ZERO);
            final SqlNode paramRow = new SqlBasicCall(SqlStdOperatorTable.ROW, paramRowArray, SqlParserPos.ZERO);

            condition = new SqlBasicCall(SqlStdOperatorTable.IN,
                new SqlNode[] {columnRow, paramRow},
                SqlParserPos.ZERO);
        }
        return condition;
    }

    public static SqlNode buildTargetNode() {
        return new SqlDynamicParam(PlannerUtils.TABLE_NAME_PARAM_INDEX, SqlParserPos.ZERO);
    }

    public static SqlNode buildAndCondition(List<String> columns, AtomicInteger paramIndex) {
        List<SqlNode> equalNodes = new ArrayList<>(columns.size());
        for (String primaryKeyName : columns) {
            SqlIdentifier sqlIdentifier = new SqlIdentifier(primaryKeyName, SqlParserPos.ZERO);
            SqlDynamicParam dynamicParam = new SqlDynamicParam(paramIndex.getAndIncrement(), SqlParserPos.ZERO);
            SqlNode equal = new SqlBasicCall(SqlStdOperatorTable.EQUALS,
                new SqlNode[] {sqlIdentifier, dynamicParam},
                SqlParserPos.ZERO);
            equalNodes.add(equal);
        }
        return buildAndTree(equalNodes);
    }

    public static SqlNode buildAndNotDistinctCondition(List<String> columns, AtomicInteger paramIndex) {
        List<SqlNode> equalNodes = new ArrayList<>(columns.size());
        for (String primaryKeyName : columns) {
            SqlIdentifier sqlIdentifier = new SqlIdentifier(primaryKeyName, SqlParserPos.ZERO);
            SqlDynamicParam dynamicParam = new SqlDynamicParam(paramIndex.getAndIncrement(), SqlParserPos.ZERO);
            SqlNode equal = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                new SqlNode[] {sqlIdentifier, dynamicParam},
                SqlParserPos.ZERO);
            equalNodes.add(equal);
        }
        return buildAndTree(equalNodes);
    }

    private static SqlNode buildAndTree(List<SqlNode> subClauses) {
        if (subClauses.size() == 0) {
            return null;
        }
        if (subClauses.size() == 1) {
            return subClauses.get(0);
        }
        if (subClauses.size() == 2) {
            return new SqlBasicCall(SqlStdOperatorTable.AND, subClauses.toArray(new SqlNode[2]), SqlParserPos.ZERO);
        }
        SqlNode subTree = buildAndTree(subClauses.subList(1, subClauses.size()));
        return new SqlBasicCall(SqlStdOperatorTable.AND,
            new SqlNode[] {subClauses.get(0), subTree},
            SqlParserPos.ZERO);
    }

    /**
     * Get BitSet that marks which column of input rel belongs to target table(instead of source subquery)
     *
     * @param srcInfos Source table info
     * @return BitSet
     */
    public static BitSet getTargetBitSet(List<TableInfoNode> srcInfos) {
        final AtomicInteger columnCount = new AtomicInteger(0);
        final BitSet targetBitSet = new BitSet();
        srcInfos.forEach(srcInfo -> {
            if (srcInfo.isTable()) {
                targetBitSet.set(columnCount.get(), columnCount.addAndGet(srcInfo.getColumnCount()));
            } else {
                columnCount.addAndGet(srcInfo.getColumnCount());
            }
        });
        return targetBitSet;
    }

    public static <R extends RelNode> R getRelInput(RelNode rel) {
        return getRelInput(rel, 0);
    }

    public static <R> R getRelInput(RelNode rel, int i) {
        RelNode input = rel.getInput(i);

        if (input instanceof HepRelVertex) {
            input = ((HepRelVertex) input).getCurrentRel();
        }

        return (R) input;
    }

    /**
     * 禁止全表删或者全表更新操作
     */
    public static void forbidDMLAllTableSql(SqlNode sqlNode) {
        try {
            if (sqlNode instanceof SqlDelete) {
                SqlDelete sqlDelete = (SqlDelete) sqlNode;
                if (sqlDelete.getCondition() == null && sqlDelete.getFetch() == null) {
                    if (sqlDelete.getFrom() instanceof SqlJoin
                        && ((SqlJoin) sqlDelete.getFrom()).getCondition() != null) {
                        return;
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_FORBID_EXECUTE_DML_ALL);
                }
            } else if (sqlNode instanceof SqlUpdate) {
                SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
                if (sqlUpdate.getCondition() == null && sqlUpdate.getFetch() == null) {
                    SqlSelect sqlSelect = sqlUpdate.getSourceSelect();
                    if (sqlSelect != null) {
                        if (sqlSelect.getWhere() != null || sqlSelect.getFetch() != null || (
                            sqlSelect.getFrom() instanceof SqlJoin
                                && ((SqlJoin) sqlSelect.getFrom()).getCondition() != null)) {
                            return;
                        }
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_FORBID_EXECUTE_DML_ALL);
                }
            } else if (sqlNode instanceof SqlNodeList) {
                List<SqlNode> sqlNodes = ((SqlNodeList) sqlNode).getList();
                if (sqlNodes != null) {
                    for (SqlNode subNode : sqlNodes) {
                        forbidDMLAllTableSql(subNode);
                    }
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    static class CacheFinder extends RelVisitor {

        List<RelNode> cacheNodes = Lists.newArrayList();

        public CacheFinder() {
            super();
        }

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof Correlate) {
                visit(((Correlate) node).getLeft(), 0, node);
                visit(((Correlate) node).getRight(), 0, node);
            } else if (node instanceof LogicalView) {
                ((LogicalView) node).getScalarList().stream().forEach(scar -> go(scar.getRel()));
            } else {
                super.visit(node, ordinal, parent);
            }

            // dont cache nodes inside of LogicalView
            if (parent instanceof LogicalView) {
                return;
            }

            // step 1: check if subnode could be cached
            if (parent != null && couldInputBeCached(node)) {
                Set<CorrelationId> tempSet = Sets.newHashSet();
                parent.collectVariablesUsed(tempSet);
                if (tempSet.size() > 0) {
                    cacheNodes.add(node);
                }
            }
        }

        private boolean couldInputBeCached(RelNode node) {
            if (RelOptUtil.getVariablesSet(node).isEmpty()) {
                return true;
            } else {
                return false;
            }
        }

        List<RelNode> findCacheNodes(RelNode node) {
            go(node);
            return cacheNodes;
        }
    }

    public static List<RelNode> findCacheNodes(RelNode node) {
        return new CacheFinder().findCacheNodes(node);
    }

    /**
     * Are all tables are single and in the group specified by singleDbIndex
     */
    public static boolean allTableInOneGroup(Map<String, TableProperties> tablePropertiesMap) {
        String currentDb = null;

        for (Map.Entry<String, TableProperties> entry : tablePropertiesMap.entrySet()) {
            final TableProperties tableProperties = entry.getValue();

            if (tableProperties == null) {
                continue;
            }

            if (null == currentDb) {
                currentDb = tableProperties.getSingleDbIndex();
            } else if (!currentDb.equalsIgnoreCase(tableProperties.getSingleDbIndex())) {
                return false;
            }
        } // end of for

        return currentDb != null;
    }

    /**
     * Are all tables are broadcast table
     */
    public static boolean allTableBroadcast(Map<String, TableProperties> tablePropertiesMap) {
        for (Map.Entry<String, TableProperties> entry : tablePropertiesMap.entrySet()) {
            if (!entry.getValue().isBroadcast()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Are all tables are not broadcast table
     */
    public static boolean allTableNotBroadcast(Map<String, TableProperties> tablePropertiesMap) {
        for (Map.Entry<String, TableProperties> entry : tablePropertiesMap.entrySet()) {
            if (entry.getValue().isBroadcast()) {
                return false;
            }
        }

        return true;
    }

    public static boolean containsBroadcastTable(Map<String, TableProperties> tablePropertiesMap,
                                                 List<String> tableNames) {
        boolean result = false;

        for (String tableName : tableNames) {
            TableProperties tableProperties = tablePropertiesMap.get(tableName);

            if (tableProperties == null) {
                continue;
            }

            result |= tableProperties.isBroadcast();

            if (result) {
                break;
            }
        }

        return result;
    }

    //for the logical table which is in scaleout writable phase, we could not push down it directly
    public static boolean containScaleOutWriableTable(Map<String, TableProperties> tablePropertiesMap,
                                                      List<String> tableNames, ExecutionContext ec) {
        boolean result = false;

        for (String tableName : tableNames) {
            TableProperties tableProperties = tablePropertiesMap.get(tableName);
            if (tableProperties == null) {
                continue;
            }

            TableMeta tableMeta = ec.getSchemaManager(tableProperties.getSchemaName()).getTable(tableName);
            if (ComplexTaskPlanUtils.canWrite(tableMeta)) {
                result = true;
                break;
            }
        }

        return result;
    }

    public static boolean containsReplicateWriableTable(Map<String, TableProperties> tablePropertiesMap,
                                                        List<String> tableNames, ExecutionContext ec) {
        boolean result = false;

        for (String tableName : tableNames) {
            TableProperties tableProperties = tablePropertiesMap.get(tableName);
            if (tableProperties == null) {
                continue;
            }
            final OptimizerContext oc = OptimizerContext.getContext(tableProperties.getSchemaName());
            TableMeta tableMeta = oc.getLatestSchemaManager().getTable(tableName);
            if (ComplexTaskPlanUtils.canDelete(tableMeta)) {
                result = true;
                break;
            }
        }

        return result;
    }

    public static boolean containOnlineModifyColumnTable(Map<String, TableProperties> tablePropertiesMap,
                                                         List<String> tableNames, ExecutionContext ec) {
        for (String tableName : tableNames) {
            TableProperties tableProperties = tablePropertiesMap.get(tableName);
            if (tableProperties == null) {
                continue;
            }
            if (TableColumnUtils.isModifying(tableProperties.getSchemaName(), tableName, ec)) {
                return true;
            }
        }

        return false;
    }

    public static boolean containsGsiTable(Map<String, TableProperties> tablePropertiesMap, List<String> tableNames) {
        boolean result = false;

        for (String tableName : tableNames) {
            TableProperties tableProperties = tablePropertiesMap.get(tableName);
            if (tableProperties == null) {
                continue;
            }
            result |= tableProperties.hasGsi();

            if (result) {
                break;
            }
        }

        return result;
    }

    public static Map<String, TableProperties> buildTablePropertiesMap(List<String> tableNames, String schemaName,
                                                                       ExecutionContext ec) {
        String schema = schemaName != null ? schemaName : DefaultSchema.getSchemaName();
        final TddlRuleManager or = OptimizerContext.getContext(schema).getRuleManager();
        final Map<String, TableProperties> result = new HashMap<>();
        for (String table : tableNames) {
            TableMeta meta = ec.getSchemaManager(schema).getTableWithNull(table);
            if (meta != null) {
                result.put(table, new TableProperties(table, or.getTddlRule(), schema, ec));
            }
        }
        return result;
    }

    public static String lastStringValue(SqlNode sqlNode) {
        String tableName = null;
        if (sqlNode instanceof SqlIdentifier) {
            tableName = Util.last(((SqlIdentifier) sqlNode).names);
        } else {
            tableName = stringValue(sqlNode);
        }
        return tableName;
    }

    public static boolean informationSchema(SqlNode tableName) {
        boolean result = false;
        if (tableName instanceof SqlIdentifier) {
            ImmutableList<String> names = ((SqlIdentifier) tableName).names;
            if (names.size() > 1 && TStringUtil.equalsIgnoreCase("information_schema", names.get(names.size() - 2))) {
                result = true;
            }
        }

        return result;
    }

    public static boolean performanceSchema(SqlNode tableName) {
        boolean result = false;
        if (tableName instanceof SqlIdentifier) {
            ImmutableList<String> names = ((SqlIdentifier) tableName).names;
            if (names.size() > 1 && TStringUtil.equalsIgnoreCase("performance_schema", names.get(names.size() - 2))) {
                result = true;
            }
        }

        return result;
    }

    public static boolean mysqlSchema(SqlNode tableName) {
        boolean result = false;
        if (tableName instanceof SqlIdentifier) {
            ImmutableList<String> names = ((SqlIdentifier) tableName).names;
            if (names.size() > 1 && TStringUtil.equalsIgnoreCase("mysql", names.get(names.size() - 2))) {
                result = true;
            }
        }

        return result;
    }

    public static CalciteCatalogReader buildCatalogReader(String schema, ExecutionContext ec) {
        final Config parserConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).setParserFactory(
            SqlParserImpl.FACTORY).build();

        final Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));
        final CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
        String schemaName = schema != null ? schema : DefaultSchema.getSchemaName();
        final CalciteSchema calciteSchema = RootSchemaFactory.createRootSchema(schemaName, ec);
        return new CalciteCatalogReader(calciteSchema,
            calciteSchema.path(schemaName),
            new TddlJavaTypeFactoryImpl(),
            connectionConfig);
    }

    public static Pair<String, String> getQualifiedTableName(RelOptTable table) {
        final List<String> qualifiedName = table.getQualifiedName();
        final String schema = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;
        final String tableName = Util.last(qualifiedName);

        return Pair.of(schema, tableName);
    }

    public static String getSchemaName(RelOptTable table) {
        return getQualifiedTableName(table).left;
    }

    /**
     * build mapping from the fields of project's input to fields of project's output
     *
     * @param inputFieldCount input field count
     * @param projects projects
     */
    public static List<Integer> inverseMap(int inputFieldCount, List<RexNode> projects) {
        final List<Integer> inverseMap = new ArrayList<>(inputFieldCount);
        IntStream.range(0, inputFieldCount).forEach(i -> inverseMap.add(-1));
        for (Ord<? extends RexNode> exp : Ord.zip(projects)) {
            if (exp.e instanceof RexInputRef) {
                inverseMap.set(((RexInputRef) exp.e).getIndex(), exp.i);
            }
        }
        return inverseMap;
    }

    public static class JoinFinder extends RelShuttleImpl {

        private Map<Integer, Integer> inputRef = Maps.newHashMap();
        private List<Pair<Integer, Integer>> pairList = Lists.newArrayList();

        public JoinFinder(int l) {
            for (int i = 0; i < l; i++) {
                inputRef.put(i, i);
            }
        }

        @Override
        public RelNode visit(LogicalProject project) {
            List<RexNode> ps = project.getProjects();
            for (int i = 0; i < ps.size(); i++) {
                RexNode r = ps.get(i);
                if (r instanceof RexInputRef) {
                    inputRef.put(((RexInputRef) r).getIndex(), i);
                }
            }
            return super.visit(project);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            RexNode condition = join.getCondition();
            ConditionFinder conditionFinder = new ConditionFinder(getPairList(), inputRef);
            condition.accept(conditionFinder);
            return super.visit(join);
        }

        @Override
        public RelNode visit(LogicalTableLookup tableLookup) {
            return super.visit(tableLookup.getProject());
        }

        public List<Pair<Integer, Integer>> getPairList() {
            return pairList;
        }
    }

    static class ConditionFinder extends RexShuttle {

        private List<Pair<Integer, Integer>> pairList;
        private Map<Integer, Integer> inputRef;

        public ConditionFinder(List<Pair<Integer, Integer>> pairList, Map<Integer, Integer> inputRef) {
            super();
            this.pairList = pairList;
            this.inputRef = inputRef;
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
                List<RexNode> rexNodes = call.getOperands();
                assert rexNodes.size() == 2;
                RexNode l = rexNodes.get(0);
                RexNode r = rexNodes.get(1);

                if (l instanceof RexInputRef && r instanceof RexInputRef) {
                    if (inputRef == null) {
                        getPairList().add(new Pair<>(((RexInputRef) l).getIndex(), ((RexInputRef) r).getIndex()));
                    } else {
                        getPairList().add(new Pair<>(inputRef.get(((RexInputRef) l).getIndex()),
                            inputRef.get(((RexInputRef) r).getIndex())));
                    }

                }
            }
            return super.visitCall(call);
        }

        public List<Pair<Integer, Integer>> getPairList() {
            return pairList;
        }
    }

    public static List<Integer> getSortOrderByIndex(Sort sort) {
        List<Integer> index = new ArrayList<>();
        for (RexNode r : sort.getChildExps()) {
            if (!(r instanceof RexInputRef)) {
                return new ArrayList<>();
            }

            index.add(((RexInputRef) r).getIndex());
        }

        return index;
    }

    public static class TableProperties {

        private String tableName;
        private String schemaName;
        private TableRule tableRule;
        private TddlRule tddlRule;
        private PartitionInfo partInfo;
        private Engine engine = Engine.INNODB;
        private ExecutionContext ec;

        public TableProperties(String tableName, TddlRule tddlRule, String schemaName,
                               ExecutionContext ec) {
            this.tableName = tableName;
            this.schemaName = schemaName;
            this.tddlRule = tddlRule;
            this.tableRule = tddlRule.getTable(tableName);
            boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
            if (isNewPartDb) {
                this.partInfo =
                    ec.getSchemaManager(schemaName).getTddlRuleManager().getPartitionInfoManager()
                        .getPartitionInfo(tableName);
            }
            this.engine = ec.getSchemaManager(schemaName).getTable(tableName).getEngine();
            this.ec = ec;
        }

        public TableProperties(String tableName, String schemaName, ExecutionContext ec) {
            this.tableName = tableName;
            this.schemaName = schemaName;
            this.ec = ec;
        }

        public Engine getEngine() {
            return engine;
        }

        public boolean isBroadcast() {

            if (partInfo != null) {
                return partInfo.isBroadcastTable();
            }

            if (null == tableRule) {
                return false;
            }
            return tableRule.isBroadcast();
        }

        /**
         * If table has more than one db index return null
         */
        public String getSingleDbIndex() {

            if (partInfo != null) {
                if (partInfo.isBroadcastTable()) {
                    // for broadcast, return default group index
                    return partInfo.defaultDbIndex();
                } else {
                    // for partitioned(or gsi) table
                    // check if has only one partitions
                    if (!partInfo.isSingleTable()) {
                        // it has multi- partitions
                        return null;
                    }
                    // for single table / partitioned(or gsi) table with only one partitions
                    // return the group index of its first partitions
                    return partInfo.getPartitionBy().getPartitions().get(0).getLocation().getGroupKey();
                }
            }

            if (isBroadcast()) {
                return tddlRule.getDefaultDbIndex();
            }

            String dbIndex = null;
            if (tableRule != null) {
                if (tableRule.getActualTopology().size() == 1) {
                    for (Map.Entry<String, Set<String>> dbEntry : tableRule.getActualTopology().entrySet()) {
                        if (dbEntry.getValue().size() > 1) { // multi tables
                            return null;
                        }
                        dbIndex = dbEntry.getKey();
                    }
                } else { // multi groups
                    return null;
                }
            } else { // single table
                dbIndex = tddlRule.getDefaultDbIndex(tableName);
            }

            return dbIndex;
        }

        public boolean hasGsi() {
            boolean needCheckGsi =
                OptimizerContext.getContext(schemaName).getRuleManager().needCheckIfExistsGsi(tableName);
            if (!needCheckGsi) {
                return false;
            } else {
                return GlobalIndexMeta.hasIndex(tableName, schemaName, ec);
            }

        }

        public String getTableName() {
            return tableName;
        }

        public String getSchemaName() {
            return schemaName;
        }
    }

    public static LogicalView createLogicalView(TableScan tableScan, SqlSelect.LockMode lockMode) {
        return createLogicalView(tableScan, lockMode, Engine.INNODB);
    }

    public static LogicalView createLogicalView(TableScan tableScan, SqlSelect.LockMode lockMode, Engine engine) {
        if (Engine.isFileStore(engine)) {
            return new OSSTableScan(tableScan, lockMode);
        } else {
            return new LogicalView(tableScan, lockMode);
        }
    }

    public static LogicalTableLookup createTableLookup(LogicalView primary, RelNode index, RelOptTable indexTable) {
        final RelDataType primaryRowType = primary.getRowType();
        final RelDataType indexRowType = index.getRowType();

        final String primaryTableName = primary.getLogicalTableName();

        final TableMeta primaryTable =
            PlannerContext.getPlannerContext(primary).getExecutionContext().getSchemaManager(primary.getSchemaName())
                .getTable(primaryTableName);
        final List<String> pkList = Optional.ofNullable(primaryTable.getPrimaryKey())
            .map(cmList -> cmList.stream().map(ColumnMeta::getName).map(x -> x.toLowerCase())
                .collect(Collectors.toList()))
            .orElse(ImmutableList.of());

        final List<String> notNullableSkList = OptimizerContext.getContext(primary.getSchemaName())
            .getRuleManager()
            .getSharedColumns(primaryTableName)
            .stream()
            .map(x -> x.toLowerCase())
            .filter(sk -> !pkList.contains(sk))
            .filter(sk -> !primaryTable.getColumn(sk).isNullable())
            .collect(Collectors.toList());

        final List<String> nullableSkList = OptimizerContext.getContext(primary.getSchemaName())
            .getRuleManager()
            .getSharedColumns(primaryTableName)
            .stream()
            .map(x -> x.toLowerCase())
            .filter(sk -> !pkList.contains(sk))
            .filter(sk -> primaryTable.getColumn(sk).isNullable())
            .collect(Collectors.toList());

        final int leftCount = indexRowType.getFieldCount();
        final Map<String, Integer> primaryColumnRefMap = primaryRowType.getFieldList()
            .stream()
            .collect(Collectors.toMap(RelDataTypeField::getName,
                RelDataTypeField::getIndex,
                (x, y) -> y,
                TreeMaps::caseInsensitiveMap));
        final Map<String, Integer> indexColumnRefMap = indexRowType.getFieldList()
            .stream()
            .collect(Collectors.toMap(RelDataTypeField::getName,
                RelDataTypeField::getIndex,
                (x, y) -> y,
                TreeMaps::caseInsensitiveMap));

        final RexBuilder rexBuilder = primary.getCluster().getRexBuilder();

        final RexNode[] condition = {null};
        buildEqCondition(pkList,
            indexRowType,
            primaryRowType,
            leftCount,
            indexColumnRefMap,
            primaryColumnRefMap,
            rexBuilder,
            condition,
            SqlStdOperatorTable.EQUALS);

        buildEqCondition(notNullableSkList,
            indexRowType,
            primaryRowType,
            leftCount,
            indexColumnRefMap,
            primaryColumnRefMap,
            rexBuilder,
            condition,
            SqlStdOperatorTable.EQUALS);

        buildEqCondition(nullableSkList,
            indexRowType,
            primaryRowType,
            leftCount,
            indexColumnRefMap,
            primaryColumnRefMap,
            rexBuilder,
            condition,
            SqlStdOperatorTable.IS_NOT_DISTINCT_FROM);

        final RelDataType rowType = PlannerUtils.deriveJoinType(primary.getCluster().getTypeFactory(),
            indexRowType,
            primaryRowType);

        final List<RexNode> projects = new ArrayList<>();
        primaryRowType.getFieldNames().forEach(cn -> {
            final Integer ref = indexColumnRefMap.getOrDefault(cn, leftCount + primaryColumnRefMap.get(cn));
            final RelDataType type = rowType.getFieldList().get(ref).getType();
            projects.add(rexBuilder.makeInputRef(type, ref));
        });

        return new LogicalTableLookup(primary.getCluster(),
            primary.getCluster().traitSetOf(Convention.NONE),
            index,
            primary,
            indexTable,
            primary.getTable(),
            primary.getCluster().getPlanner().emptyTraitSet(),
            JoinRelType.INNER,
            condition[0],
            projects,
            primary.getRowType(),
            false,
            primary.getHints());
    }

    public static void buildEqCondition(List<String> columnNames, RelDataType leftRowType, RelDataType rightRowType,
                                        int rightOffset, Map<String, Integer> leftColumnRefMap,
                                        Map<String, Integer> rightColumnRefMap, RexBuilder rexBuilder,
                                        RexNode[] condition, SqlOperator op) {
        if (GeneralUtil.isEmpty(columnNames)) {
            return;
        }

        columnNames.forEach(cn -> {
            final Integer indexPk = leftColumnRefMap.get(cn);
            final Integer primaryPk = rightColumnRefMap.get(cn);
            final RelDataTypeField leftType = leftRowType.getFieldList().get(indexPk);
            final RelDataTypeField rightType = rightRowType.getFieldList().get(primaryPk);

            final RexNode pkEq = rexBuilder.makeCall(op,
                rexBuilder.makeInputRef(leftType.getType(), indexPk),
                rexBuilder.makeInputRef(rightType.getType(), rightOffset + primaryPk));

            if (condition[0] == null) {
                condition[0] = pkEq;
            } else {
                condition[0] = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition[0], pkEq);
            }
        });
    }

    public static boolean isSimplePlan(RelNode plan, AtomicInteger complexNodeCnt) {
        if (plan instanceof Aggregate || plan instanceof Join) {
            //record the Count for the plan that contain the agg or join.
            int complexCnt = complexNodeCnt.incrementAndGet();
            if (complexCnt >= 2) {
                return false;
            }
        }
        if (plan instanceof LogicalProject) {
            return isSimplePlan(plan.getInput(0), complexNodeCnt);
        } else if (plan instanceof LogicalFilter) {
            return isSimplePlan(plan.getInput(0), complexNodeCnt);
        } else if (plan instanceof Sort) {
            if (plan instanceof Limit) {
                return isSimplePlan(plan.getInput(0), complexNodeCnt);
            } else {
                RelNode inputRel = ((Sort) plan).getInput();
                if (inputRel instanceof LogicalShow || inputRel instanceof LogicalView
                    || inputRel instanceof LogicalValues || plan instanceof PhyTableOperation
                    || plan instanceof SingleTableOperation || plan instanceof VirtualView) {
                    return true;
                } else {
                    return false;
                }
            }
        } else if (plan instanceof LogicalAggregate) {
            LogicalAggregate agg = (LogicalAggregate) plan;
            ImmutableBitSet groupIndex = agg.getGroupSet();
            if (groupIndex.cardinality() == 0) {
                //select min(pk), max(pk) from update_delete_base_autonic_multi_db_multi_tb ;
                boolean isDistinct = false;
                if (agg.getAggCallList() != null) {
                    isDistinct = agg.getAggCallList().stream().anyMatch(aggregateCall -> aggregateCall.isDistinct());
                }
                if (!isDistinct) {
                    RelNode inputRel = plan.getInput(0);
                    return isSimplePlan(inputRel, complexNodeCnt);
                }
            }
            return false;
        } else if (plan instanceof LogicalValues) {
            return true;
        } else if (plan instanceof LogicalView || plan instanceof PhyTableOperation
            || plan instanceof PhyQueryOperation || plan instanceof VirtualView) {
            return true;
        } else if (plan instanceof DirectTableOperation) {
            return true;
        } else if (plan instanceof SingleTableOperation) {
            return true;
        } else if (plan instanceof BaseDalOperation) {
            return true;
        } else if (plan instanceof DDL) {
            return true;
        } else if (plan instanceof PhyDdlTableOperation) {
            return true;
        } else if (plan instanceof LogicalModify) {
            return true;
        } else if (plan instanceof BroadcastTableModify) {
            return true;
        }
        return false;
    }

    /**
     * 构建or条件
     */
    public static Comparative or(Comparative parent, Comparative target) {
        if (parent == null) {
            ComparativeOR or = new ComparativeOR();
            or.addComparative(target);
            return or;
        } else {
            if (parent instanceof ComparativeOR) {
                ((ComparativeOR) parent).addComparative(target);
                return parent;
            } else {
                ComparativeOR or = new ComparativeOR();
                or.addComparative(parent);
                or.addComparative(target);
                return or;
            }
        }
    }

    /**
     * 构建and条件
     */
    public static Comparative and(Comparative parent, Comparative target) {
        if (parent == null) {
            ComparativeAND and = new ComparativeAND();
            and.addComparative(target);
            return and;
        } else {
            if (parent instanceof ComparativeAND) {

                ComparativeAND and = ((ComparativeAND) parent);
                if (and.getList().size() == 1) {
                    and.addComparative(target);
                    return and;
                } else {
                    ComparativeAND andNew = new ComparativeAND();
                    andNew.addComparative(and);
                    andNew.addComparative(target);
                    return andNew;
                }

            } else {
                ComparativeAND and = new ComparativeAND();
                and.addComparative(parent);
                and.addComparative(target);
                return and;
            }
        }
    }

    static class WindowFinder extends RelShuttleImpl {
        boolean hasWindow = false;

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof Window) {
                hasWindow = true;
            }
            return visitChildren(other);
        }
    }

    public static boolean hasWindow(RelNode rel) {
        WindowFinder windowFinder = new WindowFinder();
        rel.accept(windowFinder);
        return windowFinder.hasWindow;
    }

    public static <T extends RelNode> T removeHepRelVertex(RelNode vertex) {
        RelNode rel = vertex instanceof HepRelVertex ? ((HepRelVertex) vertex).getCurrentRel() : vertex;

        // Recursively process children, replacing this rel's inputs
        // with corresponding child rels.
        List<RelNode> inputs = rel.getInputs();
        for (int i = 0; i < inputs.size(); ++i) {
            RelNode child = inputs.get(i);
            if (!(child instanceof HepRelVertex)) {
                // Already replaced.
                continue;
            }
            child = removeHepRelVertex(child);
            rel.replaceInput(i, child);
            rel.recomputeDigest();
        }

        return (T) rel;
    }

    public static boolean isSimpleMergeSortPlan(RelNode plan) {
        return (plan instanceof MergeSort && ((MergeSort) plan).getInput() instanceof LogicalView);
    }

    public static boolean isAllSingleTableInSameSchema(Set<RelOptTable> scans) {
        Set<String> schemas = Sets.newHashSet();
        for (RelOptTable scan : scans) {
            final List<String> qualifiedName = scan.getQualifiedName();
            final String tableName = Util.last(qualifiedName);

            final String schemaName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;
            TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();

            schemas.add(schemaName);
            if (!or.isTableInSingleDb(tableName) && !or.isBroadCast(tableName)) {
                return false;
            }
        }
        if (schemas.size() > 1) {
            return false;
        }
        return true;
    }

    public static boolean existUnPushableLastInsertId(RelNode node) {
        class CheckUnPushableRelVisitor extends RelVisitor {
            private boolean exists = false;
            private final PlannerContext context = PlannerContext.getPlannerContext(node);

            @Override
            public void visit(RelNode relNode, int ordinal, RelNode parent) {

                if (relNode instanceof Project) {
                    exists = isNotPushLastInsertId(context, (Project) relNode) || exists;
                } else if (relNode instanceof Filter) {
                    exists = isNotPushLastInsertId(context, (Filter) relNode) || exists;
                }

                super.visit(relNode, ordinal, parent);
            }

            public boolean exists() {
                return exists;
            }
        }

        final CheckUnPushableRelVisitor checkUnPushableRelVisitor = new CheckUnPushableRelVisitor();
        checkUnPushableRelVisitor.go(node);
        return checkUnPushableRelVisitor.exists();
    }

    // select 中 last_insert_id 不可下推
    // 不可下推的 DML 中，last_insert_id 不可下推 (带有GSI，Scale-out，拆分键变更（DDL），修改分区键，广播表)
    public static boolean isNotPushLastInsertId(PlannerContext context, Project project) {
        return isNotPushLastInertIdExps(context, project.getChildExps());
    }

    public static boolean isNotPushLastInsertId(PlannerContext context, Filter filter) {
        return isNotPushLastInertIdExps(context, filter.getChildExps());
    }

    private static boolean isNotPushLastInertIdExps(PlannerContext context, List<RexNode> childExps) {
        final boolean isModifyShardingColumn = context.getExecutionContext().isModifyShardingColumn();
        final boolean modifyGsiTable = context.getExecutionContext().isModifyGsiTable();
        final boolean modifyScaleoutTable = context.getExecutionContext().isScaleoutWritableTable();
        final boolean modifyOnlineColumnTable = context.getExecutionContext().isModifyOnlineColumnTable();
        final boolean modifyBroadcastTable = context.getExecutionContext().isModifyBroadcastTable();

        boolean operands = false;
        boolean isLastInsertId = false;
        for (RexNode node : childExps) {
            if (node instanceof RexCall) {
                operands |= !verifyOperands((RexCall) node);
                isLastInsertId |= node.toString().contains("LAST_INSERT_ID");
            }
        }
        /**
         * Non push down dml rule by
         * {@link PushModifyRule#matches}.
         */
        final boolean notPushLastInsertId = operands && isLastInsertId &&
            (isModifyShardingColumn ||
                modifyGsiTable ||
                modifyScaleoutTable ||
                modifyBroadcastTable ||
                (context.getSqlKind() == SqlKind.UPDATE && modifyOnlineColumnTable) ||
                context.getSqlKind() == SqlKind.SELECT
            );
        return notPushLastInsertId;
    }

    public static boolean verifyOperands(final RexCall call) {
        for (RexNode node : call.operands) {
            if (node instanceof RexCall) {
                if (!verifyOperands((RexCall) node)) {
                    return false;
                }
            } else if (node instanceof RexDynamicParam) {
            } else if (!(node instanceof RexLiteral)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSimpleQueryPlan(RelNode plan) {
        if (plan instanceof LogicalProject) {
            return isSimpleQueryPlan(plan.getInput(0));
        } else if (plan instanceof LogicalFilter) {
            return isSimpleQueryPlan(plan.getInput(0));
        } else if (plan instanceof Gather) {
            return isSimpleQueryPlan(plan.getInput(0));
        } else if (plan instanceof Sort) {
            if (plan instanceof Limit) {
                return isSimpleQueryPlan(plan.getInput(0));
            } else if (plan instanceof MergeSort) {
                return isSimpleQueryPlan(plan.getInput(0));
            }
        } else if (plan instanceof LogicalValues) {
            return true;
        } else if (plan instanceof DynamicValues) {
            return true;
        } else if (plan instanceof LogicalView || plan instanceof BaseQueryOperation ||
            plan instanceof VirtualView) {
            if (plan instanceof OSSTableScan) {
                return false;
            }
            return true;
        } else if (plan instanceof BaseDalOperation) {
            return true;
        } else if (plan instanceof DDL) {
            return true;
        } else if (plan instanceof LogicalModify) {
            return true;
        } else if (plan instanceof BroadcastTableModify) {
            return true;
        }
        return false;
    }

    public static boolean isCursorSimplePlan(RelNode plan) {
        /*
         * Special Treatment for following plans
         * - LogicalView
         * - Gather <- LogicalView
         * - Merge <- LogicalView
         * To make it work properly, remember to call `executeByCursor` instead of `execute` in Gather/Merge cursors
         */
        return plan instanceof LogicalView ||
            plan instanceof VirtualView ||
            plan instanceof DDL ||
            plan instanceof LogicalOutFile ||
            plan instanceof TableModify ||
            plan instanceof BaseDalOperation ||
            plan instanceof BroadcastTableModify ||
            plan instanceof Gather && ((Gather) plan).getInput() instanceof LogicalView ||
            plan instanceof Gather && ((Gather) plan).getInput() instanceof BaseQueryOperation ||
            plan instanceof BaseQueryOperation; // Maybe produced by PostPlanner
    }

    public static void disableMpp(ExecutionContext ec) {
        //force close mpp.
        ec.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, false);
    }

    public static RelNode replaceAccessField(RelNode rel, RexCorrelVariable rexCorrelVariable,
                                             Map mapping) {
        return rel.accept(new ReplaceFieldAccessRelShuttle(rexCorrelVariable, mapping));
    }

    public static class ReplaceFieldAccessRelShuttle extends RelShuttleImpl {

        private final RexCorrelVariable rexCorrelVariable;
        private final Map<Integer, Integer> mapping;

        ReplaceFieldAccessRelShuttle(RexCorrelVariable rexCorrelVariable, Map<Integer, Integer> mapping) {
            this.mapping = mapping;
            this.rexCorrelVariable = rexCorrelVariable;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            ReplaceFieldAccessRexShuttle replaceFieldAccessRexShuttle =
                new ReplaceFieldAccessRexShuttle(rexCorrelVariable, mapping);
            RexNode r = filter.getCondition().accept(replaceFieldAccessRexShuttle);
            return LogicalFilter.create(filter.getInput().accept(this),
                r,
                (ImmutableSet<CorrelationId>) filter.getVariablesSet());
        }

        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            ReplaceFieldAccessRexShuttle replaceFieldAccessRexShuttle =
                new ReplaceFieldAccessRexShuttle(rexCorrelVariable, mapping);
            List<RexNode> newLeftConditions =
                correlate.getLeftConditions().stream().map(rexNode -> rexNode.accept(replaceFieldAccessRexShuttle))
                    .collect(Collectors.toList());
            return LogicalCorrelate.create(correlate.getLeft().accept(this), correlate.getRight().accept(this),
                correlate.getCorrelationId(), correlate.getRequiredColumns(), newLeftConditions, correlate.getOpKind(),
                correlate.getJoinType());
        }

        @Override
        public RelNode visit(LogicalProject project) {
            ReplaceFieldAccessRexShuttle replaceFieldAccessRexShuttle =
                new ReplaceFieldAccessRexShuttle(rexCorrelVariable, mapping);
            List<RexNode> list = Lists.newArrayList();
            for (RexNode r : project.getProjects()) {
                list.add(r.accept(replaceFieldAccessRexShuttle));
            }
            return project.copy(project.getTraitSet(), project.getInput().accept(this), list, null);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            ReplaceFieldAccessRexShuttle replaceFieldAccessRexShuttle =
                new ReplaceFieldAccessRexShuttle(rexCorrelVariable, mapping);
            return join.copy(join.getTraitSet(), join.getCondition().accept(replaceFieldAccessRexShuttle),
                join.getInput(0).accept(this), join.getInput(1).accept(this), join.getJoinType(),
                join.isSemiJoinDone());
        }

        public RelNode handle(LogicalSemiJoin join) {
            ReplaceFieldAccessRexShuttle replaceFieldAccessRexShuttle =
                new ReplaceFieldAccessRexShuttle(rexCorrelVariable, mapping);
            return join.copy(join.getTraitSet(), join.getCondition().accept(replaceFieldAccessRexShuttle),
                join.getOperands().stream().map(rexNode -> rexNode.accept(replaceFieldAccessRexShuttle))
                    .collect(Collectors.toList()),
                join.getInput(0).accept(this), join.getInput(1).accept(this), join.getJoinType(),
                join.isSemiJoinDone());
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof LogicalSemiJoin) {
                return handle((LogicalSemiJoin) other);
            }
            return super.visit(other);
        }

        @Override
        public RelNode visit(TableScan scan) {
            if (scan instanceof LogicalView) {
                RelNode newRel = ((LogicalView) scan).getPushedRelNode().accept(this);
                return ((LogicalView) scan).copy(scan.getTraitSet(), newRel);
            }
            return scan;
        }
    }

    public static class ReplaceFieldAccessRexShuttle extends RexShuttle {

        private final RexCorrelVariable rexCorrelVariable;
        private final Map<Integer, Integer> mapping;

        ReplaceFieldAccessRexShuttle(RexCorrelVariable rexCorrelVariable, Map<Integer, Integer> mapping) {
            this.mapping = mapping;
            this.rexCorrelVariable = rexCorrelVariable;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexCorrelVariable v =
                (RexCorrelVariable) fieldAccess.getReferenceExpr();
            if (v.getId().equals(rexCorrelVariable.getId())) {
                int newIndex = mapping.get(fieldAccess.getField().getIndex());
                RexFieldAccess newRexField =
                    new RexFieldAccess(rexCorrelVariable, rexCorrelVariable.getType().getFieldList().get(newIndex));
                return newRexField;
            }
            return fieldAccess;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            RelNode rel2 = subQuery.rel.accept(new ReplaceFieldAccessRelShuttle(rexCorrelVariable, mapping));
            return subQuery.clone(rel2);
        }
    }

    public static boolean dmlWithDerivedSubquery(final RelNode plan, final SqlNode ast) {
        if (plan instanceof LogicalModifyView) {
            switch (ast.getKind()) {
            case UPDATE:
                return ((SqlUpdate) ast).withSubquery();
            case DELETE:
                return ((SqlDelete) ast).withSubquery();
            default:
                return false;
            }
        }
        return false;
    }

    public static TableMeta getTableMeta(TableScan tableScan) {
        if (null != tableScan.getTable()
            && tableScan.getTable() instanceof RelOptTableImpl
            && null != ((RelOptTableImpl) tableScan.getTable()).getImplTable()
            && ((RelOptTableImpl) tableScan.getTable()).getImplTable() instanceof TableMeta) {
            return (TableMeta) ((RelOptTableImpl) tableScan.getTable()).getImplTable();
        }
        return null;
    }

    public static GsiMetaManager.GsiIndexMetaBean getGsiIndexMetaBean(LogicalIndexScan indexScan) {
        TableMeta tableMeta = RelUtils.getTableMeta(indexScan);
        if (null != tableMeta && null != tableMeta.getGsiTableMetaBean()) {
            return tableMeta.getGsiTableMetaBean().gsiMetaBean;
        }
        return null;
    }

    public static TableMeta getTableMeta(SqlIdentifier tableIdentifier, ExecutionContext ec) {
        String schemaName = ec.getSchemaName();
        if (tableIdentifier.names.size() == 2) {
            schemaName = tableIdentifier.names.get(0);
        }
        TableMeta tableMeta = null;
        try {
            tableMeta = ec.getSchemaManager(schemaName).getTable(tableIdentifier.getLastName());
        } catch (Throwable ignored) {
            // Ignore NPE.
        }
        return tableMeta;
    }

    /**
     * Whether we can optimize min/max(col) by adding FORCE INDEX (PRIMARY).
     * Return TRUE if the column is not the first column of any index.
     */
    public static boolean canOptMinMax(TableMeta tableMeta, String columnName) {
        if (null == tableMeta) {
            return false;
        }
        final ColumnMeta columnMeta = tableMeta.getColumn(columnName);
        if (null == columnMeta) {
            return false;
        }

        // Check if this column is the first column of any index.
        boolean firstColumnOfIndex = false;
        for (final IndexMeta indexMeta : tableMeta.getIndexes()) {
            if (indexMeta.getKeyColumns().size() == 0) {
                return false;
            }
            // Compare address of object.
            if (indexMeta.getKeyColumns().get(0) == columnMeta) {
                firstColumnOfIndex = true;
                break;
            }
        }

        // Add force index if this column is not the first column of any index.
        return !firstColumnOfIndex;
    }
}

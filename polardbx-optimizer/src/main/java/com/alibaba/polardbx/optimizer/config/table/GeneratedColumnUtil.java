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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class GeneratedColumnUtil {

    private static final Set<String> SUPPORTED_TYPE_NAME = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    final public static int MAX_COLUMN_NAME_LENGTH = 64;

    static {
        SUPPORTED_TYPE_NAME.add("DATETIME");
        SUPPORTED_TYPE_NAME.add("DATE");
        SUPPORTED_TYPE_NAME.add("TIMESTAMP");
        SUPPORTED_TYPE_NAME.add("BIGINT");
        SUPPORTED_TYPE_NAME.add("INT");
        SUPPORTED_TYPE_NAME.add("INTEGER");
        SUPPORTED_TYPE_NAME.add("MEDIUMINT");
        SUPPORTED_TYPE_NAME.add("SMALLINT");
        SUPPORTED_TYPE_NAME.add("TINYINT");
        SUPPORTED_TYPE_NAME.add("CHAR");
        SUPPORTED_TYPE_NAME.add("VARCHAR");
    }

    public static void validateGeneratedColumnExpr(SqlCall sqlCall) {
        ValidateIdentifierShuttle idShuttle = new ValidateIdentifierShuttle();
        idShuttle.visit(sqlCall);
        ValidateDynamicFunctionShuttle funcShuttle = new ValidateDynamicFunctionShuttle();
        funcShuttle.visit(sqlCall);
    }

    public static Set<String> getReferencedColumns(String expr) {
        SqlCall sqlCall = getSqlCallFromExpr(expr);
        return getReferencedColumns(sqlCall);
    }

    public static Set<String> getReferencedColumns(SqlCall sqlCall) {
        Set<String> referencedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        GetReferencedColumnsShuttle shuttle = new GetReferencedColumnsShuttle(referencedColumns);
        shuttle.visit(sqlCall);
        return referencedColumns;
    }

    public static boolean containUnpushableFunction(SqlCall sqlCall) {
        ValidatePushableFunctionShuttle shuttle = new ValidatePushableFunctionShuttle();
        shuttle.visit(sqlCall);
        return !shuttle.pushable;
    }

    public static SqlCall getSqlCallFromExpr(String expr) {
        SQLExpr sqlExpr = new MySqlExprParser(ByteString.from(expr)).expr();
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(null, null);
        sqlExpr.accept(visitor);
        return new SqlBasicCall(SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC, new SqlNode[] {visitor.getSqlNode()},
            SqlParserPos.ZERO);
    }

    public static SqlCall getSqlCallAndValidateFromExprWithoutTableName(String schemaName, String tableName,
                                                                        String expr, ExecutionContext ec) {
        SqlCall sqlCall = getSqlCallAndValidateFromExprWithTableName(schemaName, tableName, expr, ec);
        RemoveTableNameFromColumnShuttle shuttle = new RemoveTableNameFromColumnShuttle();
        sqlCall.accept(shuttle);
        return sqlCall;
    }

    public static SqlCall getSqlCallAndValidateFromExprWithTableName(String schemaName, String tableName, String expr,
                                                                     ExecutionContext ec) {
        SQLExpr sqlExpr = new MySqlExprParser(ByteString.from(expr)).expr();
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(null, null);
        sqlExpr.accept(visitor);
        SqlCall sqlCall =
            new SqlBasicCall(SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC, new SqlNode[] {visitor.getSqlNode()},
                SqlParserPos.ZERO);
        SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, ec);
        sqlCall = sqlConverter.getValidatedSqlCallForGeneratedColumn(tableName, sqlCall);
        return sqlCall;
    }

    public static Map<String, Set<String>> getAllLogicalReferencedColumnsByGen(TableMeta tableMeta) {
        Map<String, Set<String>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMeta.getPhysicalColumns().stream().filter(ColumnMeta::isLogicalGeneratedColumn)
            .forEach(cm -> result.computeIfAbsent(cm.getName(), k -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                .addAll(getReferencedColumns(cm.getField().getDefault())));
        return result;
    }

    public static Map<String, List<String>> getAllLogicalReferencedColumnByRef(TableMeta tableMeta) {
        Map<String, List<String>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMeta.getPhysicalColumns().stream().filter(ColumnMeta::isLogicalGeneratedColumn).forEach(
            cm -> {
                Set<String> referencedColumns = getReferencedColumns(cm.getField().getDefault());
                for (String column : referencedColumns) {
                    result.computeIfAbsent(column, k -> new ArrayList<>()).add(cm.getName());
                }
            }
        );
        return result;
    }

    public static Map<String, List<String>> getAllReferencedColumnByRef(TableMeta tableMeta) {
        Map<String, List<String>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMeta.getPhysicalColumns().stream().filter(ColumnMeta::isGeneratedColumn).forEach(
            cm -> {
                Set<String> referencedColumns = getReferencedColumns(cm.getField().getDefault());
                for (String column : referencedColumns) {
                    result.computeIfAbsent(column, k -> new ArrayList<>()).add(cm.getName());
                }
            }
        );
        return result;
    }

    public static boolean containLogicalGeneratedColumn(RelOptTable primary, ExecutionContext ec) {
        final Pair<String, String> schemaTable = RelUtils.getQualifiedTableName(primary);
        return containLogicalGeneratedColumn(schemaTable.left, schemaTable.right, ec);
    }

    public static boolean containLogicalGeneratedColumn(String schemaName, String tableName, ExecutionContext ec) {
        SchemaManager sm;
        if (ec != null) {
            sm = ec.getSchemaManager(schemaName);
        } else {
            sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        }
        final TableMeta table = sm.getTable(tableName);
        return table.hasLogicalGeneratedColumn();
    }

    private static class GetReferencedColumnsShuttle extends SqlShuttle {
        final private Set<String> referencedColumns;

        public GetReferencedColumnsShuttle(Set<String> referencedColumns) {
            this.referencedColumns = referencedColumns;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            referencedColumns.add(id.getLastName());
            return super.visit(id);
        }
    }

    private static class AddTableNameToColumnShuttle extends SqlShuttle {
        final private String tableName;

        public AddTableNameToColumnShuttle(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            id.setNames(ImmutableList.of(tableName, id.getSimple()), null);
            return super.visit(id);
        }
    }

    private static class ValidateIdentifierShuttle extends SqlShuttle {
        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (!id.isSimple()) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] can not contain table name.", id));
            }
            return super.visit(id);
        }
    }

    private static class ValidateDynamicFunctionShuttle extends SqlShuttle {
        @Override
        public SqlNode visit(SqlCall call) {
            final SqlOperator op = call.getOperator();
            if (op.isDynamicFunction()) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Expression can not contain dynamic function [%s].", op.getName()));
            }
            return super.visit(call);
        }
    }

    private static class ValidatePushableFunctionShuttle extends SqlShuttle {
        public boolean pushable = true;

        @Override
        public SqlNode visit(SqlCall call) {
            final SqlOperator op = call.getOperator();
            if (!op.canPushDown()) {
                pushable = false;
            }
            return super.visit(call);
        }
    }

    public static List<List<String>> getGeneratedColumnEvaluationOrder(TableMeta tableMeta) {
        return getGeneratedColumnEvaluationOrder(getAllLogicalReferencedColumnsByGen(tableMeta));
    }

    public static List<List<String>> getGeneratedColumnEvaluationOrder(Map<String, Set<String>> genColRefs) {
        TopologicalSort sort = new TopologicalSort();
        sort.addVertices(genColRefs.keySet());
        for (Map.Entry<String, Set<String>> entry : genColRefs.entrySet()) {
            for (String refCol : entry.getValue()) {
                sort.addEdge(entry.getKey(), refCol);
            }
        }

        List<List<String>> order = sort.sort();
        Collections.reverse(order);
        return order;
    }

    public static List<String> getModifiedGeneratedColumn(TableMeta tableMeta, Collection<String> modifiedColumns) {
        Map<String, List<String>> refColMap = getAllLogicalReferencedColumnByRef(tableMeta);
        Set<String> modifiedGenColSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String column : modifiedColumns) {
            if (refColMap.containsKey(column)) {
                modifiedGenColSet.addAll(refColMap.get(column));
            }
        }

        // Generated column could be referenced by other generated column
        int addedCnt;
        do {
            addedCnt = modifiedGenColSet.size();
            List<String> modifiedGenCols = new ArrayList<>();
            for (String col : modifiedGenColSet) {
                if (refColMap.containsKey(col)) {
                    modifiedGenCols.addAll(refColMap.get(col));
                }
            }
            modifiedGenColSet.addAll(modifiedGenCols);
        } while (addedCnt != modifiedGenColSet.size());

        List<String> modifiedGenColList = new ArrayList<>(modifiedGenColSet.size());
        List<List<String>> generatedColumnOrder = getGeneratedColumnEvaluationOrder(tableMeta);
        generatedColumnOrder.stream().flatMap(Collection::stream).forEach(
            col -> {
                if (modifiedGenColSet.contains(col)) {
                    modifiedGenColList.add(col);
                }
            }
        );
        return modifiedGenColList;
    }

    public static boolean supportDataType(ColumnMeta cm) {
        try {
            PartitionFieldBuilder.createField(cm.getDataType());
        } catch (Throwable ex) {
            return false;
        }
        return true;
    }

    public static boolean supportDataType(SqlColumnDeclaration sqlDef) {
        try {
            return SUPPORTED_TYPE_NAME.contains(sqlDef.getDataType().getTypeName().getLastName());
        } catch (Throwable ex) {
            return false;
        }
    }

    public static class TopologicalSort {
        private final Map<String, Vertex> vertexMap = new HashMap<>();

        public TopologicalSort() {
        }

        public void addVertices(Collection<String> vertexNames) {
            for (String vertexName : vertexNames) {
                vertexMap.put(vertexName.toUpperCase(), new Vertex(vertexName));
            }
        }

        public void addEdge(String outVertexName, String inVertexName) {
            Vertex outVertex = vertexMap.get(outVertexName.toUpperCase());
            Vertex inVertex = vertexMap.get(inVertexName.toUpperCase());
            if (outVertex == null || inVertex == null) {
                return;
            }
            inVertex.inDegree++;
            outVertex.adjVertices.add(inVertex);
        }

        public List<List<String>> sort() {
            List<Vertex> allVertices = new ArrayList<>(vertexMap.values());
            List<List<String>> result = new ArrayList<>();

            while (!allVertices.isEmpty()) {
                Set<Vertex> toRemove = new HashSet<>();
                for (Vertex vertex : allVertices) {
                    if (vertex.inDegree == 0) {
                        toRemove.add(vertex);
                    }
                }

                if (toRemove.isEmpty()) {
                    // There must be a cycle, something is wrong
                    String columns =
                        allVertices.stream().map(v -> String.format("[%s]", v.name)).collect(Collectors.joining(","));
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Find dependency cycle in generated column %s.", columns));
                }

                for (Vertex vertex : toRemove) {
                    for (Vertex adjVertex : vertex.adjVertices) {
                        adjVertex.inDegree--;
                    }
                }
                allVertices.removeIf(toRemove::contains);
                result.add(toRemove.stream().map(v -> v.name).collect(Collectors.toList()));
            }
            return result;
        }

        private static class Vertex {
            String name;
            int inDegree;
            List<Vertex> adjVertices;

            public Vertex(String name) {
                this.name = name;
                this.inDegree = 0;
                this.adjVertices = new ArrayList<>();
            }

            @Override
            public int hashCode() {
                return name.toUpperCase().hashCode();
            }

            @Override
            public boolean equals(Object object) {
                return this == object || (object instanceof Vertex && ((Vertex) object).name.equalsIgnoreCase(
                    this.name));
            }

        }
    }

    public static class RemoveTableNameFromColumnShuttle extends SqlShuttle {
        @Override
        public SqlNode visit(SqlIdentifier id) {
            id.setNames(ImmutableList.of(id.getLastName()), null);
            return super.visit(id);
        }
    }

    public static void buildGeneratedColumnInfoForInsert(LogicalInsert insert, ExecutionContext ec,
                                                         TableMeta tableMeta) {
        if (!tableMeta.hasLogicalGeneratedColumn()) {
            return;
        }

        Set<String> referencedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<String> generatedColumns =
            GeneratedColumnUtil.getGeneratedColumnEvaluationOrder(tableMeta).stream().flatMap(Collection::stream)
                .collect(Collectors.toList());
        GeneratedColumnUtil.getAllLogicalReferencedColumnsByGen(tableMeta).values().forEach(referencedColumns::addAll);
        generatedColumns.forEach(referencedColumns::remove);

        // Build row to eval generated columns
        List<String> evalFieldNames = new ArrayList<>();
        evalFieldNames.addAll(referencedColumns);
        evalFieldNames.addAll(generatedColumns);

        List<RelDataType> evalFieldTypes = new ArrayList<>();
        List<ColumnMeta> evalColumnMetas = new ArrayList<>();
        for (String column : evalFieldNames) {
            ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(column);
            evalColumnMetas.add(columnMeta);
            evalFieldTypes.add(columnMeta.getField().getRelType());
        }
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType rowType = typeFactory.createStructType(evalFieldTypes, evalFieldNames);

        RelDataType inputRowType =
            insert.isSourceSelect() ? insert.getInsertRowType() : RelUtils.getRelInput(insert).getRowType();
        List<String> inputFieldNames = inputRowType.getFieldNames();

        // From inputFieldNames to evalFieldNames
        List<Integer> inputToEvalFieldsMapping = new ArrayList<>();
        Map<String, Integer> inputFieldIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < inputFieldNames.size(); i++) {
            inputFieldIndexMap.put(inputFieldNames.get(i), i);
        }

        for (int i = 0; i < evalFieldNames.size(); i++) {
            inputToEvalFieldsMapping.add(inputFieldIndexMap.get(evalFieldNames.get(i)));
        }

        List<SqlCall> sourceNodes = new ArrayList<>();
        for (String generatedColumn : generatedColumns) {
            sourceNodes.add(GeneratedColumnUtil.getSqlCallAndValidateFromExprWithoutTableName(tableMeta.getSchemaName(),
                tableMeta.getTableName(), tableMeta.getColumn(generatedColumn).getField().getDefault(),
                ec));
        }
        SqlConverter sqlConverter = SqlConverter.getInstance(tableMeta.getSchemaName(), ec);
        RelOptCluster cluster = sqlConverter.createRelOptCluster();
        PlannerContext plannerContext = PlannerContext.getPlannerContext(cluster);
        List<RexNode> rexNodes = sqlConverter.getRexForGeneratedColumn(rowType, sourceNodes, plannerContext);

        insert.setEvalRowColMetas(evalColumnMetas);
        insert.setGenColRexNodes(rexNodes);
        insert.setInputToEvalFieldsMapping(inputToEvalFieldsMapping);
    }

    public static void buildGeneratedColumnInfoForModify(TableModify modify, ExecutionContext ec) {
        RelNode input = modify.getInput();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        Set<Integer> targetTableIndexSet = new HashSet<>(modify.getTargetTableIndexes());
        Map<Integer, List<ColumnMeta>> evalRowColumnMetas = new HashMap<>();
        Map<Integer, List<Integer>> inputToEvalFieldMappings = new HashMap<>();
        Map<Integer, List<RexNode>> genColRexNodes = new HashMap<>();

        for (Integer tableIndex : targetTableIndexSet) {
            final RelOptTable table = modify.getTableInfo().getSrcInfos().get(tableIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final TableMeta tableMeta = ec.getSchemaManager(qn.left).getTable(qn.right);

            if (!tableMeta.hasLogicalGeneratedColumn()) {
                continue;
            }

            // 1. Get all generated column, should already in evaluation order
            List<String> generatedColumns = new ArrayList<>();
            for (int i = 0; i < modify.getTargetTableIndexes().size(); i++) {
                if (modify.getTargetTableIndexes().get(i).equals(tableIndex)) {
                    String column = modify.getUpdateColumnList().get(i);
                    if (tableMeta.getColumn(column).isLogicalGeneratedColumn()) {
                        generatedColumns.add(column);
                    }
                }
            }

            // 2. Get all referenced columns
            Set<String> referencedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (String column : generatedColumns) {
                referencedColumns.addAll(
                    GeneratedColumnUtil.getReferencedColumns(tableMeta.getColumn(column).getField().getDefault()));
            }
            generatedColumns.forEach(referencedColumns::remove);

            // 3. Next we build row struct
            List<String> evalFieldNames = new ArrayList<>();
            evalFieldNames.addAll(referencedColumns);
            evalFieldNames.addAll(generatedColumns);

            List<RelDataType> evalFieldTypes = new ArrayList<>();
            List<ColumnMeta> evalColumnMetas = new ArrayList<>();
            for (String column : evalFieldNames) {
                ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(column);
                evalColumnMetas.add(columnMeta);
                evalFieldTypes.add(columnMeta.getField().getRelType());
            }
            evalRowColumnMetas.put(tableIndex, evalColumnMetas);

            Map<String, Integer> columnMap = modify.getSourceColumnIndexMap().get(tableIndex);
            int offset = input.getRowType().getFieldCount() - modify.getUpdateColumnList().size();

            List<Integer> inputToEvalMapping = new ArrayList<>();
            // Before update
            for (int i = 0; i < evalFieldNames.size(); i++) {
                if (columnMap.get(evalFieldNames.get(i)) != null) {
                    inputToEvalMapping.add(columnMap.get(evalFieldNames.get(i)));
                } else {
                    // generated column in WRITE_ONLY
                    inputToEvalMapping.add(-1);
                }
            }

            // After update
            for (int i = 0; i < evalFieldNames.size(); i++) {
                for (int j = 0; j < modify.getTargetTableIndexes().size(); j++) {
                    if (modify.getTargetTableIndexes().get(j).equals(tableIndex) && modify.getUpdateColumnList()
                        .get(j).equalsIgnoreCase(evalFieldNames.get(i))) {
                        inputToEvalMapping.set(i, offset + j);
                        break;
                    }
                }
            }

            inputToEvalFieldMappings.put(tableIndex, inputToEvalMapping);

            List<SqlCall> sourceNodes = new ArrayList<>();
            for (String generatedColumn : generatedColumns) {
                sourceNodes.add(GeneratedColumnUtil.getSqlCallAndValidateFromExprWithoutTableName(qn.left, qn.right,
                    tableMeta.getColumn(generatedColumn).getField().getDefault(), ec));
            }

            SqlConverter sqlConverter = SqlConverter.getInstance(qn.left, ec);
            RelOptCluster cluster = sqlConverter.createRelOptCluster();
            PlannerContext plannerContext = PlannerContext.getPlannerContext(cluster);
            RelDataType rowType = typeFactory.createStructType(evalFieldTypes, evalFieldNames);
            List<RexNode> rexNodes = sqlConverter.getRexForGeneratedColumn(rowType, sourceNodes, plannerContext);
            genColRexNodes.put(tableIndex, rexNodes);
        }

        if (!evalRowColumnMetas.isEmpty()) {
            if (modify instanceof LogicalModify) {
                ((LogicalModify) modify).setEvalRowColumnMetas(evalRowColumnMetas);
                ((LogicalModify) modify).setInputToEvalFieldMappings(inputToEvalFieldMappings);
                ((LogicalModify) modify).setGenColRexNodes(genColRexNodes);
            } else {
                ((LogicalRelocate) modify).setEvalRowColumnMetas(evalRowColumnMetas);
                ((LogicalRelocate) modify).setInputToEvalFieldMappings(inputToEvalFieldMappings);
                ((LogicalRelocate) modify).setGenColRexNodes(genColRexNodes);
            }
        }
    }

    public static boolean hasCompatibleType(TableMeta tableMeta, String generatedColumn, ExecutionContext ec) {
        ColumnMeta generatedColumnMeta = tableMeta.getColumnIgnoreCase(generatedColumn);
        String expr = generatedColumnMeta.getField().getDefault();

        RelDataType type = getRexNodeFromExpr(tableMeta, expr, ec).getType();
        RelDataType fieldType = generatedColumnMeta.getField().getRelType();
        return fieldType.equalsSansFieldNames(type);
    }

    public static String getGenColExprReturnType(TableMeta tableMeta, String expr, ExecutionContext ec) {
        RelDataType type = getRexNodeFromExpr(tableMeta, expr, ec).getType();
        SqlDataTypeSpec sqlDataTypeSpec = SqlTypeUtil.convertTypeToSpec(type);
        if (sqlDataTypeSpec.getTypeName().getLastName().equalsIgnoreCase("VARCHAR")) {
            // max length of single key is 3072B
            sqlDataTypeSpec = new SqlDataTypeSpec(
                sqlDataTypeSpec.getTypeName(),
                sqlDataTypeSpec.isUnsigned(),
                sqlDataTypeSpec.getPrecision() != -1 ? sqlDataTypeSpec.getPrecision() : 768,
                -1,
                null,
                null,
                SqlParserPos.ZERO);
        }
        return sqlDataTypeSpec.toString();
    }

    private static RexNode getRexNodeFromExpr(TableMeta tableMeta, String expr, ExecutionContext ec) {
        List<String> evalFieldNames = new ArrayList<>(getReferencedColumns(expr));

        List<RelDataType> evalFieldTypes = new ArrayList<>();
        for (String column : evalFieldNames) {
            ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(column);
            evalFieldTypes.add(columnMeta.getField().getRelType());
        }

        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType rowType = typeFactory.createStructType(evalFieldTypes, evalFieldNames);

        List<SqlCall> sourceNodes = new ArrayList<>();
        sourceNodes.add(GeneratedColumnUtil.getSqlCallAndValidateFromExprWithoutTableName(tableMeta.getSchemaName(),
            tableMeta.getTableName(), expr, ec));
        SqlConverter sqlConverter = SqlConverter.getInstance(tableMeta.getSchemaName(), ec);
        RelOptCluster cluster = sqlConverter.createRelOptCluster();
        PlannerContext plannerContext = PlannerContext.getPlannerContext(cluster);
        List<RexNode> rexNodes = sqlConverter.getRexForGeneratedColumn(rowType, sourceNodes, plannerContext);

        return rexNodes.get(0);
    }

    public static boolean isExpression(SQLSelectOrderByItem column, Set<String> tableColumns) {
        // see com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils.constructIndexColumnNames
        SQLExpr expr = column.getExpr();
        if (expr instanceof SQLIdentifierExpr) {
            return false;
        } else if (expr instanceof SQLMethodInvokeExpr) {
            if (((SQLMethodInvokeExpr) expr).getArguments().size() == 1) {
                String colName = SQLUtils.normalizeNoTrim(((SQLMethodInvokeExpr) expr).getMethodName());
                return !tableColumns.contains(colName);
            } else {
                return true;
            }
        }
        return true;
    }

    public static boolean isGeneratedColumn(SQLSelectOrderByItem column, Set<String> tableColumns,
                                            TableMeta tableMeta) {
        // see com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils.constructIndexColumnNames
        SQLExpr expr = column.getExpr();
        if (expr instanceof SQLIdentifierExpr) {
            String colName = SQLUtils.normalizeNoTrim(((SQLIdentifierExpr) expr).getSimpleName());
            if (tableColumns.contains(colName)) {
                return tableMeta.getColumnIgnoreCase(colName).isGeneratedColumn();
            }
        } else if (expr instanceof SQLMethodInvokeExpr) {
            if (((SQLMethodInvokeExpr) expr).getArguments().size() == 1) {
                String colName = SQLUtils.normalizeNoTrim(((SQLMethodInvokeExpr) expr).getMethodName());
                if (tableColumns.contains(colName)) {
                    return tableMeta.getColumnIgnoreCase(colName).isGeneratedColumn();
                }
            }
        }
        return false;
    }

    public static String getExprIndexGeneratedColumnName(String indexName, int index) {
        // idx_name$no
        String suffix = "$" + index;
        if (indexName.length() + suffix.length() >= MAX_COLUMN_NAME_LENGTH) {
            indexName = indexName.substring(0, MAX_COLUMN_NAME_LENGTH - suffix.length());
        }
        return SqlIdentifier.surroundWithBacktick(indexName + suffix);
    }

    public static List<SQLAlterTableItem> rewriteExprIndex(TableMeta tableMeta,
                                                           SQLIndexDefinition sqlIndexDefinition,
                                                           ExecutionContext executionContext) {
        String indexName = SQLUtils.normalizeNoTrim(sqlIndexDefinition.getName().getSimpleName());

        List<SQLAlterTableItem> resultAlterTableItems = new ArrayList<>();

        SQLIndexDefinition newSqlIndexDefinition = new SQLIndexDefinition();
        sqlIndexDefinition.cloneTo(newSqlIndexDefinition);
        newSqlIndexDefinition.setColumns(new ArrayList<>());

        Set<String> tableColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tableColumns.addAll(
            tableMeta.getPhysicalColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList()));

        for (int i = 0; i < sqlIndexDefinition.getColumns().size(); i++) {
            SQLSelectOrderByItem column = sqlIndexDefinition.getColumns().get(i);
            SQLSelectOrderByItem sqlSelectOrderByItem;
            if (isExpression(column, tableColumns)) {
                String newColName = getExprIndexGeneratedColumnName(indexName, i);

                // create add column stmt
                SQLAlterTableAddColumn sqlAlterTableAddColumn = new SQLAlterTableAddColumn();
                SQLColumnDefinition sqlColumnDefinition = new SQLColumnDefinition();
                sqlColumnDefinition.setName(newColName);
                String type = GeneratedColumnUtil.getGenColExprReturnType(tableMeta, column.getExpr().toString(),
                    executionContext);
                MySqlExprParser parser = new MySqlExprParser(ByteString.from(type));
                sqlColumnDefinition.setDataType(parser.parseDataType());

                SQLExpr expr = column.getExpr().clone();

                sqlColumnDefinition.setGeneratedAlawsAs(expr);
                sqlAlterTableAddColumn.addColumn(sqlColumnDefinition);
                resultAlterTableItems.add(sqlAlterTableAddColumn);

                // add new column name to index list
                sqlSelectOrderByItem = column.clone();
                SQLIdentifierExpr id = new SQLIdentifierExpr(newColName);
                sqlSelectOrderByItem.setExpr(id);
            } else {
                sqlSelectOrderByItem = column.clone();
            }
            sqlSelectOrderByItem.setParent(newSqlIndexDefinition);
            newSqlIndexDefinition.getColumns().add(sqlSelectOrderByItem);
        }
        // Add index item
        SQLAlterTableAddIndex sqlAlterTableAddIndex = new SQLAlterTableAddIndex();
        newSqlIndexDefinition.cloneTo(sqlAlterTableAddIndex.getIndexDefinition());
        resultAlterTableItems.add(sqlAlterTableAddIndex);

        return resultAlterTableItems;
    }
}

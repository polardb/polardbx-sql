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
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author wenki
 */
public class DefaultExprUtil {

    private static final Set<String> SUPPORTED_TYPE_NAME = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

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
        SUPPORTED_TYPE_NAME.add("FLOAT");
        SUPPORTED_TYPE_NAME.add("DOUBLE");
        SUPPORTED_TYPE_NAME.add("JSON");
        SUPPORTED_TYPE_NAME.add("BINARY");
        SUPPORTED_TYPE_NAME.add("BLOB");
        SUPPORTED_TYPE_NAME.add("TEXT");
    }

    public static void validateColumnExpr(SqlCall sqlCall) {
        DefaultExprUtil.ValidateIdentifierShuttle idShuttle = new DefaultExprUtil.ValidateIdentifierShuttle();
        idShuttle.visit(sqlCall);
    }

    private static class ValidateIdentifierShuttle extends SqlShuttle {
        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (id != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced identifier `%s` not support in default expression yet.", id));
            }
            return super.visit(id);
        }
    }

    public static boolean supportDataType(SqlColumnDeclaration sqlDef) {
        try {
            return SUPPORTED_TYPE_NAME.contains(sqlDef.getDataType().getTypeName().getLastName());
        } catch (Throwable ex) {
            return false;
        }
    }

    public static void buildDefaultExprColumns(TableMeta tableMeta, LogicalInsert insert, ExecutionContext ec) {
        if (!tableMeta.hasDefaultExprColumn()) {
            return;
        }

        List<String> defaultExprColumns =
            tableMeta.getPhysicalColumns().stream().filter(ColumnMeta::isDefaultExpr).map(ColumnMeta::getName)
                .collect(Collectors.toList());

        List<String> evalFieldNames = new ArrayList<>(defaultExprColumns);

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
        for (String defaultExprColumn : defaultExprColumns) {
            sourceNodes.add(getSqlCallAndValidateFromExprWithoutTableName(tableMeta.getSchemaName(),
                tableMeta.getTableName(), tableMeta.getColumn(defaultExprColumn).getField().getDefault(),
                ec));
        }

        SqlConverter sqlConverter = SqlConverter.getInstance(tableMeta.getSchemaName(), ec);
        RelOptCluster cluster = sqlConverter.createRelOptCluster();
        PlannerContext plannerContext = PlannerContext.getPlannerContext(cluster);
        List<RexNode> rexNodes = sqlConverter.getRexForGeneratedColumn(rowType, sourceNodes, plannerContext);

        insert.setDefaultExprColMetas(evalColumnMetas);
        insert.setDefaultExprEvalFieldsMapping(inputToEvalFieldsMapping);
        insert.setDefaultExprColRexNodes(rexNodes);
    }

    public static SqlCall getSqlCallAndValidateFromExprWithoutTableName(String schemaName, String tableName,
                                                                        String expr, ExecutionContext ec) {
        SqlCall sqlCall = getSqlCallAndValidateFromExprWithTableName(schemaName, tableName, expr, ec);
        GeneratedColumnUtil.RemoveTableNameFromColumnShuttle
            shuttle = new GeneratedColumnUtil.RemoveTableNameFromColumnShuttle();
        sqlCall.accept(shuttle);
        return sqlCall;
    }

    public static SqlCall getSqlCallAndValidateFromExprWithTableName(String schemaName, String tableName, String expr,
                                                                     ExecutionContext ec) {
        SQLExpr sqlExpr = new MySqlExprParser(ByteString.from(expr)).expr();
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(null, null);
        sqlExpr.accept(visitor);
        return new SqlBasicCall(SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC, new SqlNode[] {visitor.getSqlNode()},
            SqlParserPos.ZERO);
    }
}

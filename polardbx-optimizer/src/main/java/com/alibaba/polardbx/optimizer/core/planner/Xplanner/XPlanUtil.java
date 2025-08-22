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

package com.alibaba.polardbx.optimizer.core.planner.Xplanner;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanEqualTuple;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.alibaba.polardbx.rpc.XUtil;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.mysql.cj.x.protobuf.PolarxDatatypes;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import com.mysql.cj.x.protobuf.PolarxExpr;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class XPlanUtil {

    // Static options.
    private static final boolean IGNORE_PROJECT_FIELD_NAMES = true;

    private final List<SqlIdentifier> tableNames = new ArrayList<>(1); // For table name and schema name.
    private final List<ScalarParamInfo> paramInfos = new ArrayList<>();

    private final HashMap<String, Pair<Integer, Integer>> reverseTableFinder = new HashMap<>();
    // schema.table -> tableNames's idx
    private final HashMap<Integer, Integer> reverseParamFinder = new HashMap<>(); // dynParam -> position

    public void reset() {
        tableNames.clear();
        paramInfos.clear();
        reverseTableFinder.clear();
        reverseParamFinder.clear();
    }

    // Restore to add more operators for dynamic filter.
    public void restore(List<SqlIdentifier> tableNames, List<ScalarParamInfo> paramInfos) {
        reset();
        this.tableNames.addAll(tableNames);
        this.paramInfos.addAll(paramInfos);
        // Restore reverse finder.
        for (int i = 0; i < this.tableNames.size(); ++i) {
            final String digest = SqlIdentifier.getString(this.tableNames.get(i).names);
            int tablePos = -1, schemaPos = -1;
            for (int j = 0; j < this.paramInfos.size(); ++j) {
                final ScalarParamInfo info = this.paramInfos.get(j);
                if (-1 == tablePos && ScalarParamInfo.Type.TableName == info.getType() && info.getId() == i) {
                    tablePos = j;
                } else if (-1 == schemaPos && ScalarParamInfo.Type.SchemaName == info.getType() && info.getId() == i) {
                    schemaPos = j;
                }
                if (tablePos != -1 && schemaPos != -1) {
                    break;
                }
            }
            if (tablePos != -1 && schemaPos != -1) {
                // Found.
                reverseTableFinder.put(digest, new Pair<>(tablePos, schemaPos));
            }
        }
        for (int i = 0; i < paramInfos.size(); ++i) {
            final ScalarParamInfo info = paramInfos.get(i);
            if (info.getType() != ScalarParamInfo.Type.DynamicParam) {
                continue;
            }
            reverseParamFinder.put(info.getId(), i);
        }
    }

    public List<SqlIdentifier> getTableNames() {
        return tableNames;
    }

    public List<ScalarParamInfo> getParamInfos() {
        return paramInfos;
    }

    /**
     * Static plan judgment.
     */

    // Whether the project contains only reorder and cut.
    public static boolean isSimpleProject(Project project) {
        for (RexNode rexNode : project.getChildExps()) {
            if (!(rexNode instanceof RexInputRef)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Static structure generator.
     */

    private static PolarxDatatypes.Scalar literalToScalar(RexLiteral literal, RexContext context) {
        final Comparable value = RexLiteral.value(literal);
        switch (literal.getTypeName()) {
        case CHAR:
            NlsString nlsString = (NlsString) value;
            return XUtil.genUtf8StringScalar(nlsString.getValue());
        case INTEGER:
        case MEDIUMINT:
        case SMALLINT:
        case TINYINT:
            return XUtil.genSIntScalar((Integer) value);
        case TINYINT_UNSIGNED:
        case SMALLINT_UNSIGNED:
        case MEDIUMINT_UNSIGNED:
            return XUtil.genUIntScalar((Integer) value);
        case BOOLEAN:
            return XUtil.genBooleanScalar((Boolean) value);
        case BIGINT_UNSIGNED: // BigInteger
            return XUtil.genUIntScalar(((BigInteger) value).longValue()); // longValue return the low 64bits.
        case DOUBLE: // BigDecimal
            return XUtil.genDoubleScalar(((BigDecimal) value).doubleValue());
        case BIGINT:
            return XUtil.genSIntScalar((Long) value);
        case INTEGER_UNSIGNED:
            return XUtil.genUIntScalar((Long) value);
        case BINARY: // org.apache.calcite.avatica.util.ByteString
            final byte[] b = ((org.apache.calcite.avatica.util.ByteString) value).getBytes();
            if (context.getPreferType().getSqlTypeName() == SqlTypeName.BIT) {
                final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).put(b).order(ByteOrder.LITTLE_ENDIAN);
                buf.rewind();
                return XUtil.genUIntScalar(buf.getLong(0));
            } else {
                return XUtil.genOctetsScalar(ByteBuffer.wrap(b));
            }
        case NULL:
            if (!context.isNullable()) {
                throw GeneralUtil.nestedException("Null when non-nullable literal.");
            }
            return XUtil.genNullScalar();
        case SYMBOL: // Enum
            return XUtil.genUtf8StringScalar("FLAG(" + value + ")");
        case DATE: // DateString
        case TIME: // TimeString
        case TIME_WITH_LOCAL_TIME_ZONE: // TimeString
        case TIMESTAMP: // TimestampString
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE: // TimestampString
            return XUtil.genUtf8StringScalar(value.toString());
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
            if (value != null) {
                return XUtil.genUtf8StringScalar(value.toString());
            } else {
                return XUtil.genNullScalar();
            }
        case DECIMAL: // BigDecimal FIXME
        case MULTISET:
        case ROW:
        default:
            throw GeneralUtil.nestedException("RexLiteral " + literal.getTypeName().getName() + " not support.");
        }
    }

    public static PolarxExpr.Expr genRefExpr(int refId) {
        final PolarxExpr.Expr.Builder exprBuilder = PolarxExpr.Expr.newBuilder();

        exprBuilder.setType(PolarxExpr.Expr.Type.REF);
        exprBuilder.setRefId(refId);
        return exprBuilder.build();
    }

    public static PolarxExpr.Expr genScalarExpr(PolarxDatatypes.Scalar scalar) {
        final PolarxExpr.Expr.Builder exprBuilder = PolarxExpr.Expr.newBuilder();

        exprBuilder.setType(PolarxExpr.Expr.Type.LITERAL);
        exprBuilder.setLiteral(scalar);
        return exprBuilder.build();
    }

    public static PolarxExpr.Expr genBloomFilter(PolarxExpr.Expr input,
                                                 int bits, int numHash, String strategy, ByteBuffer data) {
        final PolarxExpr.Expr.Builder exprBuilder = PolarxExpr.Expr.newBuilder();
        final PolarxExpr.FunctionCall.Builder functionBuilder = PolarxExpr.FunctionCall.newBuilder();
        final PolarxExpr.Identifier.Builder identifierBuilder = PolarxExpr.Identifier.newBuilder();

        identifierBuilder.setName("bloom_filter");
        functionBuilder.setName(identifierBuilder);
        functionBuilder.addParam(input);
        functionBuilder.addParam(genScalarExpr(XUtil.genUIntScalar(bits)));
        functionBuilder.addParam(genScalarExpr(XUtil.genUIntScalar(numHash)));
        functionBuilder.addParam(genScalarExpr(XUtil.genUtf8StringScalar(strategy)));
        functionBuilder.addParam(genScalarExpr(XUtil.genOctetsScalar(data)));
        exprBuilder.setType(PolarxExpr.Expr.Type.FUNC_CALL);
        exprBuilder.setFunctionCall(functionBuilder);
        return exprBuilder.build();
    }

    public static PolarxExecPlan.AnyPlan genTableProject(PolarxExecPlan.AnyPlan subPlan, List<String> fields) {
        assert subPlan.getPlanType() == PolarxExecPlan.AnyPlan.PlanType.GET ||
            subPlan.getPlanType() == PolarxExecPlan.AnyPlan.PlanType.TABLE_SCAN;

        final PolarxExecPlan.TableProject.Builder projectBuilder = PolarxExecPlan.TableProject.newBuilder();
        final PolarxExecPlan.AnyPlan.Builder planBuilder = PolarxExecPlan.AnyPlan.newBuilder();

        projectBuilder.setSubReadPlan(subPlan);
        for (String field : fields) {
            projectBuilder.addFields(XUtil.genUtf8StringScalar(field));
        }

        planBuilder.setPlanType(PolarxExecPlan.AnyPlan.PlanType.TABLE_PROJECT);
        planBuilder.setTableProject(projectBuilder);
        return planBuilder.build();
    }

    public static PolarxExecPlan.AnyPlan genFilter(PolarxExecPlan.AnyPlan sub, PolarxExpr.Expr condition) {
        final PolarxExecPlan.Filter.Builder filterBuilder = PolarxExecPlan.Filter.newBuilder();
        final PolarxExecPlan.AnyPlan.Builder planBuilder = PolarxExecPlan.AnyPlan.newBuilder();

        filterBuilder.setSubReadPlan(sub);
        filterBuilder.setExpr(condition);
        planBuilder.setPlanType(PolarxExecPlan.AnyPlan.PlanType.FILTER);
        planBuilder.setFilter(filterBuilder);
        return planBuilder.build();
    }

    public static PolarxExecPlan.AnyPlan genProject(PolarxExecPlan.AnyPlan sub, List<String> fields,
                                                    List<PolarxExpr.Expr> exprs) {
        final PolarxExecPlan.Project.Builder projectBuilder = PolarxExecPlan.Project.newBuilder();
        final PolarxExecPlan.AnyPlan.Builder planBuilder = PolarxExecPlan.AnyPlan.newBuilder();

        projectBuilder.setSubReadPlan(sub);
        if (!IGNORE_PROJECT_FIELD_NAMES) {
            fields.stream().map(XUtil::genUtf8StringScalar).forEach(projectBuilder::addFields);
        }
        exprs.forEach(projectBuilder::addExprs);
        planBuilder.setPlanType(PolarxExecPlan.AnyPlan.PlanType.PROJECT);
        planBuilder.setProject(projectBuilder);
        return planBuilder.build();
    }

    /**
     * Dynamic structure generator.
     */

    // <table_pos, schema_pos>
    public Pair<Integer, Integer> table2position(SqlIdentifier identifier) {
        final String digest = SqlIdentifier.getString(identifier.names);
        return reverseTableFinder.computeIfAbsent(digest, key -> {
            final int identifierIdx = tableNames.size();
            tableNames.add(identifier);
            final int tableIdx = paramInfos.size();
            paramInfos.add(new ScalarParamInfo(ScalarParamInfo.Type.TableName, identifierIdx, null, false));
            final int schemaIdx = paramInfos.size();
            paramInfos.add(new ScalarParamInfo(ScalarParamInfo.Type.SchemaName, identifierIdx, null, false));
            return new Pair<>(tableIdx, schemaIdx);
        });
    }

    public int dynamicParam2position(RexDynamicParam dynamicParam, RexContext context) {
        return reverseParamFinder.computeIfAbsent(dynamicParam.getIndex(), key -> {
            final int pos = paramInfos.size();
            if (dynamicParam.getIndex() < 0) {
                throw GeneralUtil.nestedException("TODO: support sub-query rex.");
            }
            // FIXME
            if ((null == context ? dynamicParam.getType() : context.getPreferType()).getSqlTypeName()
                == SqlTypeName.DECIMAL) {
                throw GeneralUtil.nestedException("TODO: support decimal param in plan.");
            }
            paramInfos.add(new ScalarParamInfo(ScalarParamInfo.Type.DynamicParam, dynamicParam.getIndex(),
                null == context ? dynamicParam.getType() : context.getPreferType(),
                null == context || context.isNullable()));
            return pos;
        });
    }

    public PolarxExecPlan.TableInfo genTableInfo(TableScan tableScan) {
        final SqlIdentifier identifier = new SqlIdentifier(tableScan.getTable().getQualifiedName(), SqlParserPos.ZERO);

        final PolarxExecPlan.TableInfo.Builder tableBuilder = PolarxExecPlan.TableInfo.newBuilder();

        final Pair<Integer, Integer> posPair = table2position(identifier);
        tableBuilder.setName(XUtil.genPlaceholderScalar(posPair.getKey()));
        tableBuilder.setSchemaName(XUtil.genPlaceholderScalar(posPair.getValue()));
        return tableBuilder.build();
    }

    public PolarxExecPlan.AnyPlan genTableScan(TableScan tableScan) {
        final PolarxExecPlan.TableScanPlan.Builder scanBuilder = PolarxExecPlan.TableScanPlan.newBuilder();
        final PolarxExecPlan.AnyPlan.Builder planBuilder = PolarxExecPlan.AnyPlan.newBuilder();

        scanBuilder.setTableInfo(genTableInfo(tableScan));

        // At least one index to scan.
        final TableMeta tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        if (!tableMeta.isHasPrimaryKey()) {
            final List<IndexMeta> indexMetas = tableMeta.getIndexes();
            String physicalIndexName = null;
            for (IndexMeta indexMeta : indexMetas) {
                if (indexMeta.getPhysicalIndexName().equalsIgnoreCase("PRIMARY") && !tableMeta.isHasPrimaryKey()) {
                    continue;
                }
                physicalIndexName = indexMeta.getPhysicalIndexName();
                break;
            }
            if (null == physicalIndexName) {
                throw GeneralUtil.nestedException("Not support table scan without any index.");
            }
            final PolarxExecPlan.IndexInfo.Builder indexBuilder = PolarxExecPlan.IndexInfo.newBuilder();
            indexBuilder.setName(XUtil.genUtf8StringScalar(physicalIndexName));
            scanBuilder.setIndexInfo(indexBuilder);
        } // Use primary by default.

        planBuilder.setPlanType(PolarxExecPlan.AnyPlan.PlanType.TABLE_SCAN);
        planBuilder.setTableScanPlan(scanBuilder);
        return planBuilder.build();
    }

    public PolarxDatatypes.Scalar rexToScalar(RexNode rexNode, RexContext context) {
        switch (rexNode.getKind()) {
        case DYNAMIC_PARAM:
            return XUtil.genPlaceholderScalar(dynamicParam2position((RexDynamicParam) rexNode, context));

        case LITERAL:
            return literalToScalar((RexLiteral) rexNode, context);

        default:
            throw GeneralUtil.nestedException("Not support " + rexNode.getKind().name() + " to scalar.");
        }
    }

    public static class RexContext {

        private final RelDataType preferType;
        private final boolean nullable;

        public RexContext(RelDataType preferType) {
            this(preferType, true);
        }

        public RexContext(RelDataType preferType, boolean nullable) {
            this.preferType = preferType;
            this.nullable = nullable;
        }

        public RelDataType getPreferType() {
            return preferType;
        }

        public boolean isNullable() {
            return nullable;
        }

    }

    public PolarxExpr.Expr rexToExpr(RexNode rexNode) {
        return rexToExpr(rexNode, null);
    }

    private void exprCompatibleCheck(RexNode rexNode) {
        switch (rexNode.getKind()) {
        // func:bloom_filter
        case OTHER_FUNCTION:
            if (((RexCall) rexNode).getOperator().getName().equals("bloom_filter")) {
                return;
            }
            break;

        // ?,ref,literal
        case DYNAMIC_PARAM:
        case INPUT_REF:
        case LITERAL:
            return;

        // +,-,*,/,div,mod
        case PLUS:
            if (((RexCall) rexNode).getOperator().getName().equals("+")) {
                return;
            }
            break;

        case MINUS:
            if (((RexCall) rexNode).getOperator().getName().equals("-")) {
                return;
            }
            break;

        case TIMES:
            if (((RexCall) rexNode).getOperator().getName().equals("*")) {
                return;
            }
            break;

        case DIVIDE: // Include / and DIV.
            if (((RexCall) rexNode).getOperator().getName().equals("/")) {
                return;
            } else if (((RexCall) rexNode).getOperator().getName().equalsIgnoreCase("DIV")) {
                return;
            }
            break;

        case MOD:
            if (((RexCall) rexNode).getOperator().getName().equalsIgnoreCase("MOD")) {
                return;
            }
            break;

        // ==,!=,>,>=,<,<=
        case EQUALS:
            if (((RexCall) rexNode).getOperator().getName().equals("=")) {
                return;
            } else if (((RexCall) rexNode).getOperator().getName().equals("<=>")) {
                return;
            }
            break;

        case IS_NOT_DISTINCT_FROM:
            if (((RexCall) rexNode).getOperator().getName().equals("<=>")) {
                return;
            }
            break;

        case NOT_EQUALS:
            if (((RexCall) rexNode).getOperator().getName().equals("<>")) {
                return;
            }
            break;

        case GREATER_THAN:
            if (((RexCall) rexNode).getOperator().getName().equals(">")) {
                return;
            }
            break;

        case GREATER_THAN_OR_EQUAL:
            if (((RexCall) rexNode).getOperator().getName().equals(">=")) {
                return;
            }
            break;

        case LESS_THAN:
            if (((RexCall) rexNode).getOperator().getName().equals("<")) {
                return;
            }
            break;

        case LESS_THAN_OR_EQUAL:
            if (((RexCall) rexNode).getOperator().getName().equals("<=")) {
                return;
            }
            break;

        // &&,||,!
        case AND:
            if (((RexCall) rexNode).getOperator().getName().equalsIgnoreCase("AND")) {
                return;
            }
            break;

        case OR:
            if (((RexCall) rexNode).getOperator().getName().equalsIgnoreCase("OR")) {
                return;
            }
            break;

        case NOT:
            if (((RexCall) rexNode).getOperator().getName().equalsIgnoreCase("NOT")) {
                return;
            }
            break;

        // is null, is not null  operand must be null(scalar).
        case IS_NULL:
            if (((RexCall) rexNode).getOperator().getName().equalsIgnoreCase("IS NULL")) {
                return;
            }
            break;

        case IS_NOT_NULL:
            if (((RexCall) rexNode).getOperator().getName().equalsIgnoreCase("IS NOT NULL")) {
                return;
            }
            break;

        default:
            break;
        }
        throw GeneralUtil.nestedException("TODO: need to impl rex on server. type:" + rexNode.getKind().name());
    }

    public PolarxExpr.Expr rexToExpr(RexNode rexNode, RexContext context) {
        if (rexNode.isAlwaysTrue()) {
            return genScalarExpr(XUtil.genBooleanScalar(true));
        }
        if (rexNode.isAlwaysFalse()) {
            return genScalarExpr(XUtil.genBooleanScalar(false));
        }

        // Check compatible of server.
        exprCompatibleCheck(rexNode);

        final PolarxExpr.Expr.Builder exprBuilder = PolarxExpr.Expr.newBuilder();
        final PolarxExpr.FunctionCall.Builder funcBuilder = PolarxExpr.FunctionCall.newBuilder();
        final PolarxExpr.Identifier.Builder identifierBuilder = PolarxExpr.Identifier.newBuilder();
        final PolarxExpr.Operator.Builder operatorBuilder = PolarxExpr.Operator.newBuilder();
        final PolarxExpr.Array.Builder arrayBuilder = PolarxExpr.Array.newBuilder();

        // Convertible binary operator. Should fill rex context.
        switch (rexNode.getKind()) {
        case EQUALS:
        case IS_NOT_DISTINCT_FROM:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case LIKE:
        case IN:
        case NOT_IN:
        case MINUS_PREFIX:
        case PLUS_PREFIX:
        case INVERT_PREFIX:
        case PLUS:
        case MINUS:
        case TIMES:
        case DIVIDE:
        case MOD:
            if (rexNode instanceof RexCall && ((RexCall) rexNode).getOperands().size() >= 1) {
                context = new RexContext(((RexCall) rexNode).getOperands().get(0).getType());
            }
            break;

        default:
            break;
        }

        final RexContext finalContext = context;
        switch (rexNode.getKind()) {
        case DYNAMIC_PARAM:
            exprBuilder.setType(PolarxExpr.Expr.Type.PLACEHOLDER);
            exprBuilder.setPosition(dynamicParam2position((RexDynamicParam) rexNode, finalContext));
            return exprBuilder.build();

        case INPUT_REF:
            exprBuilder.setType(PolarxExpr.Expr.Type.REF);
            exprBuilder.setRefId(((RexInputRef) rexNode).getIndex());
            return exprBuilder.build();

        case OTHER_FUNCTION:
            exprBuilder.setType(PolarxExpr.Expr.Type.FUNC_CALL);
            identifierBuilder.setName(((RexCall) rexNode).getOperator().getName());
            funcBuilder.setName(identifierBuilder);
            ((RexCall) rexNode).getOperands().forEach(op -> funcBuilder.addParam(rexToExpr(op, finalContext)));
            exprBuilder.setFunctionCall(funcBuilder);
            return exprBuilder.build();

        case AND:
        case OR:
        case EQUALS:
        case IS_NOT_DISTINCT_FROM:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case LIKE:
            exprBuilder.setType(PolarxExpr.Expr.Type.OPERATOR);
            switch (rexNode.getKind()) {
            case AND:
                operatorBuilder.setName("&&");
                break;
            case OR:
                operatorBuilder.setName("||");
                break;
            case NOT_EQUALS:
                operatorBuilder.setName("!=");
                break;
            case EQUALS:
                if (((RexCall) rexNode).getOperator().getName().equals("=")) {
                    operatorBuilder.setName("==");
                } else {
                    operatorBuilder.setName(((RexCall) rexNode).getOperator().getName().toLowerCase());
                }
                break;
            case IS_NOT_DISTINCT_FROM:
                operatorBuilder.setName("<=>");
                break;
            default:
                // >,>=,<,<=,LIKE
                operatorBuilder.setName(((RexCall) rexNode).getOperator().getName().toLowerCase());
                break;
            }
            ((RexCall) rexNode).getOperands().forEach(op -> operatorBuilder.addParam(rexToExpr(op, finalContext)));
            exprBuilder.setOperator(operatorBuilder);
            return exprBuilder.build();

        case IS_NULL:
        case IS_NOT_NULL:
            exprBuilder.setType(PolarxExpr.Expr.Type.OPERATOR);
            operatorBuilder.setName(SqlKind.IS_NULL == rexNode.getKind() ? "is" : "is_not");
            operatorBuilder.addParam(rexToExpr(((RexCall) rexNode).getOperands().get(0), finalContext));
            operatorBuilder.addParam(genScalarExpr(XUtil.genNullScalar()));
            exprBuilder.setOperator(operatorBuilder);
            return exprBuilder.build();

        case LITERAL:
            exprBuilder.setType(PolarxExpr.Expr.Type.LITERAL);
            exprBuilder.setLiteral(literalToScalar((RexLiteral) rexNode, finalContext));
            return exprBuilder.build();

        case ROW:
            exprBuilder.setType(PolarxExpr.Expr.Type.ARRAY);
            ((RexCall) rexNode).getOperands().forEach(op -> arrayBuilder.addValue(rexToExpr(op, finalContext)));
            exprBuilder.setArray(arrayBuilder);
            return exprBuilder.build();

        case IN:
        case NOT_IN:
            exprBuilder.setType(PolarxExpr.Expr.Type.OPERATOR);
            operatorBuilder.setName(SqlKind.IN == rexNode.getKind() ? "in" : "not_in");
            operatorBuilder.addParam(rexToExpr(((RexCall) rexNode).operands.get(0), finalContext));
            // Extend all rest ROW.
            if (SqlKind.ROW == ((RexCall) rexNode).operands.get(1).getKind()) {
                ((RexCall) ((RexCall) rexNode).operands.get(1)).getOperands()
                    .forEach(op -> operatorBuilder.addParam(rexToExpr(op, finalContext)));
                exprBuilder.setOperator(operatorBuilder);
                return exprBuilder.build();
            }
            throw GeneralUtil.nestedException("Unsupported rex in.");

        case NOT: {
            final RexNode operand = ((RexCall) rexNode).operands.get(0);
            PolarxExpr.Expr expr = rexToExpr(operand, finalContext);
            exprBuilder.setType(PolarxExpr.Expr.Type.OPERATOR);
            switch (operand.getKind()) {
            case IN:
                operatorBuilder.setName("not_in");
                operatorBuilder.addAllParam(expr.getOperator().getParamList());
                exprBuilder.setOperator(operatorBuilder);
                return exprBuilder.build();
            case LIKE:
                operatorBuilder.setName("not_like");
                operatorBuilder.addAllParam(expr.getOperator().getParamList());
                exprBuilder.setOperator(operatorBuilder);
                return exprBuilder.build();
            case SIMILAR:
                break;

            default:
                operatorBuilder.setName("!");
                operatorBuilder.addParam(expr);
                exprBuilder.setOperator(operatorBuilder);
                return exprBuilder.build();
            }
        }
        break;

        case MINUS_PREFIX:
        case PLUS_PREFIX:
        case INVERT_PREFIX:
        case PLUS:
        case MINUS:
        case TIMES:
        case DIVIDE:
        case MOD:
            final String name;
            if (rexNode.getKind() == SqlKind.MOD) {
                name = "%";
            } else {
                // -,+,~,*,/,DIV
                name = ((RexCall) rexNode).getOperator().getName().toLowerCase();
            }
            exprBuilder.setType(PolarxExpr.Expr.Type.OPERATOR);
            operatorBuilder.setName(name);
            ((RexCall) rexNode).getOperands().forEach(op -> operatorBuilder.addParam(rexToExpr(op, finalContext)));
            exprBuilder.setOperator(operatorBuilder);
            return exprBuilder.build();
        }
        throw GeneralUtil.nestedException("TODO: support more rex. type:" + rexNode.getKind().name());
    }

    /**
     * General converter.
     */

    public PolarxExecPlan.AnyPlan convert(XPlanTableScan tableScan, boolean tableIdentical) {
        final PolarxExecPlan.AnyPlan subPlan;

        if (0 == tableScan.getGetExprs().size()) {
            // No get.
            if (XConnectionManager.getInstance().isEnableXplanTableScan()) {
                subPlan = genTableScan(tableScan);
            } else {
                throw GeneralUtil.nestedException("XPlan of table scan denied.");
            }
        } else {
            // With get.
            if (null == tableScan.getGetIndex()) {
                throw GeneralUtil.nestedException("XPlan get without index info.");
            }

            final PolarxExecPlan.AnyPlan.Builder planBuilder = PolarxExecPlan.AnyPlan.newBuilder();
            final PolarxExecPlan.GetPlan.Builder getBuilder = PolarxExecPlan.GetPlan.newBuilder();
            final PolarxExecPlan.IndexInfo.Builder indexBuilder = PolarxExecPlan.IndexInfo.newBuilder();
            final PolarxExecPlan.KeyExpr.Builder keyExprBuilder = PolarxExecPlan.KeyExpr.newBuilder();
            final PolarxExecPlan.GetExpr.Builder getExprBuilder = PolarxExecPlan.GetExpr.newBuilder();

            getBuilder.setTableInfo(genTableInfo(tableScan));
            indexBuilder.setName(XUtil.genUtf8StringScalar(tableScan.getGetIndex()));
            getBuilder.setIndexInfo(indexBuilder);

            for (List<XPlanEqualTuple> getExpr : tableScan.getGetExprs()) {
                // Clear GetExpr.
                getExprBuilder.clear();

                for (XPlanEqualTuple keyExpr : getExpr) {
                    final String keyName =
                        tableScan.getOriginalRowType().getFieldNames().get(keyExpr.getKey().getIndex());
                    final RelDataType keyType =
                        tableScan.getOriginalRowType().getFieldList().get(keyExpr.getKey().getIndex()).getType();
                    final SqlCollation collation =
                        keyType != null && keyType.getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER ?
                            keyType.getCollation() : null;
                    final String name = collation != null ? collation.getCharset().name().toUpperCase() : null;
                    if (name != null && !name.startsWith("UTF8") && !name.startsWith("UTF-8")) {
                        throw GeneralUtil.nestedException("XPlan does not support charset except utf8.");
                    }
                    final PolarxDatatypes.Scalar value =
                        rexToScalar(keyExpr.getValue(), new RexContext(keyType, keyExpr.getOperator().isName("<=>")));
                    keyExprBuilder.setField(XUtil.genUtf8StringScalar(keyName));
                    keyExprBuilder.setValue(value);
                    // Add to get expr.
                    getExprBuilder.addKeys(keyExprBuilder);
                }
                // Add to get.
                getBuilder.addKeys(getExprBuilder);
            }

            planBuilder.setPlanType(PolarxExecPlan.AnyPlan.PlanType.GET);
            planBuilder.setGetPlan(getBuilder);
            subPlan = planBuilder.build();
        }

        // Now generate TableProject if needed.
        if (0 == tableScan.getProjects().size()) {
            if (tableIdentical) {
                return subPlan;
            } else {
                return genTableProject(subPlan, tableScan.getRowType().getFieldNames());
            }
        } else {
            List<String> fields = tableScan.getProjects().stream()
                .map(idx -> tableScan.getOriginalRowType().getFieldNames().get(idx))
                .collect(Collectors.toList());
            return genTableProject(subPlan, fields);
        }
    }

    public static class ScalarParamInfo {
        public enum Type {
            DynamicParam,
            TableName,
            SchemaName
        }

        public final Type type;
        public final int id;
        private final boolean isBit;
        public final boolean nullable;

        public ScalarParamInfo(Type type, int id, RelDataType dataType, boolean nullable) {
            this.type = type;
            this.id = id;
            this.isBit = dataType == null ? false : dataType.getSqlTypeName() == SqlTypeName.BIT;
            this.nullable = nullable;
        }

        public ScalarParamInfo(Type type, int id, boolean isBit, boolean nullable) {
            this.type = type;
            this.id = id;
            this.isBit = isBit;
            this.nullable = nullable;
        }

        public Type getType() {
            return type;
        }

        public int getId() {
            return id;
        }

        public boolean isNullable() {
            return nullable;
        }

        public boolean isBit() {
            return isBit;
        }
    }
}

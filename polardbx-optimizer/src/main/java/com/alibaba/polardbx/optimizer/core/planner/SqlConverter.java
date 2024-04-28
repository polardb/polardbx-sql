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

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlValidator;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.exception.SqlValidateException;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.DrdsViewExpander;
import com.google.common.collect.Lists;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelPartitionWiseTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SQL转换类,包括validate和生成逻辑计划
 *
 * @author lingce.ldm 2017-07-05 19:32
 */
public class SqlConverter {

    private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);

    private final RelDataTypeFactory typeFactory;
    private final SqlParser.Config parserConfig;
    private final SqlToRelConverter.Config converterConfig;
    private final TddlValidator validator;
    private final CalciteCatalogReader catalog;
    private final SqlOperatorTable opTab;
    private final Map<String, SchemaManager> smMap;

    private SqlConverter(String schemaName, ExecutionContext ec) {
        this.typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        this.parserConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).setParserFactory(SqlParserImpl.FACTORY).build();
        this.converterConfig = SqlToRelConverter.configBuilder()
            .withConvertTableAccess(false)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withExpand(false)
            .build();
        this.catalog = RelUtils.buildCatalogReader(schemaName, ec);

        this.opTab = TddlOperatorTable.instance();
        this.validator = new TddlValidator(opTab, catalog, typeFactory, SqlConformanceEnum.MYSQL_5);
        // enable type coercion
        validator.setEnableTypeCoercion(true);
        validator.setTypeCoercion(new TypeCoercionImpl(typeFactory, validator));
        validator.setDefaultNullCollation(NullCollation.LOW);
        validator.setIdentifierExpansion(false);
        validator.setCallRewrite(false);
        this.smMap = ec.getSchemaManagers();
    }

    public static SqlConverter getInstance(ExecutionContext ec) {
        return new SqlConverter(DefaultSchema.getSchemaName(), ec);
    }

    public static SqlConverter getInstance(String schemaName, ExecutionContext ec) {
        if (schemaName == null) {
            schemaName = DefaultSchema.getSchemaName();
        }
        return new SqlConverter(schemaName, ec);
    }

    public void enableAutoPartition() {
        validator.setEnableAutoPartition(true);
    }

    public void disableAutoPartition() {
        validator.setEnableAutoPartition(false);
    }

    public void setAutoPartitionDatabase(boolean autoModeDb) {
        validator.setAutoPartitionDatabase(autoModeDb);
    }

    public void setDefaultSingle(boolean isDefaultSingle) {
        validator.setDefaultSingle(isDefaultSingle);
    }

    public SqlNode validate(final SqlNode parsedNode) {
        checkSqlKindSupport(parsedNode);
        SqlNode validatedNode;
        try {
            validatedNode = validator.validate(parsedNode);
        } catch (Exception e) {
            logger.error("Sql validate error : " + parsedNode, e);
            if (ErrorCode.match(e.getMessage())) {
                throw e;
            } else {
                throw new SqlValidateException(e, e.getMessage());
            }
        }
        return validatedNode;
    }

    private void checkSqlKindSupport(final SqlNode ast) {
        if (!OptimizerUtils.supportedSqlKind(ast)) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Unsupported SQL kind: " + ast.getKind());
        }
        if (ConfigDataMode.isReadOnlyMode() && SqlKind.DDL.contains(ast.getKind())) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPERATION_NOT_ALLOWED,
                "DDL Operations are not allowed on a Read-Only Instance.");
        }
    }

    public RelNode toRel(final SqlNode validatedNode) {
        return toRel(validatedNode, new PlannerContext(new ExecutionContext().setSchemaManagers(smMap)));
    }

    public RelNode toRel(final SqlNode validatedNode, final PlannerContext plannerContext) {
        final RelOptCluster cluster = createRelOptCluster(plannerContext);
        return toRel(validatedNode, cluster);
    }

    public RelNode toRel(final SqlNode validatedNode, final RelOptCluster cluster) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(cluster);
        int threshold = Integer.MAX_VALUE;
        if (plannerContext.getExecutionContext() != null) {
            if (validatedNode.getKind() == SqlKind.SELECT) {
                threshold = plannerContext.getExecutionContext().getParamManager().getInt(
                    ConnectionParams.IN_SUB_QUERY_THRESHOLD);
            } else if (plannerContext.getExecutionContext().getParamManager().getBoolean(
                ConnectionParams.ENABLE_IN_SUB_QUERY_FOR_DML)) {
                threshold = plannerContext.getExecutionContext().getParamManager().getInt(
                    ConnectionParams.IN_SUB_QUERY_THRESHOLD);
            }
        }

        final SqlToRelConverter sqlToRelConverter = new TddlSqlToRelConverter(
            new DrdsViewExpander(cluster, this, catalog),
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            PlannerContext.getPlannerContext(cluster),
            threshold);

        RelRoot root = sqlToRelConverter.convertQuery(validatedNode, false, true);
        return root.rel;
    }

    public RelOptCluster createRelOptCluster() {
        PlannerContext pc = new PlannerContext(new ExecutionContext().setSchemaManagers(smMap));
        return createRelOptCluster(pc);
    }

    public RelOptCluster createRelOptCluster(PlannerContext plannerContext) {
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCostFactory costFactory = DrdsRelOptCostImpl.FACTORY;
        VolcanoPlanner planner = new VolcanoPlanner(costFactory, plannerContext);
        if (plannerContext != null && plannerContext.getExecutionContext() != null &&
            plannerContext.getExecutionContext().isEnableRuleCounter()) {
            planner.setRuleCounter();
        }
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelPartitionWiseTraitDef.INSTANCE);
        RelOptCluster relOptCluster = RelOptCluster.create(planner, rexBuilder);
        relOptCluster.setMetadataProvider(DrdsRelMetadataProvider.INSTANCE);
        return relOptCluster;
    }

    public RelOptSchema getCatalog() {
        return catalog;
    }

    public SqlValidatorImpl getValidator() {
        return validator;
    }

    public SqlInsert rewriteSqlInsert(SqlInsert oldInsert, final PlannerContext plannerContext, final int[] flag) {
        final RelOptCluster cluster = createRelOptCluster(plannerContext);
        final SqlToRelConverter sqlToRelConverter = new TddlSqlToRelConverter(null,
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            plannerContext);
        final List<SqlNode> newSelectList = Lists.newArrayList();
        final List<SqlNode> newUpdateList = Lists.newArrayList();
        final List<SqlNode> operandList = oldInsert.getOperandList();
        boolean hasUpdateAttribute = (operandList.get(4) != null && ((SqlNodeList) operandList.get(4)).size() > 0);
        boolean hasAllColumns = operandList.get(3) == null;
        // all columns are included in the target column list
        // and has not on duplicate key update columns
        if (hasAllColumns && !hasUpdateAttribute) {
            return oldInsert;
        }
        final List<SqlNode> columnList = Lists.newArrayList();
        final List<SqlNode> valueClauseList = Lists.newArrayList();
        RelOptTable targetTable = sqlToRelConverter.getTargetTable(oldInsert);
        final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
        final TableMeta tableMeta = plannerContext.getExecutionContext().getSchemaManager(qn.left).getTable(qn.right);
        final TreeSet<String> targetColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final TreeSet<String> updateColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final RelDataType tableRowType = targetTable.getRowType();

        if (!hasAllColumns) {
            columnList.addAll(((SqlNodeList) operandList.get(3)).getList());
        }

        for (int i = 0; i < columnList.size(); i++) {
            SqlIdentifier id = (SqlIdentifier) columnList.get(i);
            RelDataTypeField field = SqlValidatorUtil
                .getTargetField(tableRowType, typeFactory, id, catalog, targetTable);
            assert field != null : "column " + id.toString() + " not found";
            targetColumnSet.add(field.getName());
        }
        final AtomicInteger ordinal = new AtomicInteger(columnList.size());

        final List<List<SqlNode>> valueList = Lists.newArrayList();
        if (oldInsert.getOperandList().get(2) instanceof SqlSelect) {
            newSelectList.addAll(((SqlSelect) oldInsert.getOperandList().get(2)).getSelectList().getList());
        } else {
            for (SqlNode sqlNode : ((SqlBasicCall) operandList.get(2)).getOperands()) {
                assert sqlNode instanceof SqlBasicCall;
                valueList.add(new ArrayList<>(Arrays.asList(((SqlBasicCall) sqlNode).getOperands())));
            }
            valueClauseList.addAll(Arrays.asList(((SqlBasicCall) operandList.get(2)).getOperands()));
        }

        if (hasUpdateAttribute) {
            newUpdateList.addAll(((SqlNodeList) operandList.get(4)).getList());
        }

        tableMeta.getAllColumns()
            .stream()
            .filter(c -> ((!targetColumnSet.contains(c.getName()) && !hasAllColumns) || (hasUpdateAttribute
                && !updateColumnSet.contains(c.getName()))))
            .forEach(columnMeta -> {
                final Field field = columnMeta.getField();

                // Add column with default value of CURRENT_TIMESTAMP
                if (TStringUtil.containsIgnoreCase(field.getDefault(), "CURRENT_TIMESTAMP")) {
                    if (field.getDataType() == DataTypes.TimestampType
                        || field.getDataType() == DataTypes.DatetimeType) {

                        if ((!targetColumnSet.contains(columnMeta.getName()) && !hasAllColumns)) {
                            columnList
                                .add(new SqlIdentifier(columnMeta.getName(), SqlParserPos.ZERO));
                            if (newSelectList.size() > 0) {
                                newSelectList
                                    .add(SqlValidatorUtil.addAlias(new SqlBasicCall(TddlOperatorTable.NOW,
                                        SqlNode.EMPTY_ARRAY,
                                        SqlParserPos.ZERO), SqlUtil.deriveAliasFromOrdinal(ordinal.getAndIncrement())));
                            } else {
                                int i = 0;
                                for (SqlNode sqlNode : ((SqlBasicCall) operandList.get(2)).getOperands()) {
                                    assert sqlNode instanceof SqlBasicCall;
                                    valueList.get(i).add(new SqlBasicCall(TddlOperatorTable.NOW,
                                        SqlNode.EMPTY_ARRAY,
                                        SqlParserPos.ZERO));
                                    i++;
                                }
                            }
                        }
                        flag[0] = 1;
                    }
                } else if (TStringUtil.containsIgnoreCase(field.getExtra(), "CURRENT_TIMESTAMP")) {
                    if (field.getDataType() == DataTypes.TimestampType
                        || field.getDataType() == DataTypes.DatetimeType) {
                        if (hasUpdateAttribute && !updateColumnSet.contains(columnMeta.getName())) {
                            newUpdateList.add(new SqlBasicCall(SqlStdOperatorTable.EQUALS,
                                new SqlNode[] {
                                    new SqlIdentifier(columnMeta.getName(), SqlParserPos.ZERO),
                                    new SqlBasicCall(TddlOperatorTable.NOW,
                                        SqlNode.EMPTY_ARRAY,
                                        SqlParserPos.ZERO)},
                                SqlParserPos.ZERO));
                        }
                        flag[0] = 1;
                    }
                }
            });

        if (flag[0] == 1 && newSelectList.size() == 0) {
            // new insert statement
            List<SqlNode> newValueClauseNodes = Lists.newArrayList();
            int i = 0;
            for (SqlNode valueClause : valueClauseList) {
                SqlNode[] t = new SqlNode[valueList.get(i).size()];
                (valueList.get(i)).toArray(t);
                SqlBasicCall sqlNode = new SqlBasicCall(((SqlBasicCall) valueClause).getOperator(),
                    t, SqlParserPos.ZERO);
                newValueClauseNodes.add(sqlNode);
                i++;
            }
            SqlBasicCall newValueClause = new SqlBasicCall(((SqlBasicCall) operandList.get(2)).getOperator(),
                newValueClauseNodes.toArray(new SqlNode[newValueClauseNodes.size()]), SqlParserPos.ZERO);

            SqlInsert sqlInsert = new SqlInsert(oldInsert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                operandList.get(1),
                newValueClause,
                new SqlNodeList(columnList, operandList.get(3).getParserPosition()),
                hasUpdateAttribute ? new SqlNodeList(newUpdateList, operandList.get(4).getParserPosition()) :
                    (SqlNodeList) operandList.get(4),
                oldInsert.getBatchSize(),
                oldInsert.getHints());
            return sqlInsert;
        } else if (flag[0] == 1) {
            // new insert..select statement
            ((SqlSelect) oldInsert.getOperandList().get(2))
                .setSelectList(new SqlNodeList(newSelectList, SqlParserPos.ZERO));
            SqlInsert sqlInsert = new SqlInsert(oldInsert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                operandList.get(1),
                operandList.get(2),
                new SqlNodeList(columnList, operandList.get(3).getParserPosition()),
                hasUpdateAttribute ? new SqlNodeList(newUpdateList, operandList.get(4).getParserPosition()) :
                    (SqlNodeList) operandList.get(4),
                oldInsert.getBatchSize(),
                oldInsert.getHints());
            return sqlInsert;
        } else {
            return oldInsert;
        }
    }

    public Map<SqlNode, RexNode> getRexInfoFromPartition(SqlPartitionBy sqlPartitionBy,
                                                         final PlannerContext plannerContext) {
        final RelOptCluster cluster = createRelOptCluster(plannerContext);
        final SqlToRelConverter sqlToRelConverter = new TddlSqlToRelConverter(null,
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            plannerContext);
        return sqlToRelConverter.getRexInfoFromPartition(sqlPartitionBy);
    }

    public Map<SqlNode, RexNode> convertPartition(SqlPartitionBy sqlPartitionBy,
                                                  final PlannerContext plannerContext) {
        final RelOptCluster cluster = createRelOptCluster(plannerContext);
        final SqlToRelConverter sqlToRelConverter = new TddlSqlToRelConverter(null,
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            plannerContext);
        return sqlToRelConverter.convertPartition(sqlPartitionBy);
    }

    public Map<SqlNode, RexNode> getRexInfoFromSqlAlterSpec(SqlNode parentNode,
                                                            List<SqlAlterSpecification> sqlAlterSpecifications,
                                                            final PlannerContext plannerContext) {
        final RelOptCluster cluster = createRelOptCluster(plannerContext);
        final SqlToRelConverter sqlToRelConverter = new TddlSqlToRelConverter(null,
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            plannerContext);
        validate(parentNode);
        return sqlToRelConverter.getRexInfoFromSqlAlterSpec(sqlAlterSpecifications);
    }

    public List<RexNode> getRexForGeneratedColumn(RelDataType rowType, List<SqlCall> sqlCalls,
                                                  final PlannerContext plannerContext) {
        final RelOptCluster cluster = createRelOptCluster(plannerContext);
        final SqlToRelConverter sqlToRelConverter = new TddlSqlToRelConverter(null,
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            plannerContext);
        return sqlToRelConverter.getRexForGeneratedColumn(rowType, sqlCalls);
    }

    public RexNode getRexForDefaultExpr(RelDataType rowType, SqlCall sqlCall,
                                        final PlannerContext plannerContext) {
        final RelOptCluster cluster = createRelOptCluster(plannerContext);
        final SqlToRelConverter sqlToRelConverter = new TddlSqlToRelConverter(null,
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            plannerContext);
        return sqlToRelConverter.getRexForDefaultExpr(rowType, sqlCall);
    }

    public SqlCall getValidatedSqlCallForGeneratedColumn(String tableName, SqlCall sqlCall) {
        // TODO(qianjing): check here
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        String alias = SqlUtil.deriveAliasFromOrdinal(0);
        selectList.add(SqlValidatorUtil.addAlias(sqlCall, alias));
        SqlNode sourceTable = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
            null, null, null, null, null, null, null);
        validator.validate(sqlSelect);
        return (SqlCall) ((SqlCall) (sqlSelect.getSelectList().get(0))).getOperandList().get(0);
    }
}

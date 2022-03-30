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

package com.alibaba.polardbx.optimizer.hint;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.hint.DirectlyRouteCondition;
import com.alibaba.polardbx.common.model.hint.ExtraCmdRouteCondition;
import com.alibaba.polardbx.common.model.hint.FullRouteCondition;
import com.alibaba.polardbx.common.model.hint.RouteCondition;
import com.alibaba.polardbx.common.model.hint.RuleRouteCondition;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.schema.RootSchemaFactory;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.TddlJavaTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.planner.rule.RuleToUse;
import com.alibaba.polardbx.optimizer.core.rel.AffectedRowsSum;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ExecutionPlanPropertiesVisitor;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyViewUnion;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceLogicalTableNameWithPhysicalTableNameVisitor;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyDal;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropDatabase;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.alibaba.polardbx.optimizer.hint.operator.BaseHintOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator.CmdBean;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushdownOperator;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter.HintCollection;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.hint.visitor.HintCollectVisitor;
import com.alibaba.polardbx.optimizer.hint.visitor.HintRelVisitor;
import com.alibaba.polardbx.optimizer.hint.visitor.HintRelVisitor.HintTableFinder;
import com.alibaba.polardbx.optimizer.hint.visitor.HintRelVisitor.PushdownHandlerVisitor;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.hint.SimpleHintParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils.TableProperties;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TABLE_EMPTY_WITH_HINT;
import static org.apache.calcite.sql.SqlUtil.stripAs;

/**
 * @author chenmo.cm
 */
public class HintPlanner extends TddlSqlToRelConverter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public HintPlanner(SqlValidatorImpl validator, CalciteCatalogReader catalog,
                       RelOptCluster cluster, Config converterConfig) {
        super(null,
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            PlannerContext.EMPTY_CONTEXT);
    }

    public static HintPlanner getInstance(String schemaName, ExecutionContext ec) {
        CalciteCatalogReader catalog;
        TddlTypeFactoryImpl typeFactory;
        RelOptCluster cluster;
        Config converterConfig;

        typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        SqlParser.Config parserConfig = SqlParser.configBuilder()
            .setLex(Lex.MYSQL)
            .setParserFactory(SqlParserImpl.FACTORY)
            .build();
        converterConfig = SqlToRelConverter.configBuilder()
            .withConvertTableAccess(false)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withExpand(false)
            .build();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
        CalciteSchema calciteSchema = RootSchemaFactory.createRootSchema(schemaName, ec);
        catalog = new CalciteCatalogReader(calciteSchema,
            calciteSchema.path(schemaName),
            new TddlJavaTypeFactoryImpl(),
            connectionConfig);

        VolcanoPlanner planner = new VolcanoPlanner(DrdsRelOptCostImpl.FACTORY, Contexts.EMPTY_CONTEXT);
        if (ec.isEnableRuleCounter()) {
            planner.setRuleCounter();
        }
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(DrdsRelMetadataProvider.INSTANCE);

        TddlOperatorTable opTab = TddlOperatorTable.instance();
        TddlValidator validator = new TddlValidator(opTab, catalog, typeFactory, SqlConformanceEnum.DEFAULT);
        validator.setDefaultNullCollation(NullCollation.LOW);
        validator.setIdentifierExpansion(false);
        validator.setCallRewrite(false);

        return new HintPlanner(validator, catalog, cluster, converterConfig);
    }

    /**
     * !!!!!FOR TEST ONLY!!!!
     */
    public ExecutionPlan getPlan(SqlNode ast) {
        // FIXME: support plan cache
        Map<Integer, ParameterContext> param = null;
        PlannerContext plannerContext = new PlannerContext();
        plannerContext.setExecutionContext(new ExecutionContext());
        return getPlan(ast, plannerContext, plannerContext.getExecutionContext());
    }

    public ExecutionPlan getPlan(SqlNode ast, PlannerContext plannerContext, ExecutionContext ec) {
        Map<Integer, ParameterContext> param = plannerContext.getParams().getCurrentParameter();
        // 使用现有优化器生成逻辑查询计划
        ExecutionPlan executionPlan = getOriginLogicalPlan(ast, plannerContext);
        // 找到所有 LogicalView
        final HintRelVisitor hintRelVisitor = new HintRelVisitor();
        executionPlan.getPlan().accept(hintRelVisitor);
        final List<LogicalView> logicalViews = hintRelVisitor.getLogicalViews();
        final LogicalJoin join = hintRelVisitor.getLogicalJoin();
        final Map<RelNode, SqlNodeList> relHintsMap = hintRelVisitor.getRelNodeHints();

        /**
         * handle pushdown
         */
        RelNode relResult = executionPlan.getPlan();

        final boolean singleLv = logicalViews.size() == 1;
        final boolean singleHint = relHintsMap.size() == 1;
        if (singleLv || singleHint) {
            final SqlNodeList hints = singleLv ? logicalViews.get(0).getHints() : relHintsMap.entrySet()
                .iterator()
                .next()
                .getValue();

            if (null == hints || hints.size() <= 0) {
                return executionPlan;
            }

            final List<HintPushdownOperator> pushdowns = new LinkedList<>();
            final HintCollection hintCollection = HintConverter.convertPushdown(hints, pushdowns, ec);
            if (hintCollection.pushdownOnly()) {
                return executionPlan;
            }
        }

        /**
         * handle push_xxx and add_xxx
         */
        if (relResult instanceof LogicalView) {
            relResult = handleLogicalView((LogicalView) relResult, param, ec);
            return buildLogicalPlan(ast, relResult, executionPlan);
        } else if (relResult instanceof Gather || relResult instanceof MergeSort) {
            RelNode newRelNode = relResult.getInput(0);
            newRelNode = handleLogicalView((LogicalView) newRelNode, param, ec);

            if (newRelNode == relResult.getInput(0)) {
                return buildLogicalPlan(ast, relResult, executionPlan);
            } else {
                return buildLogicalPlan(ast, newRelNode, executionPlan);
            }
        }

        if (singleLv) {

            final SqlNodeList hints = logicalViews.get(0).getHints();

            final List<HintPushdownOperator> pushdowns = new LinkedList<>();
            HintCollection hintCollection = HintConverter.convertPushdown(hints, pushdowns, ec);
            if (hintCollection.pushOnly()) {
                handlePushHint(logicalViews.get(0), param, ec);
                return executionPlan;
            } else {
                relResult = handleLogicalView(logicalViews.get(0), param, ec);
                return buildLogicalPlan(ast, relResult, executionPlan);
            }

        }

        //
        // if (join != null && join.getHints().size() > 0) {
        // // handle join
        //
        // relResult = buildRelNode(join, join.getHints(), ast);
        //
        // // handle logicalView
        // for (LogicalView logicalView : logicalViews) {
        // handleLogicalView(logicalView, param);
        // }
        //
        // return buildLogicalPlan(relResult);
        // }

        /**
         * handle push_xxx
         */
        for (LogicalView logicalView : logicalViews) {
            final SqlNodeList hints = logicalView.getHints();
            if (hints == null || hints.size() <= 0) {
                continue;
            }
            final List<HintPushdownOperator> pushdowns = new LinkedList<>();
            HintCollection hintCollection = HintConverter.convertPushdown(hints, pushdowns, ec);
            if (hintCollection.pushOnly()) {
                handlePushHint(logicalView, param, ec);
            }
        }

        return executionPlan;
    }

    public HintCollection collectAndPreExecute(SqlNode ast, CmdBean cmdBean, boolean testMode, ExecutionContext ec) {

        HintCollectVisitor cmdVisitor = new HintCollectVisitor(testMode, ec);
        ast.accept(cmdVisitor);

        List<HintCmdOperator> cmdOperators = cmdVisitor.getCmdOperator();

        for (HintCmdOperator op : cmdOperators) {
            // add connection properties to ExecutionContext
            op.handle(cmdBean);
        }

        return cmdVisitor.getCollection();
    }

    public ExecutionPlan direct(SqlNode ast, CmdBean cmdBean, HintCollection hintCollection,
                                Map<Integer, ParameterContext> param, String schemaName, ExecutionContext ec) {
        // init group
        List<String> finalGroups;
        if (cmdBean.jsonHint()) {
            // JSON HINT
            RouteCondition rc =
                SimpleHintParser.convertHint2RouteCondition(schemaName, SimpleHintParser.TDDL_HINT_PREFIX
                        + cmdBean.getJson()
                        + SimpleHintParser.TDDL_HINT_END,
                    param);
            cmdBean.getExtraCmd().putAll(rc.getExtraCmds());

            if (rc instanceof DirectlyRouteCondition) {
                hintCollection.routeCount++;
                final DirectlyRouteCondition drc = (DirectlyRouteCondition) rc;
                final List<String> groups = HintUtil.splitAndTrim(drc.getDbId(), ",");
                finalGroups = ImmutableList.copyOf(groups);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "unsupport HINT type "
                    + rc.getClass().getName()
                    + " for direct plan!");
            }
        } else {
            // NODE
            finalGroups = cmdBean.getGroups();
        }

        final SqlConverter converter = SqlConverter.getInstance(schemaName, ec);
        final RelOptCluster cluster = converter.createRelOptCluster(null);
        final List<Integer> dynamicParamIndex = PlannerUtils.getDynamicParamIndex(ast);

        List<RelNode> results = new ArrayList<>();
        for (String group : finalGroups) {
            PhyQueryOperation phyQueryOperation = new PhyQueryOperation(cluster,
                RelTraitSet.createEmpty(),
                ast,
                group,
                param,
                dynamicParamIndex);
            phyQueryOperation.setKind(ast.getKind());
            results.add(phyQueryOperation);
        } // end of for

        final ExecutionPlanPropertiesVisitor logicalPlanPropertiesVisitor = new ExecutionPlanPropertiesVisitor();
        ast.accept(logicalPlanPropertiesVisitor);

        final List<String> modifiedTableNames = logicalPlanPropertiesVisitor.getModifiedTableNames();

        boolean pushdownHintOnGsi = false;
        if (cmdBean.getExtraCmd().containsKey(ConnectionProperties.PUSHDOWN_HINT_ON_GSI)) {
            pushdownHintOnGsi =
                Boolean.valueOf(cmdBean.getExtraCmd().get(ConnectionProperties.PUSHDOWN_HINT_ON_GSI).toString());
        }

        List<TableProperties> tableModified = null;
        if (ast.getKind().belongsTo(SqlKind.DML)) {
            if (!pushdownHintOnGsi) {
                tableModified = checkModifyGsiDirectly(schemaName, modifiedTableNames, ec);
            } else {
                tableModified = getModifiedTable(schemaName, modifiedTableNames, ec);
            }
        }

        final BitSet planProperties = logicalPlanPropertiesVisitor.appendPlanProperties(null, null, ec);

        final ExecutionPlan result = new ExecutionPlan(ast, wrapWithViewUnion(results), null, planProperties);
        result.setModifiedTables(tableModified);

        return result;
    }

    public <T> List<T> getLogicalTable(String schemaName, List<String> phyTables,
                                       BiFunction<String, TableMeta, T> checker, ExecutionContext ec) {
        return getLogicalTable(schemaName, phyTables, checker, false, ec);
    }

    public <T> List<T> getLogicalTable(String schemaName, List<String> phyTables,
                                       BiFunction<String, TableMeta, T> checker, boolean allowDuplicate,
                                       ExecutionContext ec) {
        final List<T> result = new ArrayList<>();
        final Set<String> currentTableNames = new HashSet<>();

        List<String> untested = new ArrayList<>();

        SchemaManager schemaManager = ec.getSchemaManager(schemaName);
        PartitionInfoManager partitionInfoManager = schemaManager.getTddlRuleManager().getPartitionInfoManager();
        phyTables.forEach(phyTable -> {
            final TableRule tableRule = schemaManager.getTddlRuleManager().getTableRule(phyTable);
            if (null != tableRule || partitionInfoManager.isNewPartDbTable(phyTable)) {
                final TableMeta table = schemaManager.getTable(phyTable);

                if (allowDuplicate || !currentTableNames.contains(table.getTableName())) {
                    final T resultItem = checker.apply(phyTable, table);

                    if (null != resultItem) {
                        result.add(resultItem);
                        currentTableNames.add(table.getTableName());
                    }
                }
            } else {
                untested.add(phyTable);
            }
        });

        if (!untested.isEmpty()) {

            Collection<TableRule> tables =
                OptimizerContext.getContext(schemaName).getRuleManager().getTddlRule().getTables();

            tables = tables.stream().filter(rule ->
                schemaManager.getTable(rule.getVirtualTbName()).isGsi()
                    || schemaManager.getTable(rule.getVirtualTbName()).withGsi()).collect(Collectors.toSet());

            Map<String, Set<String>> logicalTableMap = buildLogicalTableMap(tables);

            Set<String> partitionTables = partitionInfoManager.getPartitionTables();
            if (!partitionTables.isEmpty()) {
                partitionTables = partitionTables.stream().filter(tableName ->
                    schemaManager.getTable(tableName).isGsi()
                        || schemaManager.getTable(tableName).withGsi()).collect(Collectors.toSet());

                for (String partitionTableName : partitionTables) {
                    PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(partitionTableName);
                    Map<String, Set<String>> topology = partitionInfo.getTopology();
                    logicalTableMap
                        .computeIfAbsent(partitionTableName, s -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
                    for (Set<String> phyTableNames : topology.values()) {
                        logicalTableMap.get(partitionTableName).addAll(phyTableNames);
                    }
                }
            }

            untested.forEach(phyTable -> logicalTableMap.entrySet()
                .stream()
                .filter(e -> e.getValue().contains(phyTable))
                .findAny()
                .map(entry -> ec.getSchemaManager(schemaName).getTable(entry.getKey()))
                .filter(table -> allowDuplicate || !currentTableNames.contains(table.getTableName()))
                .map(table -> {
                    final T resultItem = checker.apply(phyTable, table);
                    if (null != resultItem) {
                        result.add(resultItem);
                        currentTableNames.add(table.getTableName());
                    }
                    return table;
                }));
        }

        return result;
    }

    private Map<String, Set<String>> buildLogicalTableMap(Collection<TableRule> tables) {
        Map<String, Set<String>> logicalTableMap = TreeMaps.caseInsensitiveMap();
        for (TableRule tableRule : tables) {
            final String virtualTbName = tableRule.getVirtualTbName();
            logicalTableMap.computeIfAbsent(virtualTbName, s -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
            for (Set<String> actualTables : tableRule.getActualTopology().values()) {
                logicalTableMap.get(virtualTbName).addAll(actualTables);
            }
        }
        return logicalTableMap;
    }

    public List<TableProperties> checkModifyGsiDirectly(final String schemaName,
                                                        final List<String> modifiedTableNames, ExecutionContext ec) {
        return getLogicalTable(schemaName, modifiedTableNames, (phyTable, tableMeta) -> {
            if (tableMeta.isGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY,
                    phyTable);
            } else if (tableMeta.withGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_PRIMARY_TABLE_DIRECTLY,
                    phyTable);
            }

            return new TableProperties(tableMeta.getTableName(), schemaName, ec);
        }, ec);
    }

    public List<TableProperties> getModifiedTable(final String schemaName, final List<String> modifiedTableNames,
                                                  ExecutionContext ec) {
        final List<TableMeta> logicalTables = getLogicalTable(schemaName,
            modifiedTableNames,
            (phyTable, tableMeta) -> tableMeta, ec);

        return logicalTables.stream()
            .map(tableMeta -> new TableProperties(tableMeta.getTableName(), schemaName, ec))
            .collect(Collectors.toList());
    }

    private RelNode wrapWithViewUnion(List<RelNode> results) {
        RelNode relResult;
        if (results.size() > 1) {
            relResult = PhyViewUnion.create(results);
        } else if (results.size() == 1) {
            relResult = results.get(0);
        } else {
            throw new NotSupportException("None group remained to push sql!");
        }
        return relResult;
    }

    public ExecutionPlan pushdown(ExecutionPlan executionPlan, SqlNode ast, CmdBean cmdBean,
                                  HintCollection hintCollection,
                                  Map<Integer, ParameterContext> param, Map<String, Object> extraCmd,
                                  ExecutionContext ec) {
        ExecutionPlan result = executionPlan;
        String schemaName = cmdBean.getSchemaName();
        if (executionPlan.getPlan() instanceof BaseDdlOperation) {
            schemaName = ((BaseDdlOperation) executionPlan.getPlan()).getSchemaName();
        } else if (executionPlan.getPlan() instanceof LogicalShow) {
            schemaName = ((LogicalShow) executionPlan.getPlan()).getSchemaName();
        }

        if (cmdBean.doPushdown()) {
            final RelNode origin = executionPlan.getPlan();

            SqlNode realAst = ast;
            if (ast.getKind() == SqlKind.EXPLAIN) {
                realAst = ((SqlExplain) ast).getExplicandum();
            }

            final HintTableFinder tableFinder = new HintTableFinder();
            RelOptUtil.go(tableFinder, origin);

            boolean noTable = tableFinder.noTable();
            List<String> finalGroups = new ArrayList<>();

            List<String> tableNames = new ArrayList<>();
            RelNode pushed = null;
            if (!noTable) {
                if (origin instanceof LogicalModifyView) { // pushed down dml
                    pushed = origin;
                    tableNames = ((LogicalModifyView) origin).getTableNames();
                } else if (origin instanceof LogicalModify) {
                    LogicalView lv =
                        new LogicalView(origin, tableFinder.getTable(), new SqlNodeList(SqlParserPos.ZERO));
                    lv.getPushDownOpt().setNativeSqlNode(realAst);
                    lv.setSqlTemplate(realAst);

                    final LogicalModifyView logicalModifyView = new LogicalModifyView(lv);
                    logicalModifyView.push(origin);
                    RelUtils.changeRowType(logicalModifyView, origin.getRowType());

                    tableNames = lv.getTableNames();
                    pushed = logicalModifyView;
                } else if (origin instanceof LogicalInsert) {

                    LogicalInsert logicalInsert = (LogicalInsert) origin;
                    tableNames = ImmutableList.of(Util.last(logicalInsert.getTable().getQualifiedName()));

                    // If it's INSERT SELECT, change LogicalInsert to
                    // LogicalModifyView.
                    if (logicalInsert.isSourceSelect()) {
                        LogicalView lv = new LogicalView(origin,
                            tableFinder.getTable(),
                            new SqlNodeList(SqlParserPos.ZERO));

                        LogicalModifyView lmv = new LogicalModifyView(lv);
                        lmv.getPushDownOpt().setNativeSqlNode(realAst);
                        lmv.setSqlTemplate(realAst);

                        tableNames = lmv.getTableNames();
                        pushed = lmv;
                    } else {
                        pushed = origin;
                    }

                } else if (origin instanceof BaseDdlOperation) {
                    tableNames = Lists.newArrayList(((BaseDdlOperation) origin).getTableName());
                    finalGroups = new ArrayList<>();
                } else {
                    LogicalView lv =
                        new LogicalView(origin, tableFinder.getTable(), new SqlNodeList(SqlParserPos.ZERO));
                    lv.getPushDownOpt().setNativeSqlNode(realAst);
                    lv.setSqlTemplate(realAst);

                    tableNames = lv.getTableNames();
                    pushed = lv;
                } // end of else
            } // end of if

            /**
             * build target table
             */
            Map<String, List<List<String>>> targetTable = new LinkedHashMap<>();

            if (cmdBean.jsonHint()) {
                /**
                 * JSON HINT
                 */
                RouteCondition rc = SimpleHintParser.convertHint2RouteCondition(schemaName,
                    SimpleHintParser.TDDL_HINT_PREFIX
                        + cmdBean.getJson()
                        + SimpleHintParser.TDDL_HINT_END, param);
                cmdBean.getExtraCmd().putAll(rc.getExtraCmds());

                if (rc instanceof DirectlyRouteCondition) {
                    hintCollection.routeCount++;
                    finalGroups = handleDirectlyRouteCondition(tableNames, targetTable, (DirectlyRouteCondition) rc);
                } else if (rc instanceof FullRouteCondition) {
                    hintCollection.routeCount++;
                    finalGroups = handleFullRouteCondition(tableNames,
                        cmdBean.getCondition(),
                        param,
                        targetTable,
                        (FullRouteCondition) rc,
                        schemaName, ec);
                } else if (rc instanceof RuleRouteCondition) {
                    hintCollection.routeCount++;
                    finalGroups = handleRuleRouteCondition(schemaName, tableNames, param, targetTable,
                        (RuleRouteCondition) rc, ec);
                } else if (rc instanceof ExtraCmdRouteCondition) {
                    // just add extra cmd
                    return executionPlan;
                }
            } else {
                /**
                 * CoronaDB HINT
                 */
                if (cmdBean.logicalTableSpecified()) {
                    if (cmdBean.realTableSpecified() && !noTable) {
                        final List<String> vtNames = HintUtil.splitAndTrim(cmdBean.getTable(), ",");
                        final List<List<String>> realTables = new LinkedList<>();
                        for (List<String> rtNames : cmdBean.getRealTable()) {
                            List<String> phyTables = HintUtil.mergeTableNames(tableNames, vtNames, rtNames);
                            realTables.add(phyTables);
                        }

                        for (String group : cmdBean.getGroups()) {

                            targetTable.put(group, realTables);
                        }

                        finalGroups = cmdBean.getGroups();
                    } else if (cmdBean.groupSpecified()) {
                        Map<String, List<List<String>>> tmpTargetTable = null;
                        if (noTable) {
                            Map<String, Map<String, Comparative>> comparatives =
                                BaseHintOperator.buildComparative(cmdBean.getTable(),
                                    cmdBean.getCondition(),
                                    new LinkedList<Integer>(),
                                    schemaName, ec);
                            tableNames.addAll(comparatives.keySet());

                            /**
                             * tmpTargetTable = buildTargetTables(tableNames,
                             * comparatives, param, schemaName);
                             */

                            tmpTargetTable =
                                HintUtil.buildTargetTables(tableNames, comparatives, param, schemaName, ec);

                        } else {
                            Map<String, Map<String, Comparative>> comparatives = new HashMap<>();
                            for (String tableName : tableNames) {
                                comparatives.put(tableName, new HashMap<String, Comparative>());
                            }

                            /**
                             * tmpTargetTable = buildTargetTables(tableNames,
                             * comparatives, param, schemaName);
                             */

                            tmpTargetTable =
                                HintUtil.buildTargetTables(tableNames, comparatives, param, schemaName, ec);

                        }

                        for (String group : cmdBean.getGroups()) {
                            if (tmpTargetTable.containsKey(group)) {
                                targetTable.put(group, tmpTargetTable.get(group));
                            }
                        }
                        finalGroups.addAll(cmdBean.getGroups());
                    } else {

                        finalGroups = HintUtil.allGroup();
                        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                            Map<String, Map<String, Comparative>> comparatives =
                                BaseHintOperator.buildComparative(cmdBean.getTable(),
                                    cmdBean.getCondition(),
                                    new LinkedList<Integer>(),
                                    schemaName, ec);
                            if (noTable) {
                                tableNames.addAll(comparatives.keySet());

                                /**
                                 * targetTable = buildTargetTables(tableNames,
                                 * comparatives, param, schemaName); } else {
                                 * targetTable = buildTargetTables(tableNames,
                                 * comparatives, param,
                                 * OptimizerContext.getContext().getSchemaName());
                                 */

                                targetTable =
                                    HintUtil.buildTargetTables(tableNames, comparatives, param, schemaName, ec);
                            } else {
                                targetTable =
                                    HintUtil.buildTargetTables(tableNames, comparatives, param, schemaName, ec);

                            }
                        } else {
                            Map<String, PartitionPruneStep> pruneStepMap =
                                BaseHintOperator.buildPartitionPruneStepMap(cmdBean.getTable(),
                                    cmdBean.getCondition(),
                                    new LinkedList<Integer>(),
                                    schemaName, ec);
                            if (noTable) {
                                tableNames.addAll(pruneStepMap.keySet());

                                /**
                                 * targetTable = buildTargetTables(tableNames,
                                 * comparatives, param, schemaName); } else {
                                 * targetTable = buildTargetTables(tableNames,
                                 * comparatives, param,
                                 * OptimizerContext.getContext().getSchemaName());
                                 */

                                targetTable =
                                    HintUtil.buildTargetTablesByPruneStepMap(tableNames, pruneStepMap, schemaName, ec);
                            } else {
                                targetTable =
                                    HintUtil.buildTargetTablesByPruneStepMap(tableNames, pruneStepMap, schemaName, ec);

                            }
                        }

                    }
                } else {
                    finalGroups = cmdBean.getGroups();

                    if (!noTable) {
                        finalGroups = cmdBean.getGroups();
                        final TddlRuleManager rule = OptimizerContext.getContext(schemaName).getRuleManager();
                        if (tableNames.size() == 1 && rule.isBroadCast(tableNames.get(0))) {

                            String logTbName = tableNames.get(0);
                            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                                // broadcast table only
                                final String physicalTableName = rule.getTableRule(logTbName).getTbNamePattern();
                                for (String group : cmdBean.getGroups()) {
                                    targetTable.put(group, ImmutableList.of(Lists.newArrayList(physicalTableName)));
                                }
                            } else {
                                // broadcast table only

                                Map<String, List<PhysicalPartitionInfo>> phyTblInfos =
                                    rule.getPartitionInfoManager().getPartitionInfo(logTbName)
                                        .getPhysicalPartitionTopology(null);
                                for (String group : cmdBean.getGroups()) {
                                    String phyTblName = phyTblInfos.get(group).get(0).getPhyTable();
                                    targetTable.put(group, ImmutableList.of(Lists.newArrayList(phyTblName)));
                                }
                            }

                        } else {
                            final Map<String, List<List<String>>> tmpTargetTable =
                                fullTableScan(tableNames, schemaName, ec);
                            for (String group : cmdBean.getGroups()) {
                                if (tmpTargetTable.containsKey(group)) {
                                    targetTable.put(group, tmpTargetTable.get(group));
                                }
                            }
                        }
                    }
                }
            } // end of else
            if (noTable || origin instanceof BaseDdlOperation) {
                // DDL or SQL without table (I.e. DirectTableOperation)
                result = handleSqlWithoutTable(executionPlan, ast, param, origin, finalGroups, targetTable, tableNames,
                    schemaName, ec);
            } else if (ast.getKind() == SqlKind.EXPLAIN) {
                // pushdown explain
                if (pushed instanceof LogicalView) {
                    final LogicalView lv = (LogicalView) pushed;
                    lv.setTargetTables(targetTable);

                    ExecutionContext executionContext = new ExecutionContext();
                    if (schemaName != null) {
                        executionContext.setSchemaName(schemaName);
                    }
                    executionContext.setParams(new Parameters(param, false));
                    executionContext.setExtraCmds(extraCmd);
                    final List<RelNode> inputs = lv.getInput(executionContext);
                    final List<RelNode> results = new ArrayList<>();
                    for (RelNode input : inputs) {
                        PhyTableOperation phyTableOperation = (PhyTableOperation) input;
                        SqlNode oriSqlNode = phyTableOperation.getNativeSqlNode();

                        SqlExplain explainSqlNode = (SqlExplain) ast.clone(SqlParserPos.ZERO);
                        explainSqlNode.setExplicandum(oriSqlNode);
                        PhyQueryOperation phyQueryOperation = new PhyQueryOperation(phyTableOperation.getCluster(),
                            phyTableOperation.getTraitSet(),
                            explainSqlNode,
                            phyTableOperation.getDbIndex(),
                            phyTableOperation.getParam());
                        results.add(phyQueryOperation);
                    }

                    result = new ExecutionPlan(ast, wrapWithViewUnion(results), executionPlan.getCursorMeta());
                } else {
                    throw new NotSupportException("Not support HINT + EXPLAIN for sql type other than SELECT");
                }
            } else {
                boolean dmlPushed = false;

                if (targetTable != null && targetTable.isEmpty()) {
                    //目标表个数为0，运行容易产生歧义
                    throw new TddlRuntimeException(ERR_TABLE_EMPTY_WITH_HINT);
                }

                // DML, DQL
                if (pushed instanceof LogicalModifyView) {
                    ((LogicalModifyView) pushed).setTargetTables(targetTable);
                    dmlPushed = true;
                } else if (pushed instanceof LogicalInsert) {
                    ((LogicalInsert) pushed).setTargetTables(targetTable);
                    dmlPushed = true;
                } else {
                    ((LogicalView) pushed).setTargetTables(targetTable);
                }

                /**
                 * add MergeSort/Gather
                 */
                RelNode relNode = pushed;
                if (pushed instanceof LogicalInsert) {
                    // do nothing
                } else if (targetTable.size() > 1
                    || (targetTable.size() == 1 && targetTable.entrySet().iterator().next().getValue().size() > 1)) {
                    if (origin instanceof Sort) {
                        Sort sort = (Sort) origin;
                        if (null == sort.getCollation() || sort.getCollation().getFieldCollations().isEmpty()) {
                            relNode = Gather.create(relNode);
                        } else {
                            relNode = MergeSort.create(relNode, sort.getCollation(), null, null);
                        }
                    } else if (origin instanceof MppExchange) {
                        relNode = MppExchange.create(relNode, ((MppExchange) origin).getCollation(),
                            ((MppExchange) origin).getDistribution());
                    } else if (pushed instanceof LogicalModifyView) {
                        relNode = AffectedRowsSum.create(pushed, true);
                    } else {
                        relNode = Gather.create(relNode);
                    }
                }

                result = new ExecutionPlan(ast, relNode, executionPlan.getCursorMeta());

                if (dmlPushed) {
                    final List<String> phyTables = targetTable.values().stream().flatMap(
                        value -> value.stream().flatMap(Collection::stream)).collect(Collectors.toList());

                    final List<TableProperties> modifiedTable = getModifiedTable(schemaName, phyTables, ec);
                    result.setModifiedTables(modifiedTable);
                }
            }
        } // end of if

        /**
         * keep original plan properties
         */
        result.setPlanProperties(executionPlan.getPlanProperties());
        result.setUsePostPlanner(false);

        return result;
    }

    public static Map<String, List<List<String>>> fullTableScan(List<String> tableNames, String schemaName,
                                                                ExecutionContext ec) {
        PartitionInfoManager partitionInfoManager =
            ec.getSchemaManager(schemaName).getTddlRuleManager().getPartitionInfoManager();

        if (tableNames.stream().allMatch(x -> partitionInfoManager.isNewPartDbTable(x))) {

            List<PartPrunedResult> allTbPrunedResults = new ArrayList<>();
            for (int i = 0; i < tableNames.size(); i++) {
                PartitionPruneStep pruneStepInfo = PartitionPruneStepBuilder.generateFullScanPruneStepInfo(schemaName,
                    tableNames.get(i), ec);
                PartPrunedResult tbPrunedResult = PartitionPruner.doPruningByStepInfo(pruneStepInfo, ec);
                allTbPrunedResults.add(tbPrunedResult);
            }
            return PartitionPrunerUtils.buildTargetTablesByPartPrunedResults(allTbPrunedResults);
        } else {
            Map<String, List<List<String>>> tmpTargetTable;
            Map<String, Map<String, Comparative>> comparatives = new HashMap<>();
            for (String tableName : tableNames) {
                comparatives.put(tableName, new HashMap<String, Comparative>());
            }
            tmpTargetTable = HintUtil.buildTargetTables(tableNames,
                comparatives,
                ImmutableMap.<Integer, ParameterContext>of(),
                schemaName, ec);
            return tmpTargetTable;
        }
    }

    public ExecutionPlan handleSqlWithoutTable(ExecutionPlan executionPlan, SqlNode ast,
                                               Map<Integer, ParameterContext> param, RelNode origin,
                                               List<String> finalGroups, Map<String, List<List<String>>> targetTable,
                                               List<String> tableNames,
                                               String schemaName,
                                               ExecutionContext ec) {
        if (finalGroups.size() <= 0) {
            // no group specified
            throw new NotSupportException("execute node/scan HINT without group or table specified");
        }

        boolean single = (finalGroups.size() == 1);
        if (targetTable.isEmpty()) {
            for (String group : finalGroups) {
                targetTable.put(group, new LinkedList<List<String>>());
            }
        } else {
            single &= PlannerUtils.isSingle(targetTable);
        }

        if (origin instanceof BaseDdlOperation &&
            !(origin instanceof LogicalCreateDatabase || origin instanceof LogicalDropDatabase)) {
            ((BaseDdlOperation) origin).setTargetTablesHintCache(targetTable);
            return executionPlan.copy(origin);
        }

        if (origin instanceof BroadcastTableModify) {
            final DirectTableOperation directTableOperation = ((BroadcastTableModify) origin).getDirectTableOperation();
            final List<Integer> dynamicParamIndex = PlannerUtils.getDynamicParamIndex(ast);

            SqlNode clonedAst = ast.clone(SqlParserPos.ZERO);
            clonedAst = clonedAst.accept(new ReplaceLogicalTableNameWithPhysicalTableNameVisitor(schemaName, ec));

            List<RelNode> results = new ArrayList<>();
            for (String group : finalGroups) {
                PhyQueryOperation phyQueryOperation = new PhyQueryOperation(directTableOperation.getCluster(),
                    directTableOperation.getTraitSet(),
                    clonedAst,
                    group,
                    param,
                    dynamicParamIndex);
                phyQueryOperation.setKind(ast.getKind());
                results.add(phyQueryOperation);
            } // end of for

            final ExecutionPlan resultPlan =
                new ExecutionPlan(ast, wrapWithViewUnion(results), executionPlan.getCursorMeta());
            // for acquire MDL
            resultPlan.setModifiedTables(executionPlan.getModifiedTables());
            return resultPlan;
        }

        if (single) {
            // only one group specified
            if (origin instanceof LogicalShow) {
                // LogicalShow show to PhyShow
                final LogicalShow logicalShow = (LogicalShow) origin;
                final PhyShow phyShow = PhyShow.create(logicalShow, targetTable, tableNames);

                phyShow.setTableNames(tableNames);

                return new ExecutionPlan(ast, phyShow, executionPlan.getCursorMeta());
            } else if (origin instanceof BaseDalOperation) {
                targetTable.put(finalGroups.get(0), new LinkedList<List<String>>());
                ((BaseDalOperation) origin).setTargetTable(targetTable);

                return new ExecutionPlan(ast, origin, executionPlan.getCursorMeta());
            } else if (origin instanceof DirectTableOperation) {
                return executionPlan;
            } else {
                SqlNode clonedAst = ast.clone(SqlParserPos.ZERO);
                clonedAst = clonedAst.accept(new ReplaceLogicalTableNameWithPhysicalTableNameVisitor(schemaName, ec));
                PhyTableOperation phyTableOperation = buildPhyTableOperation(executionPlan,
                    clonedAst,
                    param,
                    finalGroups.get(0));

                return new ExecutionPlan(ast, phyTableOperation, executionPlan.getCursorMeta());
            }
        } else {
            // multi group specified
            List<RelNode> resulRel = new LinkedList<>();
            if (origin instanceof LogicalShow) {
                // LogicalShow show to PhyShow
                final LogicalShow logicalShow = (LogicalShow) origin;

                PhyShow phyShow = PhyShow.create(logicalShow, targetTable, tableNames);

                resulRel.add(phyShow);
            } else if (origin instanceof LogicalDal) {
                // LogicalDal show to PhyDal
                final LogicalDal logicalDal = (LogicalDal) origin;

                PhyDal phyDal = PhyDal.create(logicalDal, targetTable, tableNames);
                phyDal.setTargetTable(targetTable);

                resulRel.add(phyDal);
            } else if (origin instanceof BaseDalOperation) {
                ((BaseDalOperation) origin).setTargetTable(targetTable);
                if (origin instanceof PhyShow) {
                    ((PhyShow) origin).setTableNames(tableNames);
                }

                resulRel.add(origin);
            } else {
                if (SqlKind.OPTIMIZE_TABLE == ast.getKind() && origin instanceof Gather) {
                    final PhyDal optimizeTable = (PhyDal) origin.getInputs().get(0);

                    final Map<String, List<List<String>>> optimizeTargets = new HashMap<>(targetTable.size());

                    targetTable.forEach((group, phyTables) -> optimizeTargets.put(group,
                        phyTables.stream()
                            .flatMap(phyTable -> phyTable.stream().map(ImmutableList::of))
                            .collect(Collectors.toList())));

                    optimizeTable.setTargetTable(optimizeTargets);

                    resulRel.add(optimizeTable);
                } else {
                    SqlNode clonedAst = ast.clone(SqlParserPos.ZERO);
                    clonedAst =
                        clonedAst.accept(new ReplaceLogicalTableNameWithPhysicalTableNameVisitor(schemaName, ec));
                    for (String group : finalGroups) {
                        PhyTableOperation phyTableOperation =
                            buildPhyTableOperation(executionPlan, clonedAst, param, group);

                        resulRel.add(phyTableOperation);
                    }
                    return new ExecutionPlan(ast, PhyViewUnion.create(resulRel), executionPlan.getCursorMeta());
                }
            }

            return new ExecutionPlan(ast, PhyViewUnion.create(resulRel), executionPlan.getCursorMeta());
        } // end of else
    }

    private List<String> handleRuleRouteCondition(String schemaName, List<String> tableNames,
                                                  Map<Integer, ParameterContext> param,
                                                  Map<String, List<List<String>>> targetTable, RuleRouteCondition rrc,
                                                  ExecutionContext ec) {
        final boolean noTable = (null == tableNames || tableNames.size() == 0);
        List<String> finalGroups = new LinkedList<>();

        if (noTable) {
            finalGroups = HintUtil.allGroup();
        } else {
            Map<String, Map<String, Comparative>> comparatives = new HashMap<>();

            List<String> vtNames = HintUtil.splitAndTrim(rrc.getVirtualTableName(), ",");

            for (String vTable : vtNames) {
                comparatives.put(vTable, rrc.getParameters());
            }

            /**
             * targetTable.putAll(buildTargetTables(tableNames, comparatives,
             * param, OptimizerContext.getContext().getSchemaName()));
             */
            targetTable.putAll(HintUtil.buildTargetTables(tableNames,
                comparatives,
                param,
                schemaName, ec));

            finalGroups.addAll(targetTable.keySet());
        }
        return finalGroups;
    }

    private List<String> handleFullRouteCondition(List<String> tableNames, String condition,
                                                  Map<Integer, ParameterContext> param,
                                                  Map<String, List<List<String>>> targetTable, FullRouteCondition frc,
                                                  String schemaName, ExecutionContext ec) {
        final boolean noTable = (null == tableNames || tableNames.size() == 0);
        List<String> finalGroups = new LinkedList<>();

        Map<String, Map<String, Comparative>> comparatives =
            BaseHintOperator.buildComparative(frc.getVirtualTableName(),
                condition,
                new LinkedList<Integer>(),
                schemaName, ec);

        if (noTable) {
            if (TStringUtil.isNotBlank(frc.getVirtualTableName())) {
                tableNames.addAll(HintUtil.splitAndTrim(frc.getVirtualTableName(), ","));

                /**
                 * targetTable.putAll(buildTargetTables(tableNames,
                 * comparatives, param, schemaName));
                 */

                targetTable.putAll(HintUtil.buildTargetTables(tableNames, comparatives, param, schemaName, ec));

                finalGroups = ImmutableList.copyOf(targetTable.keySet());
            } else {
                finalGroups = HintUtil.allGroup();
            }
        } else {

            /**
             * targetTable.putAll(buildTargetTables(tableNames, comparatives,
             * param, schemaName));
             */

            targetTable.putAll(HintUtil.buildTargetTables(tableNames, comparatives, param, schemaName, ec));

            finalGroups.addAll(targetTable.keySet());
        }

        return finalGroups;
    }

    private List<String> handleDirectlyRouteCondition(List<String> tableNames,
                                                      Map<String, List<List<String>>> targetTable,
                                                      DirectlyRouteCondition rc) {
        List<String> finalGroups = new LinkedList<>();

        final DirectlyRouteCondition drc = rc;
        final List<String> groups = HintUtil.splitAndTrim(drc.getDbId(), ",");
        final boolean noTable = (null == tableNames || tableNames.size() == 0);

        if (noTable) {
            finalGroups = ImmutableList.copyOf(groups);
        } else {

            if (null == drc.getTables() || drc.getTables().size() <= 0) {
                final List<String> vtName = HintUtil.splitAndTrim(drc.getVirtualTableName(), ",");

                if (null != vtName && vtName.size() > 0) {
                    // should never be here
                    for (String group : groups) {
                        targetTable.put(group, ImmutableList.<List<String>>of(ImmutableList.copyOf(vtName)));
                    }
                } else {
                    for (String group : groups) {
                        targetTable.put(group, ImmutableList.of(tableNames));
                    }
                }
            } else {
                final List<String> vtNames = HintUtil.splitAndTrim(drc.getVirtualTableName(), ",");
                for (String group : groups) {
                    List<List<String>> tables = new LinkedList<>();

                    for (String relTable : drc.getTables()) {
                        List<String> rtNames = HintUtil.splitAndTrim(relTable, ",");

                        List<String> phyTables = HintUtil.mergeTableNames(tableNames, vtNames, rtNames);
                        tables.add(phyTables);
                    } // end of for

                    targetTable.put(group, tables);
                } // end of for
            } // end of else

            finalGroups.addAll(targetTable.keySet());
        } // end of else
        return finalGroups;
    }

    private PhyTableOperation buildPhyTableOperation(ExecutionPlan executionPlan, SqlNode ast,
                                                     Map<Integer, ParameterContext> param, String group) {
        RelNode plan = executionPlan.getPlan();
        PhyTableOperation phyTableOperation =
            new PhyTableOperation(plan.getCluster(), plan.getTraitSet(), plan.getRowType(), null, plan);
        phyTableOperation.setDbIndex(group);
        phyTableOperation.setParam(param);
        String sql = RelUtils.toNativeSql(ast, DbType.MYSQL);
        phyTableOperation.setSqlTemplate(sql);
        return phyTableOperation;
    }

    private RelNode handlePushdown(RelNode originPlan, Map<RelNode, RelNode> relRootMap,
                                   Map<Integer, ParameterContext> param, ExecutionContext ec) {
        PushdownHandlerVisitor pushdownHandler = new PushdownHandlerVisitor(MapUtils.invertMap(relRootMap), param, ec);

        return originPlan.accept(pushdownHandler);
    }

    private ExecutionPlan buildLogicalPlan(SqlNode ast, RelNode relResult, ExecutionPlan defaultRel) {
        if (null == relResult) {
            return defaultRel;
        }

        String tableName = "OUTPUT";
        CursorMeta oriMeta = defaultRel.getCursorMeta();
        if (oriMeta.getColumns() != null && oriMeta.getColumns().size() > 0) {
            tableName = oriMeta.getColumnMeta(0).getTableName();
        }

        RelMetadataQuery mq = relResult.getCluster().getMetadataQuery();
        final CursorMeta cursorMeta = CursorMeta.build(
            CalciteUtils.buildColumnMeta(mq.getOriginalRowType(relResult), tableName));
        return new ExecutionPlan(ast, relResult, cursorMeta);
    }

    public RelNode handleLogicalView(LogicalView logicalView, Map<Integer, ParameterContext> param,
                                     ExecutionContext ec) {

        final SqlNodeList hints = logicalView.getHints();

        if (null == hints || hints.size() <= 0) {
            return logicalView;
        }

        final List<HintPushdownOperator> pushdowns = new LinkedList<>();
        HintCollection hintCollection = HintConverter.convertPushdown(hints, pushdowns, ec);
        if (hintCollection.pushdownOnly()) {
            return logicalView;
        }

        return handleLogicalView(logicalView, param, hints, pushdowns, ec);
    }

    private RelNode handleLogicalView(LogicalView logicalView, Map<Integer, ParameterContext> param, SqlNodeList hints,
                                      List<HintPushdownOperator> pushdowns, ExecutionContext ec) {
        SqlSelect nativeSqlNode = (SqlSelect) logicalView.getNativeSqlNode();
        nativeSqlNode = updateSqlNode(nativeSqlNode, hints, ec);

        // 补上 select * 和 count(*)
        nativeSqlNode = appendStarForSelect(nativeSqlNode);

        SqlSelect originNativeSql = (SqlSelect) nativeSqlNode.clone(SqlParserPos.ZERO);

        // validate
        SqlConverter converter = SqlConverter.getInstance(logicalView.getSchemaName(), ec);
        SqlNode validatedNode = converter.validate(nativeSqlNode);

        // logical plan
        RelNode rel = converter.toRel(validatedNode);

        // optimize logical plan with basic rules
        rel = optimizeLogicalPlan(rel);

        logicalView.getPushDownOpt().setNativeSqlNode(nativeSqlNode);
        logicalView.getPushDownOpt().getBuilder().clear();
        logicalView.getPushDownOpt().getBuilder().push(rel);
        logicalView.getPushDownOpt().setPlainRowType(rel.getRowType());

        // 重建 LogicalView 上层节点
        RelNode relResult = buildRelNode(logicalView, hints, nativeSqlNode, ec);

        logicalView.getPushDownOpt().setNativeSqlNode(originNativeSql);
        logicalView.setSqlTemplate(originNativeSql);

        if (pushdowns.size() <= 0 || TStringUtil.isBlank(pushdowns.get(0).condition)) {
            rebuildComparative(logicalView, param, ec);
        }

        return relResult;
    }

    protected ExecutionPlan getOriginLogicalPlan(SqlNode ast, PlannerContext plannerContext) {
        return Planner.getInstance().getPlan(ast, plannerContext);
    }

    public LogicalView handlePushHint(LogicalView logicalView, Map<Integer, ParameterContext> param,
                                      ExecutionContext ec) {
        final SqlSelect nativeSqlNode = (SqlSelect) logicalView.getNativeSqlNode();

        // update sql node
        SqlSelect newSqlNode = updateSqlNode(nativeSqlNode, logicalView.getHints(), ec);

        if (nativeSqlNode == newSqlNode) {
            return logicalView;
        }

        // append star in 'select *' and 'count(*)'
        newSqlNode = appendStarForSelect(newSqlNode);

        // validate
        SqlConverter converter =
            SqlConverter.getInstance(logicalView.getSchemaName(), plannerContext.getExecutionContext());
        SqlNode validatedNode = converter.validate(newSqlNode);

        // logical plan
        RelNode rel = converter.toRel(validatedNode);

        // optimize logical plan with basic rules
        rel = optimizeLogicalPlan(rel);

        logicalView.setSqlTemplate(newSqlNode);
        logicalView.getPushDownOpt().setNativeSqlNode(newSqlNode);
        logicalView.getPushDownOpt().getBuilder().clear();
        logicalView.getPushDownOpt().getBuilder().push(rel);
        logicalView.getPushDownOpt().setPlainRowType(rel.getRowType());

        rebuildComparative(logicalView, param, ec);

        return logicalView;
    }

    public static void rebuildComparative(LogicalView logicalView, Map<Integer, ParameterContext> param,
                                          ExecutionContext ec) {
        // calculate target tables
        Map<String, Map<String, Comparative>> comparative = new HashMap<>();
        ConditionExtractor.partitioningConditionFrom(logicalView).extract().allCondition(comparative, null, ec);
        logicalView.setComparativeHintCache(comparative);
    }

    public RelNode optimizeLogicalPlan(RelNode rel) {
        HepProgramBuilder hepPgmBuilder = getHepProgramBuilder();

        final HepPlanner planner = new HepPlanner(hepPgmBuilder.build());
        planner.setRoot(rel);
        rel = planner.findBestExp();
        return rel;
    }

    public HepProgramBuilder getHepProgramBuilder() {
        HepProgramBuilder hepPgmBuilder = new HepProgramBuilder();
        hepPgmBuilder.addMatchOrder(HepMatchOrder.ARBITRARY);

        hepPgmBuilder.addGroupBegin();
        hepPgmBuilder.addRuleCollection(RuleToUse.SQL_REWRITE_CALCITE_RULE_PRE);
        hepPgmBuilder.addGroupEnd();
        return hepPgmBuilder;
    }

    private List<Integer> getRefList(SqlNodeList exprList, List<RelDataTypeField> inputFields) {
        List<Integer> refList = new ArrayList<>();
        if (null == exprList) {
            return refList;
        }

        for (SqlNode expr : exprList) {
            final String exprName = Util.last(((SqlIdentifier) expr).names);
            boolean gotIt = false;
            for (int index = 0; index < inputFields.size(); index++) {
                if (TStringUtil.equalsIgnoreCase(inputFields.get(index).getName(), exprName)) {
                    refList.add(index);
                    gotIt = true;
                    break;
                }
            } // end of for

            if (!gotIt) {
                // add wrong value，trigger exception
                refList.add(-1);
            }
        } // end of for
        return refList;
    }

    private SqlSelect appendStarForSelect(SqlSelect select) {
        if (null == select) {
            return select;
        }

        return (SqlSelect) select.accept(new StartAppender());
    }

    private static class StartAppender extends SqlShuttle {

        @Override
        public SqlNode visit(SqlCall call) {
            SqlNode result = super.visit(call);

            if (result instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) result;
                if (null == select.getSelectList()) {
                    SqlIdentifier star = SqlIdentifier.star(SqlParserPos.ZERO);
                    select.setSelectList(new SqlNodeList(ImmutableList.<SqlNode>of(star), SqlParserPos.ZERO));
                }
            } else if (result instanceof SqlBasicCall) {
                boolean countStar = result.getKind() == SqlKind.COUNT
                    && (null == call.getOperandList() || call.getOperandList().size() <= 0);
                if (countStar) {
                    SqlIdentifier star = SqlIdentifier.star(SqlParserPos.ZERO);
                    return new SqlBasicCall(call.getOperator(),
                        new SqlNode[] {star},
                        call.getParserPosition(),
                        call.isExpanded(),
                        call.getFunctionQuantifier());
                }
            }

            return result;
        }
    }

    private RelBuilder createBuilder(RelNode relNode) {
        return RelBuilder.proto(Contexts.EMPTY_CONTEXT).create(relNode.getCluster(),
            SqlConverter.getInstance(PlannerContext.getPlannerContext(relNode).getSchemaName(),
                PlannerContext.getPlannerContext(relNode).getExecutionContext()).getCatalog());
    }

    private int getNumberValue(SqlNode sqlNode) {
        return ((SqlNumericLiteral) sqlNode).intValue(false);
    }

    private RelNode buildRelNode(LogicalView relNode, SqlNodeList hints, SqlSelect ast, ExecutionContext ec) {
        if (hints.size() <= 0) {
            return relNode;
        }
        RelBuilder builder = createBuilder(relNode);
        builder.push(relNode);

        for (SqlNode op : hints) {
            SqlBasicCall hintOp = (SqlBasicCall) op;
            String name = hintOp.getOperator().getName();
            Map<String, SqlNode> argMap = getArgMap(hintOp);

            if (TStringUtil.equalsIgnoreCase(name, "add_ms")) {

                final String sql = "SELECT * FROM DUAL ORDER BY " + getStringValue(argMap.get(String.valueOf(0)));

                final MySqlSelectQueryBlock query = parseQuery(sql);

                final SqlNodeList orderBy = FastSqlConstructUtils.constructOrderBy(query.getOrderBy(), null, ec);

                final SqlNodeList limit = FastSqlConstructUtils.constructLimit(query.getLimit(), null, ec);

                final List<RexNode> orderByExps = gatherOrderExprs(relNode, orderBy, builder.getRexBuilder(), ast);

                int offset = -1;
                int fetch = -1;
                if (limit != null) {
                    offset = getNumberValue(limit.get(0));
                    fetch = getNumberValue(limit.get(1));
                }

                builder.sortLimit(offset, fetch, orderByExps);
                LogicalSort logicalSort = (LogicalSort) builder.build();
                builder.push(MergeSort.create(logicalSort.getInput(),
                    logicalSort.getCollation(),
                    logicalSort.offset,
                    logicalSort.fetch));

            } else if (TStringUtil.equalsIgnoreCase(name, "add_agg")) {
                String sql = "SELECT ";
                if (argMap.containsKey("agg")) {
                    sql += ((SqlLiteral) argMap.get("agg")).toValue();
                } else {
                    sql += "*";
                }

                sql += " FROM DUAL ";

                if (argMap.containsKey("group_by")) {
                    sql += " GROUP BY " + getStringValue(argMap.get("group_by"));
                }

                final MySqlSelectQueryBlock query = parseQuery(sql);

                final SqlNodeList aggCall = FastSqlConstructUtils.constructSelectList(query.getSelectList(), null, ec);
                final SqlNodeList groupBy = FastSqlConstructUtils.constructGroupBy(query.getGroupBy(), null, ec);

                final RelNode input = builder.peek();
                final List<RelDataTypeField> inputFields = input.getRowType().getFieldList();

                // get agg call list
                final List<AggregateCall> aggCallList = buildAggCall(builder, aggCall, inputFields);

                // get group field list
                final List<Integer> groupRefList = getRefList(groupBy, inputFields);

                final ImmutableBitSet groupSet = ImmutableBitSet.of(groupRefList);
                final List<ImmutableBitSet> groupSets = ImmutableList.of(groupSet);

                builder.push(LogicalAggregate.create(builder.build(), groupSet, groupSets, aggCallList));
            } else if (TStringUtil.equalsIgnoreCase(name, "add_ts")) {

                final String sql = "SELECT * FROM DUAL ORDER BY " + getStringValue(argMap.get(String.valueOf(0)));

                final MySqlSelectQueryBlock query = parseQuery(sql);

                final SqlNodeList orderBy = FastSqlConstructUtils.constructOrderBy(query.getOrderBy(), null, ec);

                final SqlNodeList limit = FastSqlConstructUtils.constructLimit(query.getLimit(), null, ec);

                final List<RexNode> orderByExps = gatherOrderExprs(relNode, orderBy, builder.getRexBuilder(), ast);

                int offset = -1;
                int fetch = -1;
                if (limit != null) {
                    offset = getNumberValue(limit.get(0));
                    fetch = getNumberValue(limit.get(1));
                }

                builder.sortLimit(offset, fetch, orderByExps);
            } else if (TStringUtil.equalsIgnoreCase(name, "add_lmt")) {

                final String sql = "SELECT * FROM DUAL LIMIT " + getStringValue(argMap.get(String.valueOf(0)));

                final MySqlSelectQueryBlock query = parseQuery(sql);

                final SqlNodeList limit = FastSqlConstructUtils.constructLimit(query.getLimit(), null, ec);

                int offset = -1;
                int fetch = -1;
                if (limit != null) {
                    offset = getNumberValue(limit.get(0));
                    fetch = getNumberValue(limit.get(1));
                }

                builder.limit(offset, fetch);
            } else if (TStringUtil.equalsIgnoreCase(name, "add_un")) {
                builder.push(Gather.create(builder.build()));
            } else if (TStringUtil.equalsIgnoreCase(name, "add_ft")) {

                final String sql = "SELECT * FROM DUAL WHERE " + getStringValue(argMap.get(String.valueOf(0)));

                final MySqlSelectQueryBlock query = parseQuery(sql);

                final SqlNode where = FastSqlConstructUtils.constructWhere(query.getWhere(), null, ec);

                final SqlValidatorImpl validatorForScope = getValidatorForScope(ast);

                final Blackboard bb = createBlackboard(validatorForScope.getSelectScope(ast), null, true);

                this.hintBlackboard.beginSelect();
                try {
                    // 初始化 where 条件的 scope
                    convertFrom(bb, ast.getFrom());
                } finally {
                    this.hintBlackboard.endFrom();
                }

                final RexNode convertedWhere = bb.convertExpression(where);

                builder.filter(convertedWhere);
            } else if (TStringUtil.equalsIgnoreCase(name, "add_pj")) {
                final String sql = "SELECT " + getStringValue(argMap.get(String.valueOf(0))) + " FROM DUAL";

                final MySqlSelectQueryBlock query = parseQuery(sql);

                final SqlNodeList selectList =
                    FastSqlConstructUtils.constructSelectList(query.getSelectList(), null, ec);

                final SqlValidatorImpl validatorForScope = getValidatorForScope(ast);

                final Blackboard bb = createBlackboard(validatorForScope.getSelectScope(ast), null, true);

                this.hintBlackboard.beginSelect();
                try {
                    // 初始化 scope
                    convertFrom(bb, ast.getFrom());
                } finally {
                    this.hintBlackboard.endFrom();
                }

                List<String> fieldNames = new ArrayList<>();
                List<String> originalNames = new ArrayList<>();
                final List<RexNode> exprs = new ArrayList<>();
                final Collection<String> aliases = new TreeSet<>();

                // Project select clause.
                int i = -1;
                for (SqlNode expr : selectList) {
                    ++i;
                    exprs.add(bb.convertExpression(expr));
                    fieldNames.add(deriveAlias(expr, aliases, i));
                    originalNames.add(deriveOriginalAlias(expr, i));
                }

                fieldNames = SqlValidatorUtil.uniquify(fieldNames, catalogReader.nameMatcher().isCaseSensitive());

                builder.push(RelOptUtil.createProject(builder.build(), exprs, fieldNames, originalNames));
            } // end of if
        } // end of for

        return builder.build();
    }

    private String getStringValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            return ((SqlCharStringLiteral) sqlNode).getNlsString().getValue();

        }
        return sqlNode.toString();
    }

    private List<AggregateCall> buildAggCall(RelBuilder builder, SqlNodeList aggs, List<RelDataTypeField> inputFields) {
        List<AggregateCall> aggCallList = new ArrayList<>();
        for (SqlNode agg : aggs) {
            String aggName = agg.toString();
            SqlCall aggCall = (SqlCall) agg;
            if (agg.getKind() == SqlKind.AS) {
                List<SqlNode> operandList = ((SqlBasicCall) agg).getOperandList();
                aggCall = (SqlCall) operandList.get(0);
                aggName = operandList.get(1).toString();
            }
            final SqlAggFunction aggFuncOp = (SqlAggFunction) aggCall.getOperator();
            boolean distinct = false;
            SqlLiteral quantifier = aggCall.getFunctionQuantifier();
            if ((null != quantifier) && (quantifier.getValue() == SqlSelectKeyword.DISTINCT)) {
                distinct = true;
            }

            final List<Integer> refList = new ArrayList<>();
            final List<RelDataType> operandTypeList = new ArrayList<>();
            for (SqlNode expr : aggCall.getOperandList()) {
                final String exprName = Util.last(((SqlIdentifier) expr).names);
                boolean gotIt = false;
                for (int index = 0; index < inputFields.size(); index++) {
                    if (TStringUtil.equalsIgnoreCase(inputFields.get(index).getName(), exprName)) {
                        refList.add(index);
                        operandTypeList.add(inputFields.get(index).getType());
                        gotIt = true;
                        break;
                    }
                } // end of for

                if (!gotIt) {
                    // add wrong value，trigger exception
                    refList.add(-1);
                }
            } // end of for

            final List<Integer> argList = getRefList(new SqlNodeList(aggCall.getOperandList(), SqlParserPos.ZERO),
                inputFields);

            final RelDataType type = aggFuncOp.inferReturnType(builder.getTypeFactory(), operandTypeList);

            aggCallList.add(AggregateCall.create(aggFuncOp, distinct, false, argList, -1, type, aggName));
        }
        return aggCallList;
    }

    private List<RexNode> gatherOrderExprs(LogicalView logicalView, SqlNodeList orderBy, RexBuilder rexBuilder,
                                           SqlSelect ast) {
        SqlValidatorImpl validator = getValidatorForScope(ast);

        final List<RexNode> orderByExps = new ArrayList<>();
        final List<SqlNode> orderExprList = new ArrayList<>();
        for (SqlNode orderByItem : orderBy) {
            RelFieldCollation relFieldCollation = buildOrderItem((SqlSelect) logicalView.getNativeSqlNode(),
                orderByItem,
                orderExprList,
                Direction.ASCENDING,
                NullDirection.UNSPECIFIED,
                validator);

            RexNode rexNode = RexInputRef.of(relFieldCollation.getFieldIndex(), logicalView.getRowType());
            if (relFieldCollation.direction == Direction.DESCENDING) {
                rexNode = rexBuilder.makeCall(SqlStdOperatorTable.DESC, rexNode);
            }

            switch (relFieldCollation.nullDirection) {
            case LAST:
                rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NULLS_LAST, rexNode);
                break;
            case FIRST:
                rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NULLS_FIRST, rexNode);
                break;
            default:
            }

            orderByExps.add(rexNode);
        } // end of for
        return orderByExps;
    }

    private SqlValidatorImpl getValidatorForScope(SqlNode sqlNode) {
        Util.discard(validator.validate(sqlNode));
        return (SqlValidatorImpl) validator;
    }

    private SqlSelect updateSqlNode(SqlSelect nativeSqlNode, SqlNodeList hints, ExecutionContext ec) {
        if (hints.size() <= 0) {
            return nativeSqlNode;
        }

        SqlSelect result = (SqlSelect) nativeSqlNode.clone(SqlParserPos.ZERO);

        // 清理 FROM 中的库名前缀
        if (result.getFrom() instanceof SqlIdentifier) {
            SqlIdentifier from = (SqlIdentifier) result.getFrom();
            if (from.names.size() > 1) {
                from.setNames(ImmutableList.of(Util.last(from.names)), ImmutableList.of(SqlParserPos.ZERO));
            }
        }

        final List<HintPushOperator> pushHints = new LinkedList<>();
        HintConverter.convertPush(hints, pushHints, ec);

        for (HintPushOperator pushHint : pushHints) {
            pushHint.handle(result);
        }

        return result;
    }

    private Map<String, SqlNode> getArgMap(SqlBasicCall hintOp) {
        Map<String, SqlNode> argMap = new LinkedHashMap<>();
        for (int index = 0; index < hintOp.getOperands().length; index++) {
            if (hintOp.getOperands()[index] instanceof SqlBasicCall) {
                SqlBasicCall arg = (SqlBasicCall) hintOp.getOperands()[index];
                if (arg.getOperator().getKind() == SqlKind.EQUALS) {
                    argMap.put(arg.getOperands()[0].toString(), arg.getOperands()[1]);
                } else {
                    argMap.put(String.valueOf(index), arg.getOperands()[0]);
                }
            } else {
                argMap.put(String.valueOf(index), hintOp.getOperands()[0]);
            }
        }
        return argMap;
    }

    private MySqlSelectQueryBlock parseQuery(String sql) {
        SQLSelectStatement sqlSelect = (SQLSelectStatement) FastsqlUtils.parseSql(sql).get(0);
        return (MySqlSelectQueryBlock) sqlSelect.getSelect().getQuery();
    }

    protected RelFieldCollation buildOrderItem(SqlSelect select, SqlNode orderItem, List<SqlNode> extraExprs,
                                               RelFieldCollation.Direction direction,
                                               RelFieldCollation.NullDirection nullDirection,
                                               SqlValidatorImpl validator) {
        assert select != null;
        // Handle DESC keyword, e.g. 'select a, b from t order by a desc'.
        switch (orderItem.getKind()) {
        case DESCENDING:
            return buildOrderItem(select,
                ((SqlCall) orderItem).operand(0),
                extraExprs,
                RelFieldCollation.Direction.DESCENDING,
                nullDirection,
                validator);
        case NULLS_FIRST:
            return buildOrderItem(select,
                ((SqlCall) orderItem).operand(0),
                extraExprs,
                direction,
                RelFieldCollation.NullDirection.FIRST,
                validator);
        case NULLS_LAST:
            return buildOrderItem(select,
                ((SqlCall) orderItem).operand(0),
                extraExprs,
                direction,
                RelFieldCollation.NullDirection.LAST,
                validator);
        default:
        }

        SqlNode converted = validator.expandOrderExpr(select, orderItem);

        switch (nullDirection) {
        case UNSPECIFIED:
            nullDirection = NullCollation.LOW.last(desc(direction)) ? RelFieldCollation.NullDirection.LAST :
                RelFieldCollation.NullDirection.FIRST;
        default:
        }

        // Scan the select list and order exprs for an identical expression.
        int ordinal = -1;
        final SelectScope selectScope = validator.getRawSelectScope(select);
        for (SqlNode selectItem : selectScope.getExpandedSelectList()) {
            ++ordinal;
            if (converted.equalsDeep(stripAs(selectItem), Litmus.IGNORE)) {
                return new RelFieldCollation(ordinal, direction, nullDirection);
            }
        }

        for (SqlNode extraExpr : extraExprs) {
            ++ordinal;
            if (orderItem.equalsDeep(extraExpr, Litmus.IGNORE)) {
                return new RelFieldCollation(ordinal, direction, nullDirection);
            }
        }

        // TODO: handle collation sequence
        // TODO: flag expressions as non-standard

        extraExprs.add(orderItem);
        return new RelFieldCollation(ordinal + 1, direction, nullDirection);
    }

    private static boolean desc(RelFieldCollation.Direction direction) {
        switch (direction) {
        case DESCENDING:
        case STRICTLY_DESCENDING:
            return true;
        default:
            return false;
        }
    }

    protected static class TddlValidator extends SqlValidatorImpl {

        protected TddlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
                                RelDataTypeFactory typeFactory, SqlConformance conformance) {
            super(opTab, catalogReader, typeFactory, conformance);
        }
    }
}

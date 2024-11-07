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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.constants.CpuStatAttribute;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.hint.ExtraCmdRouteCondition;
import com.alibaba.polardbx.common.model.hint.RouteCondition;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbVariableConfigManager;
import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.gms.locality.PrimaryZoneInfo;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime.Now;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelToXPlanConverter;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelXPlanOptimizer;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.SpecialFunctionRelFinder;
import com.alibaba.polardbx.optimizer.core.planner.rule.CBOLogicalSemiJoinLogicalJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.CBOPushJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.CBOPushSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsAggregateJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsCorrelateConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsFilterConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsOutFileConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsProjectConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsSortJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsSortProjectTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.ExpandLogicalJoinToBKAJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.FilterReorderRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.GenXplanRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionPruningRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalAggToSortAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalJoinToSortMergeJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalSemiJoinToSemiSortMergeJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.OptimizeLogicalViewRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.OptimizePhySqlRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.OuterJoinAssocRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.OuterJoinLAsscomRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PhyPushAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PhyTwoPhaseAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.ProjectSortTransitiveRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushFilterRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushModifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushProjectRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushSortRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.RuleToUse;
import com.alibaba.polardbx.optimizer.core.planner.rule.SQL_REWRITE_RULE_PHASE;
import com.alibaba.polardbx.optimizer.core.planner.rule.SemiJoinSemiJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.SubQueryToSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLProjectHashJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalAggToHashAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToBKAJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToMaterializedSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.PushBloomFilterRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CheapestFractionalPlanReplacer;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.PartitionWiseAssigner;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.SubQueryPlanEnumerator;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.BuildFinalPlanVisitor;
import com.alibaba.polardbx.optimizer.core.rel.CollectorTableVisitor;
import com.alibaba.polardbx.optimizer.core.rel.CountVisitor;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectShardingKeyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ForceIndexSingleVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ForceIndexVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LBACRowAccessChecker;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.RemoveFixedCostVisitor;
import com.alibaba.polardbx.optimizer.core.rel.RemoveIndexNodeVisitor;
import com.alibaba.polardbx.optimizer.core.rel.RemoveSchemaNameVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceDifferentDBSingleTblWithPhyTblVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceSingleTblOrBroadcastTblWithPhyTblVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ToDrdsRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.UserHintPassThroughVisitor;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.msha.TddlMshaProcessor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.HintParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.SqlTypeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.PreparedParamRef;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.hint.SimpleHintParser;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsParameterizeSqlVisitor;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.result.PlanShardInfo;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.optimizer.utils.MetaUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers;
import com.alibaba.polardbx.optimizer.variable.VariableManager;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.workload.ExchangeOptimizerType;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.OutFileParams;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_ENABLE_PQO;
import static com.alibaba.polardbx.druid.sql.SQLUtils.parserFeatures;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.getRexNodeTableMap;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainAdvisor;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOptimizer;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainStatistics;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isSuitableForDirectMode;
import static com.alibaba.polardbx.optimizer.utils.OptimizerUtils.hasDNHint;
import static com.alibaba.polardbx.optimizer.utils.RelUtils.disableMpp;
import static com.alibaba.polardbx.optimizer.workload.WorkloadUtil.determineWorkloadType;
import static org.apache.calcite.sql.SqlKind.DDL;
import static org.apache.calcite.sql.SqlKind.DML;
import static org.apache.calcite.sql.SqlKind.QUERY;

/**
 * @author lingce.ldm 2017-07-07 14:44
 */
public class Planner {

    private static final Planner INSTANCE = new Planner();
    private static final Logger logger = LoggerFactory.getLogger(Planner.class);

    public Planner() {
    }

    public static Planner getInstance() {
        return INSTANCE;
    }

    public ExecutionPlan plan(String sql, ExecutionContext executionContext) {
        return plan(ByteString.from(sql), executionContext);
    }

    public ExecutionPlan planForPrepare(ByteString sql,
                                        PreparedStmtCache preparedStmtCache,
                                        ExecutionContext executionContext) {
        ByteString afterProcessSql = removeSpecialHint(sql, executionContext);
        SqlParameterized sqlParameterized = parameterize(afterProcessSql, executionContext, true);
        SqlType sqlType = sqlParameterized.getAst().getSqlType();
        processParameters(sqlParameterized.getParameters(), executionContext);
        preparedStmtCache.setSqlParameterized(sqlParameterized);
        preparedStmtCache.setSqlType(sqlType);

        SqlNodeList astList;
        if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PARAMETER_PLAN)) {
            astList = new FastsqlParser()
                .parse(sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
        } else {
            astList = new FastsqlParser().parse(sqlParameterized.getOriginSql(), executionContext);
        }
        return plan(sql, sqlType, sqlParameterized, executionContext, astList, true);
    }

    public ExecutionPlan plan(ByteString sql, ExecutionContext executionContext) {
        if (executionContext.isExecutingPreparedStmt()) {
            PreparedStmtCache preparedStmtCache = executionContext.getPreparedStmtCache();
            Preconditions.checkState(preparedStmtCache != null, "Prepared statement cache does not exist");
            SqlParameterized sqlParameterized = preparedStmtCache.getSqlParameterized();
            if (sqlParameterized != null && !sqlParameterized.isUnparameterized()) {
                return planForExecutePrepared(sql, executionContext);
            }
            // 没有走参数化的语句 需要走兜底策略
        }
        return planAfterProcessing(sql, executionContext);
    }

    private ExecutionPlan planForExecutePrepared(ByteString sql,
                                                 ExecutionContext executionContext) {
        PreparedStmtCache preparedStmtCache = executionContext.getPreparedStmtCache();
        SqlParameterized sqlParameterized = preparedStmtCache.getSqlParameterized();
        Parameters params = executionContext.getParams();
        if (!params.isBatch()) {
            Map<Integer, ParameterContext> currentParameter = params.getCurrentParameter();
            for (int i = 0; i < sqlParameterized.getParameters().size(); i++) {
                if (sqlParameterized.getParameters().get(i) instanceof PreparedParamRef) {
                    PreparedParamRef preparedParamRef = (PreparedParamRef) sqlParameterized.getParameters().get(i);
                    int index = preparedParamRef.getIndex() + 1;
                    preparedParamRef.setValue(currentParameter.get(index).getValue());
                }
            }
            currentParameter.clear();
        }

        return plan(sql, preparedStmtCache.getSqlType(), preparedStmtCache.getSqlParameterized(),
            executionContext);
    }

    /**
     * 经过各种预处理后生成执行计划
     */
    private ExecutionPlan planAfterProcessing(ByteString sql, ExecutionContext executionContext) {
        ByteString afterProcessSql = removeSpecialHint(sql, executionContext);
        SqlParameterized parameterized = parameterize(afterProcessSql, executionContext);
        SqlType sqlType = parameterized.getAst().getSqlType();
        return plan(sql, sqlType, parameterized, executionContext);
    }

    private ExecutionPlan plan(ByteString sql, SqlType sqlType, SqlParameterized parameterized,
                               ExecutionContext executionContext) {
        return plan(sql, sqlType, parameterized, executionContext, null, false);
    }

    /**
     * Sql to ExecutionPlan
     */
    private ExecutionPlan plan(ByteString sql, SqlType sqlType,
                               SqlParameterized parameterized, ExecutionContext executionContext,
                               SqlNodeList sqlNodeList,
                               boolean forPrepare) {
        executionContext.setOriginSql(sql.toString());

        // prepare statement with parameters
        Parameters parameters = executionContext.getParams();
        if (parameters == null) {
            executionContext.setParams(new Parameters());
        }
        if (parameterized == null) {
            return null;
        }

        final SQLStatement statement = parameterized.getStmt();
        if (statement instanceof MySqlExplainStatement && !(((MySqlExplainStatement) statement).isDescribe())) {
            parameterized = handleExplain(sql, (MySqlExplainStatement) statement, executionContext, forPrepare);
        }

        parameterized.setForPrepare(forPrepare);
        executionContext.setSqlTemplateId(PlanManagerUtil.generateTemplateId(parameterized.getSql()));

        return doPlan(sqlType, parameterized, executionContext, sqlNodeList, forPrepare);
    }

    private SqlParameterized parameterize(ByteString afterProcessSql, ExecutionContext executionContext) {
        return parameterize(afterProcessSql, executionContext, false);
    }

    private SqlParameterized parameterize(ByteString afterProcessSql,
                                          ExecutionContext executionContext,
                                          boolean isPrepare) {
        boolean enableSqlCpu = false;
        if (executionContext != null) {
            enableSqlCpu = MetricLevel.isSQLMetricEnabled(
                executionContext.getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL)) &&
                executionContext.getRuntimeStatistics() != null;
        }

        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();
        long startParameterize = 0L;
        if (enableSqlCpu) {
            startParameterize = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }

        SqlParameterized result =
            SqlParameterizeUtils.parameterize(afterProcessSql, currentParameter, executionContext, isPrepare);

        if (enableSqlCpu) {
            executionContext.getRuntimeStatistics()
                .getCpuStat()
                .addCpuStatItem(CpuStatAttribute.CpuStatAttr.PARAMETERIZE_SQL,
                    ThreadCpuStatUtil.getThreadCpuTimeNano() - startParameterize);
        }
        return result;
    }

    private SqlParameterized handleExplain(ByteString sql, MySqlExplainStatement sqlExplain,
                                           ExecutionContext executionContext, boolean forPrepare) {
        // process explain
        ExplainResult result = new ExplainResult();
        result.explainMode = ExplainResult.ExplainMode.DETAIL;
        for (ExplainResult.ExplainMode mode : ExplainResult.ExplainMode.values()) {
            if (mode.name().equalsIgnoreCase(sqlExplain.getType())) {
                result.explainMode = mode;
                break;
            }
        }
        executionContext.setCalcitePlanOptimizerTrace(new CalcitePlanOptimizerTrace());
        if (isExplainOptimizer(result)) {
            executionContext.getCalcitePlanOptimizerTrace().ifPresent(x -> x.setOpen(true));
        }
        List<SQLCommentHint> oriHints = sqlExplain.getHints();
        if (null == oriHints) {
            oriHints = sqlExplain.getHeadHintsDirect();
        }
        //inherit the hint from explain statement
        if (null != oriHints && oriHints.size() != 0) {
            sqlExplain.getStatement().setHeadHints(oriHints);
        }

        executionContext.setExplain(result);

        //parameterized sql without explain keyword
        Map<Integer, ParameterContext> parameters = executionContext.getParams().getCurrentParameter();
        ByteString explainedQuery = getQueryAfterExplain(sql);
        return SqlParameterizeUtils
            .parameterize(explainedQuery, sqlExplain.getStatement(), parameters, executionContext, forPrepare);
    }

    private static ByteString getQueryAfterExplain(ByteString explainQuery) {
        MySqlLexer lexer = new MySqlLexer(explainQuery);
        while (true) {
            lexer.nextToken();
            if (lexer.token() == Token.EXPLAIN) {
                lexer.nextToken(); // Move to next token after EXPLAIN
                return explainQuery.slice(lexer.getStartPos());
            }
        }
    }

    /**
     * build plan for parameterized sql (no plan cache)
     */
    public ExecutionPlan doBuildPlan(SqlNodeList astList,
                                     ExecutionContext executionContext, boolean forPrepare) {
        SqlNode ast = astList.get(0);
        Set<Pair<String, String>> tableSet = PlanManagerUtil.getTableSetFromAst(ast);

        if (ast.getKind().belongsTo(SqlKind.DML)) {
            tableSet = PlanManagerUtil.getTableSetFromAst(ast);
        }
        ExecutionPlan plan;
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        if (executionContext.getExplain() != null && executionContext.getExplain().explainMode.isCostTrace()) {
            plannerContext.setNeedStatisticTrace(true);
        }
        if (executionContext.isUseHint()) {
            // handle plan hint first
            plan = buildPlanWithHint(ast, plannerContext, executionContext);
        } else {
            plan = getPlan(ast, plannerContext);
            plan.setExplain(executionContext.getExplain() != null);
            plan.setUsePostPlanner(
                PostPlanner.usePostPlanner(executionContext.getExplain(),
                    executionContext.isUseHint()));
        }

        // build foreign key sub plans
        if (!forPrepare) {
            ForeignKeyUtils.buildForeignKeySubPlans(executionContext, plan, plannerContext);
        }

        // 如果hint和async同时指定了异步模式，已async的为准
        if (ast.getAsync() != null) {
            executionContext.getParamManager().setBooleanVal(executionContext.getParamManager().getProps(),
                ConnectionParams.PURE_ASYNC_DDL_MODE, ast.getAsync(), true);
            executionContext.getParamManager().setBooleanVal(executionContext.getParamManager().getProps(),
                ConnectionParams.ASYNC_PAUSE, ast.getAsync(), true);
        }
        Map<String, TableMeta> tableMetas = PlanManagerUtil.getTableMetaSetByTableSet(tableSet, executionContext);
        plan.saveCacheState(tableSet, 0, null, tableMetas);
        return plan;
    }

    /**
     * build plan for parameterized sql (no plan cache)
     */
    public ExecutionPlan doBuildPlan(SqlParameterized sqlParameterized, ExecutionContext executionContext) {
        // parse
        SqlNodeList astList;
        if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PARAMETER_PLAN)) {
            astList = new FastsqlParser()
                .parse(sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
        } else {
            astList = new FastsqlParser().parse(sqlParameterized.getOriginSql(), executionContext);
        }
        return doBuildPlan(astList, executionContext, sqlParameterized.getForPrepare());
    }

    /**
     * If processing params during the execution of server-prepared stmt,
     * the params won't be updated in place. Thus, the parameterized sql
     * can keep the origin params.
     */
    public static void processParameters(List<Object> params, ExecutionContext executionContext) {
        if (params != null) {
            // record nlsString in parameters
            executionContext.setParameterNlsStrings(new HashMap<>(params.size()));
            if (!executionContext.isExecutingPreparedStmt()) {
                for (int i = 0; i < params.size(); i++) {
                    Object param = params.get(i);
                    params.set(i, processSingleParam(i, param, executionContext));
                }
            }
        }
        Parameters parameters = executionContext.getParams();
        if (parameters.isBatch()) {
            parameters.setBatchParams(OptimizerUtils.buildBatchParam(params, executionContext));
        } else {
            parameters.setParams(OptimizerUtils.buildParam(params, executionContext));
        }
    }

    /**
     * fill the userDefVariable & sysDefVariable
     *
     * @param index index of param
     */
    public static Object processSingleParam(int index, Object param, ExecutionContext executionContext) {
        if (param instanceof DrdsParameterizeSqlVisitor.UserDefVariable) {
            DrdsParameterizeSqlVisitor.UserDefVariable userDefVariable =
                (DrdsParameterizeSqlVisitor.UserDefVariable) param;
            Map<String, Object> userDefVariables = executionContext.getUserDefVariables();
            return userDefVariables.get(userDefVariable.getName().toLowerCase());
        } else if (param instanceof DrdsParameterizeSqlVisitor.SysDefVariable) {
            DrdsParameterizeSqlVisitor.SysDefVariable sysDefVariable =
                (DrdsParameterizeSqlVisitor.SysDefVariable) param;
            String name = StringUtils.strip(sysDefVariable.getName().toLowerCase(), "`");
            if ("last_insert_id".equals(name)) {
                return executionContext.getConnection().getLastInsertId();
            } else if ("tx_isolation".equals(name) || "transaction_isolation".equals(name)) {
                // Note: It's hard to get global isolation level here...
                IsolationLevel isolation = IsolationLevel.fromInt(executionContext.getTxIsolation());
                return isolation != null ? isolation.nameWithHyphen() : null;
            } else if ("compute_node".equals(name)) {
                return TddlNode.getHost() + ":" + TddlNode.getPort();
            } else if ("primary_zone".equals(name)) {
                PrimaryZoneInfo primaryZoneInfo = LocalityManager.getInstance().getSystemPrimaryZone();
                return primaryZoneInfo.serialize();
            } else if ("read_only".equals(name)) {
                if (ConfigDataMode.isMasterMode()) {
                    return 0;
                } else if (ConfigDataMode.isReadOnlyMode()) {
                    return 1;
                }
            } else if ("auto_increment_increment".equalsIgnoreCase(name) &&
                SequenceManagerProxy.getInstance()
                    .areAllSequencesSameType(executionContext.getSchemaName(), new Type[] {Type.GROUP, Type.TIME})) {
                // Since the steps of Group and Time-based Sequence are fixed to 1,
                // so we have to override auto_increment_increment set on RDS for
                // correct behavior of generated keys.
                return 1;
            } else {
                if (!ServerVariables.contains(name) && !ServerVariables.isExtra(name) && !ServerVariables.isCdcGlobal(
                    name)) {
                    throw new TddlNestableRuntimeException("Unknown system variable " + name);
                }
                VariableManager variableManager =
                    OptimizerContext.getContext(executionContext.getSchemaName()).getVariableManager();
                Object v = null;
                if (sysDefVariable.isGlobal()) {
                    v = variableManager.getGlobalVariable(name);

                    Properties cnProperties =
                        MetaDbInstConfigManager.getInstance().getCnVariableConfigMap();
                    Map<String, Object> dnProperties =
                        MetaDbVariableConfigManager.getInstance().getDnVariableConfigMap();
                    Map<String, String> cdcProperties =
                            MetaDbVariableConfigManager.getInstance().getCdcVariableConfigMap();if (cnProperties.containsKey(name)) {
                        v = cnProperties.getProperty(name);
                    } else if (dnProperties.containsKey(name)) {
                        v = dnProperties.get(name);} else if (cdcProperties.containsKey(name)) {
                            v = cdcProperties.get(name);
                        }

                    if ("server_id".equalsIgnoreCase(name) && cnProperties.containsKey(name.toUpperCase())) {
                        v = cnProperties.getProperty(name.toUpperCase());
                    }
                } else {
                    if (executionContext.getExtraServerVariables() != null
                        && executionContext.getExtraServerVariables().containsKey(name)) {
                        v = executionContext.getExtraServerVariables().get(name);
                    } else if (executionContext.getServerVariables() != null) {
                        v = executionContext.getServerVariables().get(name);
                    }
                    if (v == null) {
                        v = variableManager.getSessionVariable(name);
                    }
                }

                // IMPORTANT!
                if ("sql_mode".equals(name) && v instanceof String && "default".equalsIgnoreCase((String) v)) {
                    v = variableManager.getGlobalVariable(name);
                }

                if (v instanceof String && TStringUtil.isParsableNumber((String) v)) {
                    Number number;
                    try {
                        number = NumberFormat.getInstance().parse((String) v);
                    } catch (ParseException e) {
                        number = null;
                    }
                    if (number != null) {
                        v = number;
                    }
                }

                if (v instanceof String && !("query_cache_type".equals(name) || "gtid_mode".equals(
                    name))) {
                    if ("NULL".equalsIgnoreCase((String) v)) {
                        v = "";
                    } else if ("OFF".equalsIgnoreCase((String) v)) {
                        v = 0;
                    } else if ("ON".equalsIgnoreCase((String) v)) {
                        v = 1;
                    }
                }

                if (v instanceof Boolean) {
                    if (((Boolean) v)) {
                        v = 1;
                    } else {
                        v = 0;
                    }
                }

                if (v == null) {
                    v = "";
                }
                return v;
            }
        } else if (param instanceof DrdsParameterizeSqlVisitor.ConstantVariable) {
            DrdsParameterizeSqlVisitor.ConstantVariable constantVariable =
                (DrdsParameterizeSqlVisitor.ConstantVariable) param;
            String name = StringUtils.strip(constantVariable.getName().toLowerCase(), "`");
            if ("now".equals(name)) {
                Now now = null;
                if (constantVariable.getArgs().length != 0) {
                    now = new Now(null, (new DateTimeType((Integer) constantVariable.getArgs()[0])));
                    return new DateTimeType((Integer) constantVariable.getArgs()[0])
                        .convertFrom(executionContext.getConstantValue(name, now, new Object[] {6}));
                } else {
                    now = new Now(null, new DateTimeType());
                    return new DateTimeType()
                        .convertFrom(executionContext.getConstantValue(name, now, new Object[] {6}));
                }
            }
        } else if (param instanceof NlsString) {
            executionContext.getParameterNlsStrings().put(index, (NlsString) param);
            // reduce nlsString to Java String
            NlsString nlsString = (NlsString) param;
            if (CharsetName.of(nlsString.getCharsetName()) != CharsetName.BINARY) {
                return nlsString.getValue();
            } else {
                return nlsString.getValue().getBytes(StandardCharsets.ISO_8859_1);
            }
        }
        return param;
    }

    /**
     * parse and remove simple hint
     */
    private ByteString removeSpecialHint(ByteString sql, ExecutionContext context) {

        // SQL的HINT采用老式写法，如 /*+TDDL({'extra':{'SOCKET_TIMEOUT':'0'}})*/
        int hintCount = HintParser.getInstance().getAllHintCount(sql);
        if (hintCount == 0) {
            return sql;
        }

        Map<String, Object> extraCmds = new HashMap<String, Object>();
        ByteString newSql = HintUtil.convertSimpleHint(sql, extraCmds);
        if (extraCmds.size() > 0) {
            context.getExtraCmds().putAll(extraCmds);
        }

        String newHint = HintParser.getInstance().getTddlHint(newSql);
        RouteCondition rc = SimpleHintParser.convertHintStrToCondition(context.getSchemaName(), newHint, false);
        if (null != rc) {
            extraCmds = rc.getExtraCmds();
            if (extraCmds.size() > 0) {
                context.getExtraCmds().putAll(extraCmds);
            }
        }
        String mshaHint = HintParser.getInstance().getTddlMshaHint(newSql);
        if (!StringUtils.isEmpty(mshaHint)) {
            RouteCondition rcOfMsha =
                SimpleHintParser.convertHintStrToCondition(context.getSchemaName(), mshaHint, false);
            if (null != rcOfMsha) {
                extraCmds = rcOfMsha.getExtraCmds();
                if (extraCmds.size() > 0) {
                    context.getExtraCmds().putAll(extraCmds);
                }
            }
        }

        if (extraCmds.containsKey(ConnectionProperties.COLLECT_SQL_ERROR_INFO)
            || extraCmds.containsKey(ConnectionProperties.RETRY_ERROR_SQL_ON_OLD_SERVER)
            || extraCmds.containsKey(ConnectionProperties.SCALE_OUT_WRITE_DEBUG)) {

            if (rc != null && !(rc instanceof ExtraCmdRouteCondition)) {
                throw new NotSupportException("routing hint");
            }

            newSql = HintParser.getInstance().getSqlRemovedHintStr(sql);
        }
        return newSql;
    }

    private void processMsah(SqlParameterized sqlParameterized, ExecutionContext executionContext) {
        try {
            TddlMshaProcessor.processMsahHint(sqlParameterized.isDML(),
                sqlParameterized.isUpdateDelete(),
                executionContext.getExtraCmds());
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private ExecutionPlan buildPlanWithHint(SqlNode ast, PlannerContext plannerContext, ExecutionContext ec) {
        final Map<Integer, ParameterContext> param = ec.getParams().getCurrentParameter();
        ExplainResult explain = ec.getExplain();
        boolean isExplain = explain != null;
        ExecutionPlan executionPlan = null;
        // init HINT
        final HintPlanner hintPlanner = HintPlanner.getInstance(ec.getSchemaName(), ec);
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(ec.getSchemaName(), new HashMap<>(),
            ec.getGroupHint());
        HintConverter.HintCollection hintCollection =
            hintPlanner.collectAndPreExecute(ast, cmdBean, ec.isTestMode(), ec);
        if (!hintCollection.errorMessages.isEmpty()) {
            hintCollection.errorMessages.stream()
                .map(text -> new ExecutionContext.ErrorMessage(0, "", text))
                .forEach(e -> ec.addMessage(ExecutionContext.FAILED_MESSAGE, e));
        }
        ec.setGroupHint(cmdBean.getGroupHint().toString());
        boolean recordDefaultCmd = false;
        if (isExplain && explain.explainMode.isStatistics()) {
            recordDefaultCmd = true;
        }
        // for select into outfile statistics, record origin cmd_extra
        if (ast instanceof TDDLSqlSelect) {
            recordDefaultCmd = OutFileParams.IsStatistics(((TDDLSqlSelect) ast).getOutFileParams());
        }
        if (recordDefaultCmd) {
            ec.putAllHintCmdsWithDefault(cmdBean.getExtraCmd());
        } else {
            ec.putAllHintCmds(cmdBean.getExtraCmd());
        }
        cmdBean.setExtraCmd(ec.getExtraCmds());
        ec.setOriginSqlPushdownOrRoute(hintCollection.pushdownSqlOrRoute());

        if (cmdBean.isScan()) {
            //force close mpp.
            disableMpp(ec);
        }

        if (hintCollection.pushdownOriginSql() ||
            hintCollection.directWithRealTableName() ||
            (StringUtils.isNotEmpty(ec.getPartitionHint()) &&
                (ast.isA(DML) || ast.isA(QUERY)) &&
                checkAst(ast) &&
                isSuitableForDirectMode(explain) &&
                !ec.isVisitDBBuildIn()
            )) {
            executionPlan = hintPlanner.direct(ast, cmdBean, hintCollection, param, ec.getSchemaName(), ec);
        } else if (ast.getKind() == SqlKind.BASELINE) {
            // Do not pushdown baseline management sql
            executionPlan = getPlan(ast, plannerContext);
        } else if (hintCollection.cmdOnly() || hintCollection.errorMessages.size() > 0) {
            //FIXME here task the illegal hint as the cmd hint.
            if (ast instanceof SqlExplain) {
                executionPlan = hintPlanner.pushdown(getPlan(((SqlExplain) ast).getExplicandum(), plannerContext),
                    ast,
                    cmdBean,
                    hintCollection,
                    param,
                    ec.getExtraCmds(), ec);
            } else {
                if (hintCollection.hasPlanHint() && cmdBean.getExtraCmd().get(ConnectionProperties.PLAN) != null) {
                    String externalizePlan = cmdBean.getExtraCmd().get(ConnectionProperties.PLAN).toString();
                    plannerContext.setExternalizePlan(externalizePlan);
                    executionPlan = getPlan(ast, plannerContext);
                    plannerContext.setExternalizePlan(null);
                } else {
                    executionPlan = hintPlanner.pushdown(getPlan(ast, plannerContext),
                        ast,
                        cmdBean,
                        hintCollection,
                        param,
                        ec.getExtraCmds(), ec);
                }

            }
        } else {
            if (hintCollection.hasPlanHint() && cmdBean.getExtraCmd().get(ConnectionProperties.PLAN) != null) {
                String externalizePlan = cmdBean.getExtraCmd().get(ConnectionProperties.PLAN).toString();
                plannerContext.setExternalizePlan(externalizePlan);
                executionPlan = getPlan(ast, plannerContext);
                plannerContext.setExternalizePlan(null);
            } else {
                executionPlan = hintPlanner.getPlan(ast, plannerContext, plannerContext.getExecutionContext());
            }
        }

        executionPlan.setExplain(isExplain);
        executionPlan.setUsePostPlanner(
            StringUtils.isEmpty(ec.getPartitionHint()) &&
                (PostPlanner.usePostPlanner(explain, ec.isUseHint())
                    || hintCollection.usePostPlanner()));
        executionPlan.setHintCollection(hintCollection);
        return executionPlan;
    }

    /**
     * Check if the AST syntax tree contains from or where clauses
     */
    private boolean checkAst(SqlNode ast) {
        if (ast instanceof TDDLSqlSelect) {
            return ((TDDLSqlSelect) ast).getFrom() != null;
        }
        return true;
    }

    public ExecutionPlan insertOverwriteGetPlan(SqlNode ast, PlannerContext plannerContext) {
        SqlInsert insert = (SqlInsert) ast;
        String truncateSql = "TRUNCATE TABLE " + insert.getTargetTable().toString();
        SqlTruncateTable truncateAst = (SqlTruncateTable) new FastsqlParser().parse(truncateSql).get(0);
        truncateAst.setInsertOverwriteSql(true);
        return getPlan(truncateAst, plannerContext);
    }

    public ExecutionPlan getPlan(SqlNode ast) {
        return getPlan(ast, new PlannerContext());
    }

    public ExecutionPlan getPlan(SqlNode ast, PlannerContext plannerContext) {
        //insert overwrite 需要将DML语句转成DDL语句
        if (ast instanceof SqlInsert && SqlDmlKeyword.convertFromSqlNodeToString(((SqlInsert) ast).getKeywords())
            .stream().anyMatch("OVERWRITE"::equalsIgnoreCase)) {

            return insertOverwriteGetPlan(ast, plannerContext);
        }
        plannerContext.setSqlKind(ast.getKind());
        // enable record
        if (ast instanceof TDDLSqlSelect) {
            plannerContext.setEnableSelectStatistics(
                OutFileParams.IsStatistics(((TDDLSqlSelect) ast).getOutFileParams()));
        }
        // disable direct plan for CTE
        if (ast.getKind() == SqlKind.WITH) {
            plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_DIRECT_PLAN, false);
            plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_POST_PLANNER, false);
        }

        Boolean enableStorageTrigger = plannerContext.getParamManager().getBoolean(
            ConnectionParams.ENABLE_STORAGE_TRIGGER);

        // validate
        SqlConverter converter =
            SqlConverter.getInstance(plannerContext.getSchemaName(), plannerContext.getExecutionContext());

        // init auto part flag
        initConverterAutoPartFlag(plannerContext, enableStorageTrigger, converter);

        SqlNode validatedNode = converter.validate(ast);
        // sqlNode to relNode
        RelNode relNode = converter.toRel(validatedNode, plannerContext);
        //LBAC check
        relNode = checkLBACRowAccess(relNode, validatedNode, plannerContext.getExecutionContext());
        // relNode to drdsRelNode
        ToDrdsRelVisitor toDrdsRelVisitor = new ToDrdsRelVisitor(validatedNode, plannerContext);
        RelNode drdsRelNode = relNode.accept(toDrdsRelVisitor);
        if (plannerContext.getSqlKind().belongsTo(QUERY) && InstConfUtil.getBool(SPM_ENABLE_PQO)) {
            Map<LogicalTableScan, RexNode> predicateMap = getRexNodeTableMap(drdsRelNode);
            if (predicateMap != null && !predicateMap.values().stream()
                .anyMatch(predicate -> !PlanManagerUtil.isSimpleCondition(predicate))) {
                plannerContext.setExprMap(predicateMap);
            }
        }

        RelMetadataQuery mq = drdsRelNode.getCluster().getMetadataQuery();
        if (Boolean
            .parseBoolean(plannerContext.getParamManager().getProps().get(ConnectionProperties.PREPARE_OPTIMIZE))) {
            // does not need to optimize plan in prepare mode
            return constructExecutionPlan(mq.getOriginalRowType(drdsRelNode), drdsRelNode, drdsRelNode,
                ast, validatedNode,
                converter,
                toDrdsRelVisitor, plannerContext,
                ExecutionPlan.DirectMode.NONE);
        }
        RelNode optimizedNode;
        ExecutionPlan.DirectMode directMode;
        RelNode unoptimizedNode = drdsRelNode;
        if (plannerContext.getExternalizePlan() != null) {
            // use plan
            final RelOptCluster cluster =
                SqlConverter.getInstance(plannerContext.getSchemaName(), plannerContext.getExecutionContext())
                    .createRelOptCluster(plannerContext);
            optimizedNode =
                PlanManagerUtil.jsonToRelNode(plannerContext.getExternalizePlan(), cluster,
                    SqlConverter.getInstance(plannerContext.getSchemaName(), plannerContext.getExecutionContext())
                        .getCatalog());
            directMode = ExecutionPlan.DirectMode.NONE;
        } else {
            // optimize
            directMode =
                shouldDirectByTable(toDrdsRelVisitor, validatedNode, plannerContext, unoptimizedNode);
            if (directMode == ExecutionPlan.DirectMode.TABLE_DIRECT ||
                directMode == ExecutionPlan.DirectMode.MULTI_DB_TABLE_DIRECT) {
                optimizedNode = unoptimizedNode;
                if (OptimizerUtils.enableColumnarOptimizer(plannerContext.getParamManager())
                    && CollectionUtils.isNotEmpty(toDrdsRelVisitor.getTableNames())
                    && toDrdsRelVisitor.isAllTableHaveColumnar()) {
                    try {
                        optimizedNode = optimize(unoptimizedNode, plannerContext);
                        // columnar plan
                        if (plannerContext.isUseColumnar()) {
                            directMode = ExecutionPlan.DirectMode.NONE;
                        } else {
                            optimizedNode = unoptimizedNode;
                        }
                    } catch (NotSupportException e) {
                        if (!e.getMessage().contains(SubQueryToSemiJoinRule.NOT_SUPPORT_SUBQUERY_IN_JOIN)) {
                            throw e;
                        }
                    }
                }
            } else {
                optimizedNode = optimize(unoptimizedNode, plannerContext);
                if (canDirectByShardingKey(optimizedNode, plannerContext)) {
                    directMode = ExecutionPlan.DirectMode.SHARDING_KEY_DIRECT;
                } else {
                    directMode = ExecutionPlan.DirectMode.NONE;
                }
            }
        }
        ExecutionPlan executionPlan =
            constructExecutionPlan(mq.getOriginalRowType(drdsRelNode), unoptimizedNode, optimizedNode,
                ast,
                validatedNode,
                converter,
                toDrdsRelVisitor, plannerContext,
                directMode);

        if (!GeneralUtil.isEmpty(plannerContext.getConstantParamIndex())) {
            Map<Integer, ParameterContext> constantParams = new HashMap<>();
            for (Integer index : plannerContext.getConstantParamIndex()) {
                constantParams.put(index, plannerContext.getParams().getFirstParameter().get(index));
            }
            executionPlan.setConstantParams(constantParams);
        }

        checkModifyLimitation(executionPlan, validatedNode, plannerContext.getExecutionContext().isUseHint(),
            plannerContext);

        PostPlanner.getInstance().setSkipPostOptFlag(plannerContext, optimizedNode, directMode.isDirect(),
            toDrdsRelVisitor.existsCannotPushDown());

        return executionPlan;
    }

    private RelNode checkLBACRowAccess(RelNode relNode, SqlNode sqlNode, ExecutionContext executionContext) {
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_LBAC)) {
            return relNode;
        }

        //for unit test
        if (MetaDbDataSource.getInstance() == null) {
            return relNode;
        }

        Set<Pair<String, String>> accessTables = PlanManagerUtil.getTableSetFromAst(sqlNode);
        if (!LBACPrivilegeCheckUtils.isNeedLBACCheck(accessTables, executionContext.getSchemaName())) {
            return relNode;
        }

        LBACRowAccessChecker lbacChecker = new LBACRowAccessChecker();
        return relNode.accept(lbacChecker);
    }

    private void initConverterAutoPartFlag(PlannerContext plannerContext, boolean enableStorageTrigger,
                                           SqlConverter converter) {
        Map<String, Object> userVariables = plannerContext.getExecutionContext().getUserDefVariables();
        Boolean autoPartVariableVal = null;
        String autoPartVariableKey = ConnectionProperties.AUTO_PARTITION.toLowerCase();
        if (userVariables != null && userVariables.containsKey(autoPartVariableKey)) {
            String autoPartVarValStr = GeneralUtil.getPropertyString(userVariables, autoPartVariableKey);
            if (StringUtils.isNumeric(autoPartVarValStr)) {
                autoPartVariableVal =
                    Boolean.valueOf(GeneralUtil.getPropertyLong(userVariables, autoPartVariableKey, 0L) != 0);
            } else {
                autoPartVariableVal = GeneralUtil.getPropertyBoolean(userVariables, autoPartVariableKey, false);
            }
        }

        if (autoPartVariableVal != null && autoPartVariableVal) {
            if (!enableStorageTrigger) {
                converter.enableAutoPartition();
            }
            // Critical: Do unconditional rewrite on original sql(do this to make async ddl normal).
            final List<SQLStatement> statementList =
                SQLUtils.parseStatements(plannerContext.getExecutionContext().getOriginSql(), JdbcConstants.MYSQL,
                    parserFeatures);
            if (1 == statementList.size() && statementList.get(0) instanceof MySqlCreateTableStatement) {
                final MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);
                if (!stmt.isPrefixPartition()) {
                    stmt.setPrefixPartition(true);
                    plannerContext.getExecutionContext().setOriginSql(stmt.toString()); // Do rewrite.
                }
            }
        }
        // Dealing auto partition hint.
        if (plannerContext.getExecutionContext().getParamManager().getBoolean(ConnectionParams.AUTO_PARTITION)) {
            if (!enableStorageTrigger) {
                converter.enableAutoPartition();
            }
        }

        // Dealing new partition database.
        if (DbInfoManager.getInstance().isNewPartitionDb(plannerContext.getSchemaName())) {
            converter.setAutoPartitionDatabase(true);

            /**
             * In mode='AUTO', autoPartition is enable as default
             */
            boolean enableAutoPartitionOnNewPartitionDB = true;
            /**
             * Check if the config of AUTO_PARTITION is set value in MetaDB
             */
            if (plannerContext.getExecutionContext().getParamManager().getProps()
                .containsKey(ConnectionProperties.AUTO_PARTITION)) {
                if (plannerContext.getExecutionContext().getParamManager()
                    .getBoolean(ConnectionParams.AUTO_PARTITION)) {
                    enableAutoPartitionOnNewPartitionDB = true;
                } else {
                    enableAutoPartitionOnNewPartitionDB = false;
                }
            }

            /**
             * Check if the variable of auto_partition is set value in user def variables
             */
            if (autoPartVariableVal != null) {
                if (autoPartVariableVal) {
                    enableAutoPartitionOnNewPartitionDB = true;
                } else {
                    enableAutoPartitionOnNewPartitionDB = false;
                }
            } else {
                /**
                 * User has no set auto_partition=xxx, so ignore
                 */
            }

            /**
             * Check if the database is created with default_single=true
             */
            String schemaName = plannerContext.getSchemaName();
            Boolean isDefaultSingle = DbInfoManager.getInstance().getDbInfo(schemaName).isDefaultSingle();
            if (isDefaultSingle != null && isDefaultSingle) {
                /**
                 * Where default_single=true, all create Tbl without partition by or single
                 * should create as single table with locality='balance_single_table=on'
                 */
                enableAutoPartitionOnNewPartitionDB = false;
                converter.setDefaultSingle(true);
            } else {
                converter.setDefaultSingle(false);
            }

            // Use hint to disable auto partition on new partition table. Or it enable by default.
            if (enableAutoPartitionOnNewPartitionDB && !enableStorageTrigger) {
                converter.enableAutoPartition();
            } else {
                converter.disableAutoPartition();
            }
        }
    }

    /**
     * 是否为分片键点查（主键）
     */
    private boolean canDirectByShardingKey(RelNode optimizedNode, PlannerContext plannerContext) {
        if (plannerContext.hasLocalIndexHint()) {
            return false;
        }

        if (hasDNHint(plannerContext)) {
            return false;
        }

        if (!(optimizedNode instanceof LogicalView) || optimizedNode instanceof LogicalModifyView) {
            return false;
        }
        LogicalView lv = (LogicalView) optimizedNode;
        if (lv.getTableNames().size() > 1 || CollectionUtils.isNotEmpty(lv.getScalarList())) {
            return false;
        }
        if (optimizedNode.getHints() != null && CollectionUtils.isNotEmpty(optimizedNode.getHints().getList())) {
            // 可能用hint直接指定路由
            return false;
        }
        OptimizerContext oc = OptimizerContext.getContext(lv.getSchemaName());
        TddlRuleManager or = oc.getRuleManager();
        TableRule tableRule = or.getTableRule(lv.getLogicalTableName());
        if (tableRule == null) {
            return false;
        }
        List<String> shardColumns = tableRule.getShardColumns();
        Map<String, Comparative> comparatives = lv.getComparative();

        boolean canShard = equalsInAllColumns(shardColumns, comparatives);
        if (!canShard) {
            return false;
        }
        // 判断是否为点查
        final TableMeta primaryTableMeta = oc.getLatestSchemaManager().getTable(lv.getLogicalTableName());
        List<String> pkColumns = new ArrayList<>(primaryTableMeta.getPrimaryKey().size());
        for (ColumnMeta pk : primaryTableMeta.getPrimaryKey()) {
            pkColumns.add(pk.getName());
        }

        return equalsInAllColumns(pkColumns, comparatives);
    }

    /**
     * 判断给定列均在谓词中出现
     * 且谓词都为equal
     */
    public static boolean equalsInAllColumns(List<String> columns,
                                             Map<String, Comparative> comparatives) {
        for (String col : columns) {
            Comparative c = comparatives.get(col);
            if (c == null || !PlannerUtils.comparativeIsASimpleEqual(c)) {
                return false;
            }

            if (!(c.getValue() instanceof RexDynamicParam)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Use HepPlanner and VolcanoPlanner to optimize logical plan.
     *
     * @param input logical plan
     * @return physical plan
     */
    public RelNode optimize(RelNode input, PlannerContext plannerContext) {
        RelNode optimizedPlan = sqlRewriteAndPlanEnumerate(input, plannerContext);
        if (logger.isDebugEnabled()) {
            logger.debug("The optimized relNode: \n" + RelOptUtil.toString(input));
        }
        return optimizedPlan;
    }

    private void checkModifyLimitation(ExecutionPlan executionPlan, SqlNode sqlNode, boolean skip,
                                       PlannerContext plannerContext) {
        RelNode relNode = executionPlan.getPlan();
        if (relNode instanceof LogicalInsert) {
            CheckModifyLimitation.check((LogicalInsert) relNode, sqlNode, skip, plannerContext);
        }
    }

    private RelNode sqlRewriteAndPlanEnumerate(RelNode input, PlannerContext plannerContext) {
        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> {
                plannerContext.setEvalFuncFromExecutionContext();
                x.addSnapshot("Start", input, plannerContext);
            });

        RelNode logicalOutput = optimizeBySqlWriter(input, plannerContext);
        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.addSnapshot("PlanEnumerate", logicalOutput, plannerContext));

        RelNode bestPlan = optimizeByPlanEnumerator(input, logicalOutput, plannerContext);

        // finally we should clear the planner to release memory
        bestPlan.getCluster().getPlanner().clear();
        bestPlan.getCluster().invalidateMetadataQuery();
        return bestPlan;
    }

    public RelNode optimizeBySqlWriter(RelNode input, PlannerContext plannerContext) {
        //validate heuristic order if there are more than joins
        CountVisitor countVisitor = new CountVisitor();
        countVisitor.visit(input);
        plannerContext.setShouldUseHeuOrder(countVisitor.getJoinCount() >=
            plannerContext.getParamManager().getInt(ConnectionParams.RBO_HEURISTIC_JOIN_REORDER_LIMIT));

        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.addSnapshot("Start", input, plannerContext));

        HepProgramBuilder hepPgmBuilder = new HepProgramBuilder();
        hepPgmBuilder.addMatchOrder(HepMatchOrder.ARBITRARY);

        for (SQL_REWRITE_RULE_PHASE r : SQL_REWRITE_RULE_PHASE.values()) {
            hepPgmBuilder.addMatchOrder(r.getMatchOrder());
            hepPgmBuilder.addGroupBegin();
            for (ImmutableList<RelOptRule> relOptRuleList : r.getCollectionList()) {
                hepPgmBuilder.addRuleCollection(relOptRuleList);
            }
            for (RelOptRule relOptRule : r.getSingleList()) {
                hepPgmBuilder.addRuleInstance(relOptRule);
            }
            hepPgmBuilder.addGroupEnd();
        }

        final HepPlanner planner = new HepPlanner(hepPgmBuilder.build(), plannerContext);
        planner.setRoot(input);
        return planner.findBestExp();
    }

    public RelNode optimizeByPlanEnumerator(RelNode originInput, RelNode input, PlannerContext plannerContext) {
        VolcanoPlanner volcanoPlanner = (VolcanoPlanner) input.getCluster().getPlanner();
        volcanoPlanner.clear();

        ParamManager paramManager = plannerContext.getParamManager();
        CountVisitor countVisitor = new CountVisitor();
        input.accept(countVisitor);

        ExchangeOptimizerType exchangeOptimizerType = determineWorkload(originInput, input, plannerContext, true);
        RelNode output = input;
        if (exchangeOptimizerType != ExchangeOptimizerType.COLUMNAR) {
            // cbo config, e.g. pass_through
            configureCBO(volcanoPlanner, countVisitor, paramManager, plannerContext);
            // add rules used by cbo
            addCBORule(volcanoPlanner, countVisitor, paramManager, plannerContext);
            // optimize
            output = optimizeByCBO(input, volcanoPlanner, plannerContext);
            // optimize row plan after cbo, e.g. transform tableLookup to bka join
            output = optimizeRowAfterCBO(output, plannerContext);
            // determine workload of the query
            exchangeOptimizerType = determineWorkload(originInput, output, plannerContext, false);
        }

        switch (exchangeOptimizerType) {
        case MPP:
            output = optimizeByMppPlan(output, plannerContext);
            break;
        case COLUMNAR:
            // in columnar optimizer, there will be no tableLookup
            output = optimizeByColumnarPlan(originInput, paramManager, plannerContext);
            break;
        case SMP:
        default:
            output = optimizeBySmpPlan(output, plannerContext);
        }

        // last step rbo in optimizer, e.g. physical sql optimize
        output = optimizeByExpandViewPlan(output, plannerContext);

        if (exchangeOptimizerType == ExchangeOptimizerType.COLUMNAR) {
            output = addPartitionWiseTrait(output,
                plannerContext.getParamManager().getBoolean(ConnectionParams.JOIN_KEEP_PARTITION));
        }
        // deal with subquery
        SubQueryPlanEnumerator subQueryPlanEnumerator = new SubQueryPlanEnumerator(plannerContext.isInSubquery());
        output = output.accept(subQueryPlanEnumerator);

        PlanManagerUtil.applyCache(output);
        return output;
    }

    private void configureCBO(VolcanoPlanner volcanoPlanner,
                              CountVisitor countVisitor,
                              ParamManager paramManager,
                              PlannerContext plannerContext) {
        volcanoPlanner.setTopDownOpt(true);

        int joinCount = countVisitor.getJoinCount();
        plannerContext.setJoinCount(joinCount);
        if (joinCount > 0) {
            plannerContext.enableSPM(true);
        }

        boolean enableBranchAndBoundOptimization =
            paramManager.getBoolean(ConnectionParams.ENABLE_BRANCH_AND_BOUND_OPTIMIZATION);
        int volcanoStartUpCostJoinLimit = paramManager.getInt(ConnectionParams.CBO_START_UP_COST_JOIN_LIMIT);
        volcanoPlanner.setStartUpCostOpt(countVisitor.getLimitCount() > 0
            && joinCount <= volcanoStartUpCostJoinLimit);
        volcanoPlanner.setEnableBranchAndBound(enableBranchAndBoundOptimization);
        boolean enablePassThrough = paramManager.getBoolean(ConnectionParams.ENABLE_PASS_THROUGH_TRAIT);
        volcanoPlanner.setEnablePassThrough(enablePassThrough);
        boolean enableDerive = paramManager.getBoolean(ConnectionParams.ENABLE_DERIVE_TRAIT);
        volcanoPlanner.setEnableDerive(enableDerive);
        volcanoPlanner.setEnableColumnar(false);
        // CBO_RESTRICT_PUSH_JOIN_LIMIT < 0 means disable the restriction
        boolean enableRestrictCBOPushJoin = paramManager.getInt(ConnectionParams.CBO_RESTRICT_PUSH_JOIN_LIMIT) >= 0
            && joinCount >= paramManager
            .getInt(ConnectionParams.CBO_RESTRICT_PUSH_JOIN_LIMIT);
        plannerContext.setRestrictCboPushJoin(enableRestrictCBOPushJoin);

        // try to prune gsi if there is no join
        if (paramManager.getBoolean(ConnectionParams.ENABLE_INDEX_SELECTION_PRUNE)) {
            plannerContext.setGsiPrune(countVisitor.getJoinCount() == 0);
        }
    }

    private RelNode optimizeByCBO(RelNode input,
                                  VolcanoPlanner volcanoPlanner,
                                  PlannerContext plannerContext) {
        RelNode newInput;
        if (!input.getTraitSet().contains(DrdsConvention.INSTANCE)) {
            newInput =
                volcanoPlanner.changeTraits(input, input.getTraitSet().simplify().replace(DrdsConvention.INSTANCE));
        } else {
            // some nodes already in DrdsConvention
            newInput = input;
        }

        volcanoPlanner.setRoot(newInput);
        RelNode output;
        try {
            output = getCheapestFractionalPlan(volcanoPlanner);
        } catch (RelOptPlanner.CannotPlanException e) {
            logger.error(e);
            throw new RuntimeException("Sql could not be implemented");
        } finally {
            volcanoPlanner.clear();
            plannerContext.setRestrictCboPushJoin(false);
        }
        if (plannerContext.isEnableRuleCounter()) {
            plannerContext.setRuleCount(volcanoPlanner.getRuleCount());
        }

        if (plannerContext.getJoinCount() > 0) {
            plannerContext.enableSPM(true);
        }
        return output;
    }

    /**
     * determine the workload of the sql and the optimizer to be used
     *
     * @param input a physical plan optimized
     * @param plannerContext context of planner
     * @return optimizer to be used for the sql
     */
    private ExchangeOptimizerType determineWorkload(RelNode originInput,
                                                    RelNode input,
                                                    PlannerContext plannerContext,
                                                    boolean ignoreRowCBO) {
        // determine TP or AP
        try {
            plannerContext.setWorkloadType(WorkloadType.valueOf(
                plannerContext.getParamManager().getString(ConnectionParams.WORKLOAD_TYPE).toUpperCase()));
        } catch (Throwable t) {
            // don't get workload type from cost if row cbo is not invoked yet
            if (!ignoreRowCBO) {
                plannerContext.setWorkloadType(determineWorkloadType(input, input.getCluster().getMetadataQuery()));
            }
        }

        // columnar mode
        if (ConfigDataMode.isColumnarMode()) {
            return determineOptimizerTypeForColumnarNode(originInput, input, plannerContext);
        } else {
            return determineOptimizerTypeForMasterNode(originInput, input, plannerContext);
        }
    }

    private ExchangeOptimizerType determineOptimizerTypeForColumnarNode(RelNode originInput,
                                                                        RelNode input,
                                                                        PlannerContext plannerContext) {
        if ((!MppPlanCheckers.supportsMppPlan(input, plannerContext, plannerContext.getExecutionContext(),
            MppPlanCheckers.BASIC_CHECKERS))) {
            return ExchangeOptimizerType.SMP;
        } else {
            if ((MppPlanCheckers.supportsMppPlan(input, plannerContext, plannerContext.getExecutionContext(),
                MppPlanCheckers.SAMPLE_HINT_CHECKER,
                MppPlanCheckers.ENABLE_COLUMNAR_CHECKER,
                MppPlanCheckers.COLUMNAR_TRANSACTION_CHECKER,
                MppPlanCheckers.UPDATE_CHECKER) &&
                CBOUtil.allTablesHaveColumnar(originInput, plannerContext.getExecutionContext()))) {
                return ExchangeOptimizerType.COLUMNAR;
            }
            return ExchangeOptimizerType.SMP;
        }
    }

    private ExchangeOptimizerType determineOptimizerTypeForMasterNode(RelNode originInput,
                                                                      RelNode input,
                                                                      PlannerContext plannerContext) {
        ExchangeOptimizerType specificType = ExchangeOptimizerType.getType(
            plannerContext.getParamManager().getString(ConnectionParams.OPTIMIZER_TYPE));
        if (specificType != null) {
            return specificType;
        }

        // firstly, smp or columnar_smp
        if ((WorkloadType.AP != plannerContext.getWorkloadType()) ||
            (!MppPlanCheckers.supportsMppPlan(input, plannerContext, plannerContext.getExecutionContext(),
                MppPlanCheckers.BASIC_CHECKERS))) {
            return ExchangeOptimizerType.SMP;
        }

        // secondly, columnar
        if ((MppPlanCheckers.supportsMppPlan(input, plannerContext, plannerContext.getExecutionContext(),
            MppPlanCheckers.SAMPLE_HINT_CHECKER,
            MppPlanCheckers.ENABLE_COLUMNAR_CHECKER,
            MppPlanCheckers.COLUMNAR_TRANSACTION_CHECKER,
            MppPlanCheckers.UPDATE_CHECKER,
            MppPlanCheckers.CORRELATE_CHECKER) &&
            CBOUtil.allTablesHaveColumnar(originInput, plannerContext.getExecutionContext()))) {
            return ExchangeOptimizerType.COLUMNAR;
        }

        // finally, mpp
        return ExchangeOptimizerType.MPP;
    }

    private static RelNode getCheapestFractionalPlan(VolcanoPlanner volcanoPlanner) {
        RelNode cheapestTotalCostPlan = volcanoPlanner.findBestExp();
        if (!PlannerContext.getPlannerContext(cheapestTotalCostPlan).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_START_UP_COST) || !volcanoPlanner.isStartUpCostOpt()) {
            return cheapestTotalCostPlan;
        }
        RelNode root = volcanoPlanner.getRoot();
        CheapestFractionalPlanReplacer replacer = new CheapestFractionalPlanReplacer(volcanoPlanner);
        final RelNode cheapest = replacer.visit(root, 1);
        return cheapest;
    }

    private void addCBORule(RelOptPlanner relOptPlanner, CountVisitor countVisitor,
                            ParamManager paramManager,
                            PlannerContext plannerContext) {
        int volcanoTooManyJoinSizeLimit = paramManager.getInt(ConnectionParams.CBO_TOO_MANY_JOIN_LIMIT);
        int volcanoLeftDeepJoinSizeLimit = paramManager.getInt(ConnectionParams.CBO_LEFT_DEEP_TREE_JOIN_LIMIT);
        int volcanoZigZagJoinSizeLimit = paramManager.getInt(ConnectionParams.CBO_ZIG_ZAG_TREE_JOIN_LIMIT);
        int volcanoBushyJoinSizeLimit = paramManager.getInt(ConnectionParams.CBO_BUSHY_TREE_JOIN_LIMIT);
        int volcanoJoinTableLookupTransposeLimit =
            paramManager.getInt(ConnectionParams.CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT);
        int volcanoStartUpCostJoinLimit = paramManager.getInt(ConnectionParams.CBO_START_UP_COST_JOIN_LIMIT);
        boolean enableSemiJoinReorder = paramManager.getBoolean(ConnectionParams.ENABLE_SEMI_JOIN_REORDER);
        boolean enableOuterJoinReorder = paramManager.getBoolean(ConnectionParams.ENABLE_OUTER_JOIN_REORDER);

        int joinCount = plannerContext.getJoinCount();

        List<RelOptRule> cboReorderRuleSet = new ArrayList<>();

        if (joinCount <= volcanoBushyJoinSizeLimit) {
            cboReorderRuleSet.addAll(RuleToUse.CBO_BUSHY_TREE_JOIN_REORDER_RULE);
        } else if (joinCount <= volcanoZigZagJoinSizeLimit) {
            cboReorderRuleSet.addAll(RuleToUse.CBO_ZIG_ZAG_TREE_JOIN_REORDER_RULE);
        } else if (joinCount <= volcanoLeftDeepJoinSizeLimit) {
            cboReorderRuleSet.addAll(RuleToUse.CBO_LEFT_DEEP_TREE_JOIN_REORDER_RULE);
        } else if (joinCount <= volcanoTooManyJoinSizeLimit) {
            cboReorderRuleSet.addAll(RuleToUse.CBO_TOO_MANY_JOIN_REORDER_RULE);
        }

        if (joinCount <= volcanoJoinTableLookupTransposeLimit) {
            cboReorderRuleSet.addAll(RuleToUse.CBO_JOIN_TABLELOOKUP_REORDER_RULE);
        }

        for (RelOptRule rule : cboReorderRuleSet) {
            /** remove SemiJoinReorderRule when disable semi join reorder */
            if ((!enableSemiJoinReorder || countVisitor.getSemiJoinCount() == 0)
                && (rule instanceof CBOLogicalSemiJoinLogicalJoinTransposeRule
                || rule instanceof SemiJoinSemiJoinTransposeRule || rule instanceof SemiJoinProjectTransposeRule)) {
                continue;
            }

            /** remove OuterJoinReorderRule when disable outer join reorder */
            if ((!enableOuterJoinReorder || countVisitor.getOuterJoinCount() == 0)
                && (rule instanceof OuterJoinAssocRule || rule instanceof OuterJoinLAsscomRule)) {
                continue;
            }

            relOptPlanner.addRule(rule);
        }

        boolean enableCBOPushJoin = paramManager.getBoolean(ConnectionParams.ENABLE_CBO_PUSH_JOIN);
        boolean enableCBOPushAgg = paramManager.getBoolean(ConnectionParams.ENABLE_CBO_PUSH_AGG);

        boolean enableHashJoin = paramManager.getBoolean(ConnectionParams.ENABLE_HASH_JOIN);
        boolean enableBKAJoin = paramManager.getBoolean(ConnectionParams.ENABLE_BKA_JOIN);
        boolean enableNLJoin = paramManager.getBoolean(ConnectionParams.ENABLE_NL_JOIN);
        boolean enableSortMergeJoin = paramManager.getBoolean(ConnectionParams.ENABLE_SORT_MERGE_JOIN) &&
            !plannerContext.getRestrictCboPushJoin();
        boolean enableSemiHashJoin = paramManager.getBoolean(ConnectionParams.ENABLE_SEMI_HASH_JOIN);
        boolean enableSemiNLJoin = paramManager.getBoolean(ConnectionParams.ENABLE_SEMI_NL_JOIN);
        boolean enableMaterializedSemiJoin = paramManager.getBoolean(ConnectionParams.ENABLE_MATERIALIZED_SEMI_JOIN);
        boolean enableSemiSortMergeJoin = paramManager.getBoolean(ConnectionParams.ENABLE_SEMI_SORT_MERGE_JOIN) &&
            !plannerContext.getRestrictCboPushJoin();
        boolean enableHashAGG = paramManager.getBoolean(ConnectionParams.ENABLE_HASH_AGG);
        boolean enableSortAgg = paramManager.getBoolean(ConnectionParams.ENABLE_SORT_AGG);
        ImmutableList<RelOptRule> rules = RuleToUse.CBO_BASE_RULE;

        if (plannerContext.getExtraCmds().containsKey(ConnectionProperties.ENABLE_SORT_MERGE_JOIN)) {
            enableSortMergeJoin = Boolean.parseBoolean(
                String.valueOf(plannerContext.getExtraCmds().get(ConnectionProperties.ENABLE_SORT_MERGE_JOIN)));
        }
        if (plannerContext.getExtraCmds().containsKey(ConnectionProperties.ENABLE_SEMI_SORT_MERGE_JOIN)) {
            enableSemiSortMergeJoin = Boolean.parseBoolean(
                String.valueOf(plannerContext.getExtraCmds().get(ConnectionProperties.ENABLE_SEMI_SORT_MERGE_JOIN)));
        }

        if (!plannerContext.isAutoCommit() || plannerContext.getSqlKind().belongsTo(SqlKind.DML)) {
            // Sort-Merge Join cannot work in transaction
            enableSortMergeJoin = false;
            enableSemiSortMergeJoin = false;
        }

        for (RelOptRule rule : rules) {
            if (!enableBKAJoin && rule instanceof LogicalJoinToBKAJoinRule) {
                continue;
            }

            if (!enableNLJoin && rule instanceof LogicalJoinToNLJoinRule) {
                continue;
            }

            if (!enableHashJoin && rule instanceof LogicalJoinToHashJoinRule) {
                continue;
            }

            if (!enableSortMergeJoin && rule instanceof LogicalJoinToSortMergeJoinRule) {
                continue;
            }

            if ((!enableSemiHashJoin || countVisitor.getSemiJoinCount() == 0)
                && (rule instanceof LogicalSemiJoinToSemiHashJoinRule)) {
                continue;
            }

            if ((!enableSemiNLJoin || countVisitor.getSemiJoinCount() == 0)
                && rule instanceof LogicalSemiJoinToSemiNLJoinRule) {
                continue;
            }

            if ((!enableMaterializedSemiJoin || countVisitor.getSemiJoinCount() == 0)
                && rule instanceof LogicalSemiJoinToMaterializedSemiJoinRule) {
                continue;
            }

            if ((!enableSemiSortMergeJoin || countVisitor.getSemiJoinCount() == 0)
                && rule instanceof LogicalSemiJoinToSemiSortMergeJoinRule) {
                continue;
            }

            if (!enableHashAGG && rule instanceof LogicalAggToHashAggRule) {
                continue;
            }

            if (!enableSortAgg && rule instanceof LogicalAggToSortAggRule) {
                continue;
            }

            if ((countVisitor.getSortCount() == 0 || joinCount > volcanoStartUpCostJoinLimit)
                && (rule instanceof DrdsSortJoinTransposeRule || rule instanceof DrdsSortProjectTransposeRule)) {
                continue;
            }

            if (!enableCBOPushJoin &&
                (rule instanceof CBOPushSemiJoinRule || rule instanceof CBOPushJoinRule)) {
                continue;
            }

            if (!enableCBOPushAgg && (rule instanceof DrdsAggregateJoinTransposeRule)) {
                continue;
            }

            relOptPlanner.addRule(rule);
        }
    }

    private RelNode optimizeByColumnarPlan(RelNode input,
                                           ParamManager paramManager,
                                           PlannerContext plannerContext) {
        try {
            plannerContext.setWorkloadType(WorkloadType.valueOf(
                plannerContext.getParamManager().getString(ConnectionParams.WORKLOAD_TYPE).toUpperCase()));
        } catch (Throwable t) {
            plannerContext.setWorkloadType(WorkloadType.AP);
        }

        RelNode columnarOutput = optimizeByColumnarRBO(input, paramManager, plannerContext);
        CBOUtil.assignColumnarMaxShardCnt(columnarOutput, plannerContext);

        // cbo
        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.addSnapshot("Columnar CBO", columnarOutput, plannerContext));
        VolcanoPlanner volcanoPlanner = (VolcanoPlanner) columnarOutput.getCluster().getPlanner();
        volcanoPlanner.clear();

        CountVisitor countVisitor = new CountVisitor();
        columnarOutput.accept(countVisitor);

        addColumnarCBORule(volcanoPlanner, paramManager, countVisitor);
        volcanoPlanner.setTopDownOpt(true);
        volcanoPlanner.setStartUpCostOpt(false);
        volcanoPlanner.setEnableColumnar(true);
        volcanoPlanner.setEnableBranchAndBound(
            paramManager.getBoolean(ConnectionParams.ENABLE_BRANCH_AND_BOUND_OPTIMIZATION)
                && (!CheckJoinHint.useJoinHint(plannerContext)));

        plannerContext.setUseColumnar(true);

        RelTraitSet newTraitSet = columnarOutput.getTraitSet().simplify().replace(DrdsConvention.INSTANCE);
        RelNode newInput = volcanoPlanner.changeTraits(columnarOutput, newTraitSet);
        volcanoPlanner.setRoot(newInput);

        RelNode output;
        try {
            output = volcanoPlanner.findBestExp();
        } catch (RelOptPlanner.CannotPlanException e) {
            logger.error(e);
            throw new RuntimeException("Columnar Sql could not be implemented");
        } finally {
            volcanoPlanner.clear();
            volcanoPlanner.setEnableColumnar(false);
        }

        if (plannerContext.getParamManager().getBoolean(
            ConnectionParams.ENABLE_COLUMNAR_AFTER_CBO_PLANNER)) {
            output = optimizeColumnarAfterCBO(output, plannerContext);
        }

        output = output.accept(new RemoveFixedCostVisitor());
        plannerContext.getExtraCmds().put(ConnectionProperties.JOIN_HINT, null);

        return output;
    }

    private RelNode optimizeByColumnarRBO(RelNode input,
                                          ParamManager paramManager,
                                          PlannerContext plannerContext) {
        // rbo
        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.addSnapshot("Columnar RBO", input, plannerContext));

        HepProgramBuilder hepPgmBuilder = new HepProgramBuilder();
        hepPgmBuilder.addMatchOrder(HepMatchOrder.ARBITRARY);

        Map<String, Object> valueRecords = configureColumnarRBO(paramManager, plannerContext);
        for (SQL_REWRITE_RULE_PHASE r : SQL_REWRITE_RULE_PHASE.values()) {
            hepPgmBuilder.addMatchOrder(r.getMatchOrder());
            hepPgmBuilder.addGroupBegin();
            for (ImmutableList<RelOptRule> relOptRuleList : r.getCollectionList()) {
                hepPgmBuilder.addRuleCollection(relOptRuleList);
            }
            for (RelOptRule relOptRule : r.getSingleList()) {
                hepPgmBuilder.addRuleInstance(relOptRule);
            }
            hepPgmBuilder.addGroupEnd();
        }
        final HepPlanner planner = new HepPlanner(hepPgmBuilder.build(), plannerContext);
        planner.setRoot(input);
        RelNode rboOutput = planner.findBestExp();
        cleanConfigureColumnarRBO(valueRecords, plannerContext);

        // replace table scan to columnar table scan
        final CBOUtil.ColumnarScanReplacer columnarScanReplacer = new CBOUtil.ColumnarScanReplacer(plannerContext);
        return CBOUtil.optimizeByPullProject(rboOutput.accept(columnarScanReplacer), plannerContext);
    }

    private Map<String, Object> configureColumnarRBO(ParamManager paramManager,
                                                     PlannerContext plannerContext) {
        Map<String, Object> valueRecords = Maps.newHashMap();
        valueRecords.put(ConnectionProperties.ENABLE_PUSH_JOIN,
            paramManager.getBoolean(ConnectionParams.ENABLE_PUSH_JOIN));
        valueRecords.put(ConnectionProperties.ENABLE_PUSH_CORRELATE,
            paramManager.getBoolean(ConnectionParams.ENABLE_PUSH_CORRELATE));
        valueRecords.put(ConnectionProperties.ENABLE_PUSH_AGG,
            paramManager.getBoolean(ConnectionParams.ENABLE_PUSH_AGG));
        valueRecords.put(ConnectionProperties.ENABLE_PUSH_SORT,
            paramManager.getBoolean(ConnectionParams.ENABLE_PUSH_SORT));
        valueRecords.put(ConnectionProperties.ENABLE_EXPAND_DISTINCTAGG,
            paramManager.getBoolean(ConnectionParams.ENABLE_EXPAND_DISTINCTAGG));
        valueRecords.put(ConnectionProperties.PUSH_CORRELATE_MATERIALIZED_LIMIT,
            paramManager.getInt(ConnectionParams.PUSH_CORRELATE_MATERIALIZED_LIMIT));

        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_PUSH_JOIN, false);
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_PUSH_CORRELATE, false);
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_PUSH_AGG, false);
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_PUSH_SORT, false);
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_EXPAND_DISTINCTAGG, true);
        plannerContext.getExtraCmds().put(ConnectionProperties.PUSH_CORRELATE_MATERIALIZED_LIMIT, 0);
        plannerContext.setShouldUseHeuOrder(false);
        return valueRecords;
    }

    private Map<String, Object> cleanConfigureColumnarRBO(Map<String, Object> valueRecords,
                                                          PlannerContext plannerContext) {

        for (Map.Entry<String, Object> entry : valueRecords.entrySet()) {
            plannerContext.getExtraCmds().put(entry.getKey(), entry.getValue());
        }
        return valueRecords;
    }

    private void addColumnarCBORule(RelOptPlanner relOptPlanner, ParamManager paramManager, CountVisitor countVisitor) {
        boolean enableSemiJoinReorder = paramManager.getBoolean(ConnectionParams.ENABLE_SEMI_JOIN_REORDER);
        boolean enableOuterJoinReorder = paramManager.getBoolean(ConnectionParams.ENABLE_OUTER_JOIN_REORDER);

        if (countVisitor.getJoinCount() <= paramManager.getInt(ConnectionParams.COLUMNAR_CBO_TOO_MANY_JOIN_LIMIT)) {
            for (RelOptRule rule : RuleToUse.CBO_BUSHY_TREE_JOIN_REORDER_RULE) {
                /* remove SemiJoinReorderRule when disable semi join reorder */
                if ((!enableSemiJoinReorder)
                    && (rule instanceof CBOLogicalSemiJoinLogicalJoinTransposeRule
                    || rule instanceof SemiJoinSemiJoinTransposeRule || rule instanceof SemiJoinProjectTransposeRule)) {
                    continue;
                }

                /* remove OuterJoinReorderRule when disable outer join reorder */
                if ((!enableOuterJoinReorder)
                    && (rule instanceof OuterJoinAssocRule || rule instanceof OuterJoinLAsscomRule)) {
                    continue;
                }
                relOptPlanner.addRule(rule);
            }
        }

        for (RelOptRule rule : RuleToUse.COLUMNAR_CBO_RULE) {
            relOptPlanner.addRule(rule);
        }
    }

    private RelNode optimizeByMppPlan(RelNode input, PlannerContext plannerContext) {
        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.addSnapshot("MPP", input, plannerContext));
        VolcanoPlanner volcanoPlanner = (VolcanoPlanner) input.getCluster().getPlanner();
        volcanoPlanner.clear();

        if (plannerContext.getJoinCount() > plannerContext.getParamManager()
            .getInt(ConnectionParams.CBO_TOO_MANY_JOIN_LIMIT)) {
            volcanoPlanner.setEnableDerive(false);
        }

        for (RelOptRule rule : RuleToUse.MPP_CBO_RULE) {
            volcanoPlanner.addRule(rule);
        }

        // TODO: should we use collation as PlanEnumerator input?
        RelTraitSet newTraitSet = input.getTraitSet().simplify().replace(MppConvention.INSTANCE);
        RelNode newInput = volcanoPlanner.changeTraits(input, newTraitSet);
        volcanoPlanner.setRoot(newInput);

        RelNode output;
        try {
            output = volcanoPlanner.findBestExp();
        } catch (RelOptPlanner.CannotPlanException e) {
            logger.error(e);
            throw new RuntimeException("MPP Sql could not be implemented");
        } finally {
            volcanoPlanner.clear();
        }

        output = optimizeWithRuntimeFilter(output, plannerContext);
        return output;
    }

    private RelNode optimizeWithRuntimeFilter(RelNode input, PlannerContext plannerContext) {
        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.addSnapshot("MPP RBO AFTER CBO", input, plannerContext));
        boolean enableRuntimeFilter = plannerContext.getParamManager().getBoolean(
            ConnectionParams.ENABLE_RUNTIME_FILTER);
        boolean pushRuntimeFilter = plannerContext.getParamManager().getBoolean(
            ConnectionParams.ENABLE_PUSH_RUNTIME_FILTER_SCAN);

        HepProgramBuilder builder = new HepProgramBuilder();
        if (enableRuntimeFilter) {
            builder.addGroupBegin();
            builder.addRuleCollection(RuleToUse.RUNTIME_FILTER);
            if (pushRuntimeFilter) {
                // push filter
                builder.addRuleInstance(PushBloomFilterRule.LOGICALVIEW);
            }
            builder.addGroupEnd();
        }

        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(input);
        return planner.findBestExp();

    }

    private RelNode optimizeColumnarAfterCBO(RelNode input, PlannerContext plannerContext) {
        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.addSnapshot("Columnar RBO AFTER CBO", input, plannerContext));
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        builder.addGroupBegin();
        builder.addRuleInstance(PhyTwoPhaseAggRule.INSTANCE);
        builder.addRuleInstance(PhyTwoPhaseAggRule.PROJECT);
        builder.addGroupEnd();

        builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        builder.addGroupBegin();
        builder.addRuleInstance(AggregateProjectMergeRule.INSTANCE);
        builder.addRuleInstance(JoinConditionPruningRule.INSTANCE);
        builder.addRuleInstance(new COLProjectHashJoinTransposeRule(
            plannerContext.getParamManager().getInt(ConnectionParams.PUSH_PROJECT_INPUT_REF_THRESHOLD)));
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(input);
        RelNode output = planner.findBestExp();

        return output;
    }

    private RelNode optimizeByExpandViewPlan(RelNode input, PlannerContext plannerContext) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addGroupBegin();
        builder.addRuleCollection(RuleToUse.EXPAND_VIEW_PLAN);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(FilterReorderRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(OptimizePhySqlRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(FilterReorderRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(OptimizePhySqlRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(GenXplanRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner hepPlanner = new HepPlanner(builder.build(), plannerContext);
        hepPlanner.setRoot(input);
        return hepPlanner.findBestExp();
    }

    private RelNode optimizeBySmpPlan(RelNode input, PlannerContext plannerContext) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addGroupBegin();
        // for stable performance, push Sort for smp plan
        builder.addRuleInstance(DrdsSortProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addRuleInstance(PushSortRule.SQL_REWRITE);
        builder.addRuleInstance(PushProjectRule.INSTANCE);
        builder.addRuleInstance(PushFilterRule.LOGICALVIEW);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(ProjectSortTransitiveRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleCollection(RuleToUse.TDDL_SHARDING_RULE);
        builder.addRuleInstance(PushModifyRule.OPTIMIZE_MODIFY_TOP_N_RULE);
        builder.addGroupEnd();
        HepPlanner hepPlanner = new HepPlanner(builder.build(), plannerContext);
        hepPlanner.setRoot(input);
        RelNode output = hepPlanner.findBestExp();
        return output;
    }

    private RelNode addPartitionWiseTrait(RelNode input, boolean joinKeepPartition) {
        return new PartitionWiseAssigner(input, joinKeepPartition).assign();
    }

    private RelNode optimizeRowAfterCBO(RelNode input, PlannerContext plannerContext) {
        HepProgramBuilder builder = new HepProgramBuilder();
        // expand table lookup
        builder.addGroupBegin();
        builder.addRuleCollection(RuleToUse.EXPAND_TABLE_LOOKUP);
        builder.addGroupEnd();
        builder.addGroupBegin();
        // push filter
        builder.addRuleInstance(PushFilterRule.LOGICALVIEW);
        builder.addRuleInstance(PushFilterRule.MERGE_SORT);
        builder.addRuleInstance(PushFilterRule.LOGICALUNION);
        // push project
        builder.addRuleInstance(PushProjectRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addGroupEnd();
        // NOTE: we should preserve bottom up order to convert while using cbo convert rule.
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(SMPLogicalViewConvertRule.INSTANCE);
        builder.addRuleInstance(ExpandLogicalJoinToBKAJoinRule.LOGICALVIEW_RIGHT);
        builder.addRuleInstance(ExpandLogicalJoinToBKAJoinRule.LOGICALVIEW_NOT_RIGHT);
        builder.addRuleInstance(SMPLogicalViewConvertRule.INSTANCE);
        builder.addRuleInstance(DrdsProjectConvertRule.SMP_INSTANCE);
        builder.addRuleInstance(DrdsFilterConvertRule.SMP_INSTANCE);
        builder.addRuleInstance(DrdsCorrelateConvertRule.SMP_INSTANCE);
        builder.addRuleInstance(DrdsOutFileConvertRule.INSTANCE);
        builder.addGroupEnd();
        // push agg
        builder.addGroupBegin();
        builder.addRuleInstance(PhyPushAggRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(OptimizeLogicalViewRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner hepPlanner = new HepPlanner(builder.build(), plannerContext);
        hepPlanner.setRoot(input);
        //  we must enable BKAJoin to convert tableLookup to BKAJoin
        Object originValue = plannerContext.getParamManager().get(ConnectionProperties.ENABLE_BKA_JOIN);
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_BKA_JOIN, true);
        RelNode output = hepPlanner.findBestExp();
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_BKA_JOIN, originValue);
        return output;
    }

    /**
     * 针对单表和广播表的direct转发
     */
    protected ExecutionPlan.DirectMode shouldDirectByTable(ToDrdsRelVisitor toDrdsRelVisitor, SqlNode sqlNode,
                                                           PlannerContext plannerContext, RelNode unoptimizedNode) {
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_DIRECT_PLAN) ||
            plannerContext.isUseColumnar()) {
            return ExecutionPlan.DirectMode.NONE;
        }
        if (plannerContext.hasLocalIndexHint()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (hasDNHint(plannerContext)) {
            return ExecutionPlan.DirectMode.NONE;
        }

        final ExecutionStrategy strategy = ExecutionStrategy.fromHint(plannerContext.getExecutionContext());
        if (ExecutionStrategy.LOGICAL == strategy) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isExistForceColumnar()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.existsCannotPushDown()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isExistsGroupingSets()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.existsOSSTable()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        //should not generate direct plan for any table with gsi
        if (toDrdsRelVisitor.isModifyGsiTable()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isContainOnlineModifyColumnTable()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isContainGeneratedColumn()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isModifyForeignKey()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isExistsCheckSum()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isExistsUnpushableAgg()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isExistsCheckSumV2()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (toDrdsRelVisitor.isOutFileStatistics()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        //update / delete limit m,n; mysql 不支持，不下推执行
        if (toDrdsRelVisitor.isModifyWithLimitOffset()) {
            return ExecutionPlan.DirectMode.NONE;
        }

        if (RelUtils.existUnPushableLastInsertId(unoptimizedNode, toDrdsRelVisitor)) {
            return ExecutionPlan.DirectMode.NONE;
        }

        final boolean onlySingleOrOnlyBroadcast = toDrdsRelVisitor.isAllTableBroadcast()
            || toDrdsRelVisitor.isAllTableSingleNoBroadcast();
        if (toDrdsRelVisitor.isDirectInTheSameDB()
            && (sqlNode.getKind() != SqlKind.DELETE || ((SqlDelete) sqlNode).singleTable()
            || onlySingleOrOnlyBroadcast)
            && (sqlNode.getKind() != SqlKind.UPDATE || ((SqlUpdate) sqlNode).singleTable()
            || onlySingleOrOnlyBroadcast)
            && !toDrdsRelVisitor.isContainScaleOutWritableTable()
            && !toDrdsRelVisitor.isContainReplicateWriableTable() &&
            sqlNode.getKind() != SqlKind.CREATE_MATERIALIZED_VIEW
            && sqlNode.getKind() != SqlKind.REFRESH_MATERIALIZED_VIEW
            && sqlNode.getKind() != SqlKind.DROP_MATERIALIZED_VIEW) {
            return ExecutionPlan.DirectMode.TABLE_DIRECT;
        }

        boolean supportPushOnDifferDB = plannerContext.getParamManager().getBoolean(
            ConnectionParams.SUPPORT_PUSH_AMONG_DIFFERENT_DB);

        if (plannerContext.getExecutionContext().isAutoCommit()
            && supportPushOnDifferDB
            && !sqlNode.isA(DML)
            && toDrdsRelVisitor.isDirectInDifferentDB()
            && toDrdsRelVisitor.getSchemaNames().size() > 1
            && !toDrdsRelVisitor.isContainScaleOutWritableTable()
            && !toDrdsRelVisitor.isContainReplicateWriableTable()
            && sqlNode.getKind() != SqlKind.CREATE_MATERIALIZED_VIEW
            && sqlNode.getKind() != SqlKind.REFRESH_MATERIALIZED_VIEW
            && sqlNode.getKind() != SqlKind.DROP_MATERIALIZED_VIEW) {
            //dml的单表跨库下推没有判断insert表,忽略
            return ExecutionPlan.DirectMode.MULTI_DB_TABLE_DIRECT;
        }

        return ExecutionPlan.DirectMode.NONE;
    }

    private ExecutionPlan constructExecutionPlan(RelDataType originalRowType,
                                                 RelNode unoptimizedNode,
                                                 RelNode optimizedNode,
                                                 SqlNode originNode,
                                                 SqlNode validatedNode,
                                                 SqlConverter converter,
                                                 ToDrdsRelVisitor toDrdsRelVisitor,
                                                 PlannerContext plannerContext,
                                                 ExecutionPlan.DirectMode directPlanMode) {
        final List<String> tableNames = MetaUtils.buildTableNamesForNode(validatedNode);
        CursorMeta cursorMeta = null;

        ExecutionPlan result = null;
        RelNode finalPlan = null;
        boolean direct = false;
        switch (directPlanMode) {
        case TABLE_DIRECT:
            direct = true;
            if (validatedNode.isA(DML)) {
                cursorMeta = CalciteUtils.buildDmlCursorMeta();
            }

            // 如果都是单库单表,直接下推,构建逻辑计划
            finalPlan = buildDirectPlan(toDrdsRelVisitor.getBaseLogicalView(),
                unoptimizedNode,
                validatedNode,
                toDrdsRelVisitor,
                plannerContext,
                converter.getValidator());
            if ((validatedNode instanceof SqlSelect && ((SqlSelect) validatedNode).getOutFileParams() != null)) {
                optimizedNode = new LogicalOutFile(finalPlan.getCluster(), finalPlan.getTraitSet(), finalPlan,
                    ((SqlSelect) validatedNode).getOutFileParams());
                cursorMeta = CalciteUtils.buildDmlCursorMeta();
            } else {
                optimizedNode = finalPlan;
            }
            break;
        case MULTI_DB_TABLE_DIRECT:
            direct = true;
            if (validatedNode.isA(DML)) {
                cursorMeta = CalciteUtils.buildDmlCursorMeta();
            }

            // 如果都是单库单表,直接下推,构建逻辑计划
            finalPlan = buildMultiDirectPlan(
                unoptimizedNode,
                validatedNode,
                toDrdsRelVisitor,
                plannerContext,
                converter.getValidator());
            if ((validatedNode instanceof SqlSelect && ((SqlSelect) validatedNode).getOutFileParams() != null)) {
                optimizedNode = new LogicalOutFile(finalPlan.getCluster(), finalPlan.getTraitSet(), finalPlan,
                    ((SqlSelect) validatedNode).getOutFileParams());
                cursorMeta = CalciteUtils.buildDmlCursorMeta();
            } else {
                optimizedNode = finalPlan;
            }
            break;
        case SHARDING_KEY_DIRECT:
            direct = true;
            optimizedNode = buildDirectShardingKeyPlan((LogicalView) optimizedNode,
                unoptimizedNode,
                validatedNode,
                toDrdsRelVisitor.isShouldRemoveSchemaName(),
                plannerContext,
                converter.getValidator());

            break;
        case NONE:
            if (validatedNode.isA(DML) || optimizedNode instanceof LogicalOutFile) {
                cursorMeta = CalciteUtils.buildDmlCursorMeta();
            }
            break;
        default:
            throw new UnsupportedOperationException("Unknown directPlanMode: " + directPlanMode);
        }
        List<TableMeta> tableMetas = null;
        if (tableNames != null && tableNames.size() > 0) {
            SchemaManager schemaManager =
                OptimizerContext.getContext(plannerContext.getSchemaName()).getLatestSchemaManager();
            try {
                tableMetas = tableNames.stream()
                    .map(tableName -> StringUtils.isEmpty(tableName) ? null : schemaManager.getTable(tableName))
                    .collect(Collectors.toList());
            } catch (TableNotFoundException e) {
                // ignore
            } catch (Throwable t) {
                if (!InformationSchema.NAME.equalsIgnoreCase(plannerContext.getSchemaName())) {
                    logger.error(t.getMessage());
                }
            }
        }

        if (cursorMeta == null) {
            cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(originalRowType, tableNames, tableMetas));
        }

        // Copy user hint from AST to all logical view.
        if (validatedNode instanceof SqlSelect && !((SqlSelect) validatedNode).getOptimizerHint().getHints()
            .isEmpty()) {
            final UserHintPassThroughVisitor userHintPassthroughVisitor =
                new UserHintPassThroughVisitor(((SqlSelect) validatedNode).getOptimizerHint());
            optimizedNode = optimizedNode.accept(userHintPassthroughVisitor);
        }

        if (!direct) {
            optimizedNode = optimizePlanForTso(optimizedNode, plannerContext);
        }

        result = new ExecutionPlan(validatedNode, optimizedNode, cursorMeta);
        if (Boolean
            .parseBoolean(plannerContext.getParamManager().getProps().get(ConnectionProperties.PREPARE_OPTIMIZE))) {
            // only need cursorMeta in prepare mode
            return result;
        }

        result.setDirectShardingKey(directPlanMode == ExecutionPlan.DirectMode.SHARDING_KEY_DIRECT);
        if (direct) {
            // disable tp slow check in direct mode
            result.disableCheckTpSlow();
        }
        ExplainResult explainResult = plannerContext.getExecutionContext().getExplain();
        if (isExplainAdvisor(explainResult) || isExplainStatistics(explainResult)) {
            plannerContext.getExecutionContext().setUnOptimizedPlan(unoptimizedNode);
        }

        if (unoptimizedNode instanceof LogicalOutFile) {
            plannerContext.getExecutionContext().setUnOptimizedPlan(unoptimizedNode);
        }

        updatePlanProperties(result, toDrdsRelVisitor, validatedNode);
        initPlanShardInfo(result, plannerContext.getExecutionContext());

        // If this plan can be optimized but not yet optimized, set this flag to true.
        result.setCanOptByForcePrimary(plannerContext.isCanOptByForcePrimary() && !plannerContext.isAddForcePrimary());
        plannerContext.getExecutionContext().setUseColumnar(plannerContext.isUseColumnar());

        plannerContext.setExecutionContext(plannerContext.getExecutionContext().copy());
        return result;
    }

    private RelNode optimizePlanForTso(RelNode optimizedNode, PlannerContext plannerContext) {
        /* Here, we use global config value to judge whether we **try** to optimize this plan.
           But whether we really optimize this plan depends on local config value, i.e., pc.isAddForcePrimary().
         */
        if (InstConfUtil.getBool(ConnectionParams.ENABLE_FORCE_PRIMARY_FOR_TSO)) {
            final ForceIndexVisitor forceIndexVisitor =
                new ForceIndexVisitor(plannerContext.isAddForcePrimary(), plannerContext.getExecutionContext());
            optimizedNode = optimizedNode.accept(forceIndexVisitor);
            if (forceIndexVisitor.isCanOptByForcePrimary()) {
                plannerContext.setCanOptByForcePrimary(true);
            }
        }
        return optimizedNode;
    }

    private SqlNode optimizeAstForTso(SqlNode sqlNode, PlannerContext pc, SqlValidatorImpl validator) {
        /* Here, we use global config value to judge whether we **try** to optimize this plan.
           But whether we really optimize this plan depends on local config value, i.e., pc.isAddForcePrimary().
         */
        if (InstConfUtil.getBool(ConnectionParams.ENABLE_FORCE_PRIMARY_FOR_TSO)) {
            final ForceIndexSingleVisitor forceIndexSingleVisitor = new ForceIndexSingleVisitor(
                validator, pc.isAddForcePrimary(), pc.getExecutionContext());
            sqlNode = sqlNode.accept(forceIndexSingleVisitor);
            if (forceIndexSingleVisitor.isCanOptByForcePrimary()) {
                pc.setCanOptByForcePrimary(true);
            }
        }
        return sqlNode;
    }

    private boolean needSkipInitPlanShardInfo(RelNode input) {
        if (input instanceof SingleRel) {
            if (((SingleRel) input).getInput() instanceof VirtualView) {
                return true;
            } else {
                return needSkipInitPlanShardInfo(((SingleRel) input).getInput());
            }
        } else if (input == null) {
            return false;
        } else {
            for (RelNode relNode : input.getInputs()) {
                if (relNode instanceof VirtualView) {
                    return true;
                } else {
                    boolean skip = needSkipInitPlanShardInfo(relNode);
                    if (skip) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void initPlanShardInfo(ExecutionPlan executionPlan, ExecutionContext ec) {
        RelNode plan = executionPlan.getPlan();
        SqlNode ast = executionPlan.getAst();
        SqlKind kind = ast.getKind();

        PlanShardInfo planShardInfo;
        boolean needInitPlanShard = !(plan instanceof DirectTableOperation) && !(plan instanceof VirtualView) &&
            !(plan instanceof DirectShardingKeyTableOperation) && !((plan instanceof SingleRel) && (((SingleRel) plan)
            .getInput()) instanceof VirtualView);
        needInitPlanShard = needInitPlanShard && !needSkipInitPlanShardInfo(plan);

        if (needInitPlanShard) {
            switch (kind) {
            case SELECT:
            case UNION:
            case INSERT:
            case REPLACE:
            case UPDATE:
            case DELETE: {
                ExtractionResult er = ConditionExtractor.predicateFrom(plan).extract();
                Set<String> schemaNames = er.getSchemaNameSet();
                planShardInfo = er.allShardInfo(ec);
                planShardInfo.setEr(er);
                executionPlan.setSchemaNames(schemaNames);
                String key = OptimizerUtils.buildInExprKey(ec);
                executionPlan.setPlanShardInfo(key, planShardInfo);
            }
            break;
            default:
                break;
            }
        }
    }

    /**
     * set logical plan properties
     */
    private ExecutionPlan updatePlanProperties(ExecutionPlan plan, ToDrdsRelVisitor visitor, SqlNode sqlNode) {
        if (visitor.isModifyBroadcastTable()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_BROADCAST_TABLE);
        }

        if (visitor.isOnlyBroadcastTable()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.ONLY_BROADCAST_TABLE);
        }

        if (visitor.isModifyGsiTable()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_GSI_TABLE);
        }

        if (visitor.isContainReplicateWriableTable()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_GSI_TABLE);
        }
        if (GeneralUtil.isNotEmpty(visitor.getModifiedTables())) {
            plan.setModifiedTables(visitor.getModifiedTables());
            plan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_TABLE);
        }
        if (visitor.getLockMode() != SqlSelect.LockMode.UNDEF) {
            plan.getPlanProperties().set(ExecutionPlanProperties.SELECT_WITH_LOCK);
        }
        if (visitor.isModifyShardingColumn()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_SHARDING_COLUMN);
        }
        if (visitor.isWithIndexHint()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.WITH_INDEX_HINT);
        }
        if (!sqlNode.isA(SqlKind.DML)) {
            plan.setOriginTableNames(visitor.getTableNames());
        }

        if (sqlNode.isA(SqlKind.DML)) {
            plan.getPlanProperties().set(ExecutionPlanProperties.DML);
        }
        if (sqlNode.isA(QUERY)) {
            plan.getPlanProperties().set(ExecutionPlanProperties.QUERY);
        }

        if (sqlNode.isA(DDL)) {
            plan.getPlanProperties().set(ExecutionPlanProperties.DDL);
        }
        if (visitor.isContainScaleOutWritableTable()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.SCALE_OUT_WRITABLE_TABLE);
        }
        if (visitor.isContainScaleOutWritableTable()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_ONLINE_COLUMN_TABLE);
        }
        if (visitor.isModifyForeignKey()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_FOREIGN_KEY);
        }

        return plan;
    }

    /**
     * 查询的所有表都是位于一个库的单表,直接填充一个 LogicalView 返回即可
     */
    private RelNode buildDirectPlan(LogicalView logicalView, RelNode relNode, SqlNode sqlNode,
                                    ToDrdsRelVisitor toDrdsRelVisitor, PlannerContext pc, SqlValidatorImpl validator) {
        boolean shouldRemoveSchema = toDrdsRelVisitor.isShouldRemoveSchemaName();
        if (sqlNode instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) sqlNode;
            RelOptTable targetTable = relNode.getTable();
            final org.apache.calcite.util.Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
            final TableMeta tableMeta = pc.getExecutionContext().getSchemaManager(qn.left).getTable(qn.right);
            SqlNodeList columnList = insert.getTargetColumnList();
            // Expand insert target column list because logical column order may be different from physical column order
            if (tableMeta.requireLogicalColumnOrder() && (columnList == null || GeneralUtil.isEmpty(
                columnList.getList()))) {
                RelDataType baseRowType = targetTable.getRowType();
                List<SqlNode> nodes = new ArrayList<>();
                for (String fieldName : baseRowType.getFieldNames()) {
                    nodes.add(new SqlIdentifier(fieldName, SqlParserPos.ZERO));
                }
                SqlNodeList sqlNodeList = new SqlNodeList(nodes, SqlParserPos.ZERO);
                insert.setOperand(3, sqlNodeList);
            }
        }

        sqlNode = optimizeAstForTso(sqlNode, pc, validator);

        // Remove SchemaName
        if (shouldRemoveSchema) {
            RemoveSchemaNameVisitor visitor = new RemoveSchemaNameVisitor(logicalView.getSchemaName());
            sqlNode = sqlNode.accept(visitor);
        }
        CollectorTableVisitor collectorTableVisitor =
            new CollectorTableVisitor(logicalView.getSchemaName(), pc.getExecutionContext());
        List<String> tableName = collectorTableVisitor.getTableNames();

        if (sqlNode.getKind() == SqlKind.DELETE) {
            /**
             * non-exist force index delete should not be pushed down
             */
            String tableNameForDelete = logicalView.getLogicalTableName();
            String schemaName = logicalView.getSchemaName();
            // check force index exist or not
            final TableMeta tMeta = pc.getExecutionContext().getSchemaManager(schemaName).getTable(tableNameForDelete);
            RemoveIndexNodeVisitor removeIndexNodeVisitor = new RemoveIndexNodeVisitor(tMeta);
            sqlNode = sqlNode.accept(removeIndexNodeVisitor);
        }

        sqlNode = sqlNode.accept(collectorTableVisitor);
        List<String> logTableNames = collectorTableVisitor.getTableNames();
        sqlNode = sqlNode.accept(new ReplaceSingleTblOrBroadcastTblWithPhyTblVisitor(logicalView.getSchemaName(),
            pc.getExecutionContext()));
        BytesSql sqlTemplate = RelUtils.toNativeBytesSql(sqlNode);

        String t;
        if (sqlNode.isA(DML)) {
            t = Util.last(relNode.getTable().getQualifiedName());
        } else {
            t = logTableNames.get(0);
        }
        final List<String> physicalTableNames;
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalView.getSchemaName());

        if (isNewPartDb) {
            physicalTableNames = logTableNames.stream()
                .map(e -> {
                    PartitionInfo partitionInfo = OptimizerContext.getContext(logicalView.getSchemaName())
                        .getPartitionInfoManager().getPartitionInfo(e);
                    if (partitionInfo != null) {
                        return partitionInfo.getPrefixTableName();
                    }
                    return e;
                }).collect(Collectors.toList());
        } else {
            physicalTableNames = logTableNames.stream()
                .map(e -> {
                    TableRule tableRule = OptimizerContext.getContext(logicalView.getSchemaName())
                        .getRuleManager()
                        .getTableRule(e);
                    if (tableRule != null) {
                        return tableRule.getTbNamePattern();
                    }
                    return e;
                })
                .collect(Collectors.toList());
        }
        String dbIndex;
        TddlRuleManager or = OptimizerContext.getContext(logicalView.getSchemaName()).getRuleManager();
        TableRule tr = or.getTableRule(t);
        if (tr != null) {
            dbIndex = tr.getDbNamePattern();
        } else {
            dbIndex = or.getDefaultDbIndex(t);
        }
        if (isNewPartDb) {
            /**
             * When the join of bro_tbl & sig_tbl is pushed, should use the phy db of single tbl
             */
            dbIndex = fetchGroupKeyFromSingleTblLoication(toDrdsRelVisitor, dbIndex, or);
        }

        List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(sqlNode);

        DirectTableOperation directTableScan = new DirectTableOperation(logicalView,
            relNode.getRowType(),
            logTableNames,
            physicalTableNames,
            dbIndex,
            sqlTemplate,
            paramIndex);
        directTableScan.setNativeSqlNode(sqlNode);
        directTableScan.setSchemaName(logicalView.getSchemaName());
        if (sqlNode.getKind() == SqlKind.SELECT) {
            directTableScan.setLockMode(((SqlSelect) sqlNode).getLockMode());
        }
        if (relNode instanceof TableModify) {
            SqlKind kind = ((TableModify) relNode).getOperation().toSqlKind();
            directTableScan.setKind(kind);
            directTableScan.setHintContext(((TableModify) relNode).getHintContext());

            if (or.isBroadCast(t)) {
                return new BroadcastTableModify(directTableScan);
            }
        }
        if (RelXPlanOptimizer.canUseXPlan(sqlNode)) {
            RelXPlanOptimizer.setXTemplate(directTableScan, relNode, pc.getExecutionContext());
        }
        // Init sql digest.
        try {
            directTableScan.setSqlDigest(directTableScan.getBytesSql().digest());
        } catch (Exception ignore) {
        }
        // Check and set galaxy prepare context.
        if (or.isBroadCast(t)) {
            // Note: single or broadcast may set table names in sql, and this may cause misuse of table across databases
            directTableScan.setSupportGalaxyPrepare(false);
        } else {
            setGalaxyPrepareDigest(directTableScan, tableName, pc.getExecutionContext(), relNode);
        }

        return directTableScan;
    }

    /**
     * 查询的所有表都是位于不同库的单表
     */
    private RelNode buildMultiDirectPlan(RelNode relNode, SqlNode sqlNode,
                                         ToDrdsRelVisitor toDrdsRelVisitor, PlannerContext pc,
                                         SqlValidatorImpl validator) {
        if (sqlNode instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) sqlNode;
            RelOptTable targetTable = relNode.getTable();
            final org.apache.calcite.util.Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
            final TableMeta tableMeta = pc.getExecutionContext().getSchemaManager(qn.left).getTable(qn.right);
            SqlNodeList columnList = insert.getTargetColumnList();
            // Expand insert target column list because logical column order may be different from physical column order
            if (tableMeta.requireLogicalColumnOrder() && (columnList == null || GeneralUtil.isEmpty(
                columnList.getList()))) {
                RelDataType baseRowType = targetTable.getRowType();
                List<SqlNode> nodes = new ArrayList<>();
                for (String fieldName : baseRowType.getFieldNames()) {
                    nodes.add(new SqlIdentifier(fieldName, SqlParserPos.ZERO));
                }
                SqlNodeList sqlNodeList = new SqlNodeList(nodes, SqlParserPos.ZERO);
                insert.setOperand(3, sqlNodeList);
            }
        }

        sqlNode = optimizeAstForTso(sqlNode, pc, validator);

        ReplaceDifferentDBSingleTblWithPhyTblVisitor visitor = new ReplaceDifferentDBSingleTblWithPhyTblVisitor(
            pc.getSchemaName(),
            pc.getExecutionContext());
        sqlNode = sqlNode.accept(visitor);
        BytesSql sqlTemplate = RelUtils.toNativeBytesSql(sqlNode);

        List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(sqlNode);

        DirectMultiDBTableOperation directTableScan = new DirectMultiDBTableOperation(
            toDrdsRelVisitor.getBaseLogicalView(),
            relNode.getRowType(),
            visitor.getLogicalTables(),
            visitor.getPhysicalTables(),
            visitor.getSchemaPhysicalMapping(),
            sqlTemplate,
            paramIndex,
            pc.getSchemaName(),
            pc.getParamManager().getBoolean(ConnectionParams.SIMPLIFY_MULTI_DB_SINGLE_TB_PLAN));
        directTableScan.setNativeSqlNode(sqlNode);
        if (sqlNode.getKind() == SqlKind.SELECT) {
            directTableScan.setLockMode(((SqlSelect) sqlNode).getLockMode());
        }
        if (relNode instanceof TableModify) {
            SqlKind kind = ((TableModify) relNode).getOperation().toSqlKind();
            directTableScan.setKind(kind);
            directTableScan.setHintContext(((TableModify) relNode).getHintContext());
        }
        if (RelXPlanOptimizer.canUseXPlan(sqlNode)) {
            RelXPlanOptimizer.setXTemplate(directTableScan, relNode, pc.getExecutionContext());
        }
        // Init sql digest.
        try {
            directTableScan.setSqlDigest(directTableScan.getBytesSql().digest());
        } catch (Exception ignore) {
        }

        directTableScan.setSupportGalaxyPrepare(false);
        return directTableScan;
    }

    private String fetchGroupKeyFromSingleTblLoication(ToDrdsRelVisitor toDrdsRelVisitor, String dbIndex,
                                                       TddlRuleManager or) {
        // Find single tbl name
        List<String> tblNames = toDrdsRelVisitor.getTableNames();
        PartitionInfo partInfoOfSinTbl = null;
        boolean onlyContainSingleTblOrBroadCastTbl = true;
        PartitionInfoManager partInfoMgr = or.getPartitionInfoManager();
        for (int i = 0; i < tblNames.size(); i++) {
            String tblName = tblNames.get(i);
            PartitionInfo partInfo = partInfoMgr.getPartitionInfo(tblName);
            if (partInfo != null) {
                if (partInfo.isGsiOrPartitionedTable()) {
                    onlyContainSingleTblOrBroadCastTbl = false;
                    break;
                }
                if (partInfo.isSingleTable()) {
                    partInfoOfSinTbl = partInfo;
                    break;
                }
            }
        }
        if (partInfoOfSinTbl != null && onlyContainSingleTblOrBroadCastTbl) {
            dbIndex = partInfoOfSinTbl.getPartitionBy().getPhysicalPartitions().get(0).getLocation().getGroupKey();
        }
        return dbIndex;
    }

    /**
     * 分片键点查,直接填充 LogicalView
     */
    private RelNode buildDirectShardingKeyPlan(LogicalView logicalView, RelNode relNode, SqlNode sqlNode,
                                               boolean shouldRemoveSchema, PlannerContext pc,
                                               SqlValidatorImpl validator) {
        sqlNode = optimizeAstForTso(sqlNode, pc, validator);
        // Remove SchemaName
        String schemaName = logicalView.getSchemaName();
        if (shouldRemoveSchema) {
            RemoveSchemaNameVisitor visitor = new RemoveSchemaNameVisitor(schemaName);
            sqlNode = sqlNode.accept(visitor);
        }
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, true, pc.getExecutionContext());
        sqlNode = sqlNode.accept(visitor);
        BytesSql sqlTemplate = RelUtils.toNativeBytesSql(sqlNode, DbType.MYSQL);

        String tableName = logicalView.getLogicalTableName();
        List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(sqlNode);

        DirectShardingKeyTableOperation directTableScan = new DirectShardingKeyTableOperation(logicalView,
            relNode.getRowType(),
            tableName,
            sqlTemplate,
            paramIndex, pc.getExecutionContext());
        directTableScan.setNativeSqlNode(sqlNode);

        if (sqlNode.getKind() == SqlKind.SELECT) {
            directTableScan.setLockMode(((SqlSelect) sqlNode).getLockMode());
        }

        if (RelXPlanOptimizer.canUseXPlan(sqlNode)) {
            RelXPlanOptimizer.setXTemplate(directTableScan, relNode, pc.getExecutionContext());
        }
        // Init sql digest.
        try {
            directTableScan.setSqlDigest(directTableScan.getBytesSql().digest());
        } catch (Exception ignore) {
        }
        // Check and set galaxy prepare context.
        setGalaxyPrepareDigest(directTableScan, ImmutableList.of(tableName), pc.getExecutionContext(), relNode);

        return directTableScan;
    }

    /**
     * Generate XPlan via raw relnode.
     * TODO: lock not supported now.
     * Always generate the XPlan in case of switching connection pool.
     */
    private boolean canUseXPlan(SqlNode sqlNode) {
        return sqlNode.getKind() == SqlKind.SELECT
            && ((SqlSelect) sqlNode).getLockMode() == SqlSelect.LockMode.UNDEF;
    }

    private void setXTemplate(BaseQueryOperation operation, RelNode relNode, ExecutionContext ec) {
        final RelToXPlanConverter converter = new RelToXPlanConverter();
        try {
            RelNode node = RelXPlanOptimizer.optimize(relNode);
            operation.setXTemplate(converter.convert(node));
            if (ExplainResult.isExplainExecute(ec.getExplain())) {
                ec.setUnOptimizedPlan(relNode);
            }
        } catch (Exception e) {
            Throwable throwable = e;
            while (throwable.getCause() != null && throwable.getCause() instanceof InvocationTargetException) {
                throwable = ((InvocationTargetException) throwable.getCause()).getTargetException();
            }
            logger.info("XPlan converter: " + throwable.getMessage());
        }
    }

    static public byte[] getGalaxyPrepareDigestPrefix(String schemaName, List<String> logicalTableNames,
                                                      ExecutionContext ec) {
        final StringBuilder prefixBuilder = new StringBuilder();
        prefixBuilder.append(schemaName);
        prefixBuilder.append('#');
        for (String table_name : logicalTableNames) {
            prefixBuilder.append(table_name);
            prefixBuilder.append('#');
            final TableMeta table = ec.getSchemaManager(schemaName).getTable(table_name);
            if (null == table) {
                throw new TddlRuntimeException(ErrorCode.ERR_CANNOT_FETCH_TABLE_META, table_name,
                    "unknown reason");
            }
            prefixBuilder.append(table.getVersion());
            prefixBuilder.append('#');
        }
        return prefixBuilder.toString().getBytes();
    }

    static public com.google.protobuf.ByteString calcGalaxyPrepareDigest(
        String schemaName, BytesSql sqlTemplate, List<String> logicalTableNames, ExecutionContext ec) {
        return calcGalaxyPrepareDigest(getGalaxyPrepareDigestPrefix(schemaName, logicalTableNames, ec), sqlTemplate);
    }

    static public com.google.protobuf.ByteString calcGalaxyPrepareDigest(byte[] prefix, BytesSql sqlTemplate) {
        try {
            final MessageDigest md5 = MessageDigest.getInstance("md5");
            md5.update(prefix);
            md5.update(sqlTemplate.getBytes()); // use default UTF-8 encoding
            return com.google.protobuf.ByteString.copyFrom(md5.digest());
        } catch (Exception e) {
            logger.error(e);
        }
        return null;
    }

    static public void setGalaxyPrepareDigest(BaseQueryOperation operation, List<String> logicalTableNames,
                                              ExecutionContext ec, RelNode relNode) {
        operation.setGalaxyPrepareDigest(
            calcGalaxyPrepareDigest(operation.getSchemaName(), operation.getBytesSql(), logicalTableNames, ec));
        if (operation.getGalaxyPrepareDigest() != null) {
            final SpecialFunctionRelFinder finder = new SpecialFunctionRelFinder();
            finder.go(relNode);
            operation.setSupportGalaxyPrepare(finder.supportGalaxyPrepare());
        } else {
            operation.setSupportGalaxyPrepare(false);
        }
    }

    static public void setGalaxyPrepareDigest(
        PhyTableOpBuildParams params, String schemaName, BytesSql sql, List<String> logicalTableNames,
        ExecutionContext ec, RelNode relNode) {
        final com.google.protobuf.ByteString digest = calcGalaxyPrepareDigest(schemaName, sql, logicalTableNames, ec);
        params.setGalaxyPrepareDigest(digest);
        if (digest != null) {
            final SpecialFunctionRelFinder finder = new SpecialFunctionRelFinder();
            finder.go(relNode);
            params.setSupportGalaxyPrepare(finder.supportGalaxyPrepare());
        } else {
            params.setSupportGalaxyPrepare(false);
        }
    }

    static public void setGalaxyPrepareDigest(PhyTableOpBuildParams param, BytesSql sql, byte[] prefix,
                                              RelNode relNode) {
        final com.google.protobuf.ByteString digest = calcGalaxyPrepareDigest(prefix, sql);
        param.setGalaxyPrepareDigest(digest);
        if (digest != null) {
            final SpecialFunctionRelFinder finder = new SpecialFunctionRelFinder();
            finder.go(relNode);
            param.setSupportGalaxyPrepare(finder.supportGalaxyPrepare());
        } else {
            param.setSupportGalaxyPrepare(false);
        }
    }

    /**
     * a parameterized sql (must be a single-statement) to ExecutionPlan
     * <p>
     * two way to get plan:
     * 1. get from plan cache
     * 2. build plan
     *
     * @return ExecutionPlan
     */
    private ExecutionPlan doPlan(SqlType sqlType, SqlParameterized sqlParameterized,
                                 ExecutionContext executionContext,
                                 SqlNodeList sqlNodeList,
                                 boolean forPrepare) {
        if (!forPrepare) {
            processParameters(sqlParameterized.getParameters(), executionContext);
            processMsah(sqlParameterized, executionContext);
        }
        executionContext.setSqlType(sqlType);
        processCpuProfileForSqlType(executionContext);
        long startOptimizePlan = isStatCpuForBuildPlan(executionContext) ? ThreadCpuStatUtil.getThreadCpuTimeNano() : 0;

        ExecutionPlan executionPlan;
        if (PlanManagerUtil.useSpm(sqlParameterized, executionContext)) {
            if (executionContext.isEnableFeedBackWorkload()) {
                // plan management with feedback
                executionPlan = PlaceHolderExecutionPlan.INSTANCE;
            } else {
                String schema = executionContext.getSchemaName();
                // plan management
                if (forPrepare) {
                    executionPlan =
                        PlanManager.getInstance().choosePlanForPrepare(sqlParameterized, sqlNodeList, executionContext);
                } else {
                    executionPlan = PlanManager.getInstance().choosePlan(sqlParameterized, executionContext);
                }
            }

            if (executionPlan == PlaceHolderExecutionPlan.INSTANCE) {
                // using Hint or feedback workload
                executionPlan = doBuildPlan(sqlParameterized, executionContext);
            }
        } else {
            executionPlan = Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
        }

        // stat cpu for planner
        statCpuForBuildPlan(executionContext, startOptimizePlan);

        if (!forPrepare
            && executionPlan.isDirectShardingKey()
            && executionPlan.getPlan() instanceof DirectShardingKeyTableOperation) {
            // 点查计划在plan阶段保存sharding信息
            Pair<String, String> dbIndexAndTableName =
                ((DirectShardingKeyTableOperation) executionPlan.getPlan()).getDbIndexAndTableName(executionContext);
            executionPlan.setDbIndexAndTableName(dbIndexAndTableName);
        } else {
            // post planner
            if (executionPlan.isUsePostPlanner()) {
                ExecutionPlan executionPlanForPostPlanner = executionPlan.copy(executionPlan.getPlan());
                executionPlan = PostPlanner.getInstance().optimize(executionPlanForPostPlanner, executionContext);
            }
        }

        PlannerContext plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());
        executionContext.setUseColumnar(plannerContext.isUseColumnar());
        executionContext.setColumnarMaxShard(plannerContext.getColumnarMaxShardCnt());

        plannerContext.setExecutionContext(executionContext);
        executionContext.setFinalPlan(executionPlan);
        return executionPlan;
    }

    protected void processCpuProfileForSqlType(ExecutionContext executionContext) {

        SqlType sqlType = executionContext.getSqlType();
        if (!SqlTypeUtils.isDmlAndDqlSqlType(sqlType)) {
            // Optimize the performance for GSI
            executionContext.setOnlyUseTmpTblPool(true);
            executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.DEFAULT.metricLevel);
        } else {
            if (!SqlTypeUtils.isSelectSqlType(sqlType)) {
                executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.SQL.metricLevel);
            }
        }

        RuntimeStat runtimeStat = executionContext.getRuntimeStatistics();
        if (runtimeStat != null) {
            runtimeStat.setRunningWithCpuProfile(MetricLevel
                .isSQLMetricEnabled(executionContext.getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL)));
        }
    }

    private void statCpuForBuildPlan(ExecutionContext executionContext, long startOptimizePlan) {
        if (!isStatCpuForBuildPlan(executionContext)) {
            return;
        }
        executionContext.getRuntimeStatistics()
            .getCpuStat()
            .addCpuStatItem(CpuStatAttribute.CpuStatAttr.OPTIMIZE_PLAN,
                ThreadCpuStatUtil.getThreadCpuTimeNano() - startOptimizePlan);
    }

    private boolean isStatCpuForBuildPlan(ExecutionContext executionContext) {
        return MetricLevel.isSQLMetricEnabled(executionContext.getParamManager().getInt(
            ConnectionParams.MPP_METRIC_LEVEL)) && executionContext.getRuntimeStatistics() != null;
    }
}

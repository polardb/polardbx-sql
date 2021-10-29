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
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.model.hint.ExtraCmdRouteCondition;
import com.alibaba.polardbx.common.model.hint.RouteCondition;
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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbVariableConfigManager;
import com.alibaba.polardbx.gms.locality.PrimaryZoneInfo;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime.Now;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelToXPlanConverter;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelXPlanOptimizer;
import com.alibaba.polardbx.optimizer.core.planner.rule.CBOLogicalSemiJoinLogicalJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.CBOPushJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.CBOPushSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsAggregateJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsCorrelateConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsFilterConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsLogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsOutFileConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsProjectConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsSortConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsSortJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.DrdsSortProjectTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalAggToHashAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalAggToSortAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalJoinToBKAJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalJoinToHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalJoinToNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalJoinToSortMergeJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalSemiJoinToMaterializedSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalSemiJoinToSemiHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalSemiJoinToSemiNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalSemiJoinToSemiSortMergeJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.OptimizeLogicalViewRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.OuterJoinAssocRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.OuterJoinLAsscomRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.ProjectSortTransitiveRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushFilterRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushProjectRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushSortRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.RuleToUse;
import com.alibaba.polardbx.optimizer.core.planner.rule.SQL_REWRITE_RULE_PHASE;
import com.alibaba.polardbx.optimizer.core.planner.rule.SemiJoinSemiJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.PushBloomFilterRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CheapestFractionalPlanReplacer;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.SubQueryPlanEnumerator;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.CollectorTableVisitor;
import com.alibaba.polardbx.optimizer.core.rel.CountVisitor;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.RemoveSchemaNameVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceLogicalTableNameWithPhysicalTableNameVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ToDrdsRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.UserHintPassThroughVisitor;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.msha.TddlMshaProcessor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.HintParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.SqlTypeParser;
import com.alibaba.polardbx.optimizer.parse.SqlTypeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.hint.SimpleHintParser;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsParameterizeSqlVisitor;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.planmanager.parametric.MyParametricQueryAdvisor;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.result.PlanShardInfo;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.MetaUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers;
import com.alibaba.polardbx.optimizer.variable.VariableManager;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
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
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
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
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.getRexNodeTableMap;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainAdvisor;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOptimizer;
import static com.alibaba.polardbx.optimizer.utils.RelUtils.disableMpp;
import static com.alibaba.polardbx.optimizer.workload.WorkloadUtil.determineWorkloadType;
import static org.apache.calcite.sql.SqlKind.DML;
import static org.apache.calcite.sql.SqlKind.QUERY;

/**
 * @author lingce.ldm 2017-07-07 14:44
 */
public class Planner {

    private static final Planner INSTANCE = new Planner();
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public Planner() {
    }

    public static Planner getInstance() {
        return INSTANCE;
    }

    public ExecutionPlan plan(String sql, ExecutionContext executionContext) {
        return plan(ByteString.from(sql), executionContext);
    }

    public ExecutionPlan planForPrepare(ByteString sql,
                                        SqlNodeList sqlNodeList,
                                        PreparedStmtCache preparedStmtCache,
                                        ExecutionContext executionContext) {
        ByteString afterProcessSql = removeSpecialHint(sql, executionContext);
        SqlParameterized parameterized = parameterize(afterProcessSql, executionContext, true);
        SqlType sqlType = SqlTypeParser.typeOf(parameterized.getSql());

        preparedStmtCache.setSqlParameterized(parameterized);
        preparedStmtCache.setSqlType(sqlType);
        return plan(sql, sqlType, parameterized, executionContext, sqlNodeList, true);
    }

    public ExecutionPlan plan(ByteString sql, ExecutionContext executionContext) {
        if (executionContext.isExecutingPreparedStmt()) {
            PreparedStmtCache preparedStmtCache = executionContext.getPreparedStmtCache();
            assert preparedStmtCache != null;

            ByteString afterProcessSql = removeSpecialHint(sql, executionContext);
            SqlParameterized parameterized = parameterize(afterProcessSql, executionContext);
            return plan(sql, preparedStmtCache.getSqlType(), parameterized, executionContext);
        } else {
            ByteString afterProcessSql = removeSpecialHint(sql, executionContext);
            SqlParameterized parameterized = parameterize(afterProcessSql, executionContext);
            SqlType sqlType = SqlTypeParser.typeOf(parameterized.getSql());

            return plan(sql, sqlType, parameterized, executionContext);
        }
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
        // metaquery init
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(DrdsRelMetadataProvider.INSTANCE));

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
            parameterized = handleExplain(sql, (MySqlExplainStatement) statement, executionContext);
        }

        return doPlan(sqlType, parameterized, executionContext, sqlNodeList, forPrepare);
    }

    private SqlParameterized parameterize(ByteString afterProcessSql,
                                          ExecutionContext executionContext) {
        return parameterize(afterProcessSql, executionContext, false);
    }

    private SqlParameterized parameterize(ByteString afterProcessSql,
                                          ExecutionContext executionContext,
                                          boolean forPrepare) {
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
            SqlParameterizeUtils.parameterize(afterProcessSql, currentParameter, executionContext, forPrepare);

        if (enableSqlCpu) {
            executionContext.getRuntimeStatistics()
                .getCpuStat()
                .addCpuStatItem(CpuStatAttribute.CpuStatAttr.PARAMETERIZE_SQL,
                    ThreadCpuStatUtil.getThreadCpuTimeNano() - startParameterize);
        }
        return result;
    }

    private SqlParameterized handleExplain(ByteString sql, MySqlExplainStatement sqlExplain,
                                           ExecutionContext executionContext) {
        // process explain
        ExplainResult result = new ExplainResult();
        result.explainMode = ExplainResult.ExplainMode.DETAIL;
        for (ExplainResult.ExplainMode mode : ExplainResult.ExplainMode.values()) {
            if (mode.name().equalsIgnoreCase(sqlExplain.getType())) {
                result.explainMode = mode;
                break;
            }
        }

        if (isExplainOptimizer(result)) {
            CalcitePlanOptimizerTrace.setOpen(true);
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
            .parameterize(explainedQuery, sqlExplain.getStatement(), parameters, executionContext, false);
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
                                     ExecutionContext executionContext) {
        SqlNode ast = astList.get(0);
        Set<Pair<String, String>> tableSet = PlanManagerUtil.getTableSetFromAst(ast);
        // process special query
        SqlNode afterProcessAst = processSpecialQuery(ast, executionContext);

        if (ast.getKind().belongsTo(SqlKind.DML)) {
            tableSet = PlanManagerUtil.getTableSetFromAst(ast);
        }
        ExecutionPlan plan;
        if (executionContext.isUseHint()) {
            // handle plan hint first
            plan = buildPlanWithHint(afterProcessAst, executionContext);
        } else {
            PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
            plan = getPlan(afterProcessAst, plannerContext);
            plan.setExplain(executionContext.getExplain() != null);
            plan.setUsePostPlanner(
                PostPlanner.usePostPlanner(executionContext.getExplain(), executionContext.isUseHint()));
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
        return doBuildPlan(astList, executionContext);
    }

    public static void processParameter(List<Object> p, ExecutionContext executionContext) {

        // fill the userDefVariable & sysDefVariable
        if (p != null) {
            // record nlsString in parameters
            Map<Integer, NlsString> nlsStringMap = IntStream.range(0, p.size())
                .filter(i -> p.get(i) instanceof NlsString)
                .boxed()
                .collect(
                    Collectors.toMap(i -> i, i -> (NlsString) p.get(i))
                );
            executionContext.setParameterNlsStrings(nlsStringMap);

            VariableManager variableManager =
                OptimizerContext.getContext(executionContext.getSchemaName()).getVariableManager();
            for (int i = 0; i < p.size(); i++) {
                if (p.get(i) instanceof DrdsParameterizeSqlVisitor.UserDefVariable) {
                    DrdsParameterizeSqlVisitor.UserDefVariable userDefVariable =
                        (DrdsParameterizeSqlVisitor.UserDefVariable) p.get(i);
                    Map<String, Object> userDefVariables = executionContext.getUserDefVariables();
                    p.set(i, userDefVariables.get(userDefVariable.getName().toLowerCase()));
                } else if (p.get(i) instanceof DrdsParameterizeSqlVisitor.SysDefVariable) {
                    DrdsParameterizeSqlVisitor.SysDefVariable sysDefVariable =
                        (DrdsParameterizeSqlVisitor.SysDefVariable) p.get(i);
                    String name = StringUtils.strip(sysDefVariable.getName().toLowerCase(), "`");
                    if ("last_insert_id".equals(name)) {
                        p.set(i, executionContext.getConnection().getLastInsertId());
                    } else if ("tx_isolation".equals(name) || "transaction_isolation".equals(name)) {
                        // Note: It's hard to get global isolation level here...
                        IsolationLevel isolation = IsolationLevel.fromInt(executionContext.getTxIsolation());
                        p.set(i, isolation != null ? isolation.nameWithHyphen() : null);
                    } else if ("compute_node".equals(name)) {
                        p.set(i, TddlNode.getHost() + ":" + TddlNode.getPort());
                    } else if ("primary_zone".equals(name)) {
                        PrimaryZoneInfo primaryZoneInfo = LocalityManager.getInstance().getSystemPrimaryZone();
                        p.set(i, primaryZoneInfo.serialize());
                    } else if ("read_only".equals(name)) {
                        if (ConfigDataMode.isMasterMode()) {
                            p.set(i, 0);
                        } else if (ConfigDataMode.isSlaveMode()) {
                            p.set(i, 1);
                        }
                    } else if ("auto_increment_increment".equalsIgnoreCase(name) && !SequenceManagerProxy.getInstance()
                        .areAllSequencesSameType(executionContext.getSchemaName(), SequenceAttribute.Type.SIMPLE)) {
                        // Since the steps of Group and Time-based Sequence are fixed to 1,
                        // so we have to override auto_increment_increment set on RDS for
                        // correct behavior of generated keys, unless all sequence types
                        // are SIMPLE which allows custom step/increment.
                        p.set(i, 1);
                    } else {
                        if (!ServerVariables.contains(name) && !ServerVariables.isExtra(name)) {
                            throw new TddlNestableRuntimeException("Unknown system variable " + name);
                        }

                        Object v;
                        if (sysDefVariable.isGlobal()) {
                            v = variableManager.getGlobalVariable(name);
                            Properties cnProperties =
                                MetaDbInstConfigManager.getInstance().getCnVariableConfigMap();
                            Properties dnProperties =
                                MetaDbVariableConfigManager.getInstance().getDnVariableConfigMap();
                            if (cnProperties.containsKey(name)) {
                                v = cnProperties.getProperty(name);
                            } else if (dnProperties.containsKey(name)) {
                                v = dnProperties.getProperty(name);
                            }
                        } else {
                            if (executionContext.getExtraServerVariables().containsKey(name)) {
                                v = executionContext.getExtraServerVariables().get(name);
                            } else {
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

                        if (v != null && v instanceof String && TStringUtil.isParsableNumber((String) v)) {
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
                            if (((Boolean) v) == true) {
                                v = 1;
                            } else if (((Boolean) v) == false) {
                                v = 0;
                            }
                        }

                        if (v == null) {
                            v = "";
                        }
                        p.set(i, v);
                    }
                } else if (p.get(i) instanceof DrdsParameterizeSqlVisitor.ConstantVariable) {
                    DrdsParameterizeSqlVisitor.ConstantVariable constantVariable =
                        (DrdsParameterizeSqlVisitor.ConstantVariable) p.get(i);
                    String name = StringUtils.strip(constantVariable.getName().toLowerCase(), "`");
                    if ("now".equals(name)) {
                        Now now = null;
                        if (constantVariable.getArgs().length != 0) {
                            now = new Now(null, (new DateTimeType((Integer) constantVariable.getArgs()[0])));
                            p.set(i, new DateTimeType((Integer) constantVariable.getArgs()[0])
                                .convertFrom(executionContext.getConstantValue(name, now, new Object[] {6})));
                        } else {
                            now = new Now(null, new DateTimeType());
                            p.set(i, new DateTimeType()
                                .convertFrom(executionContext.getConstantValue(name, now, new Object[] {6})));
                        }
                    }
                } else if (p.get(i) instanceof NlsString) {
                    // reduce nlsString to Java String
                    NlsString nlsString = (NlsString) p.get(i);
                    if (CharsetName.of(nlsString.getCharsetName()) != CharsetName.BINARY) {
                        p.set(i, nlsString.getValue());
                    } else {
                        p.set(i, nlsString.getValue().getBytes(StandardCharsets.ISO_8859_1));
                    }
                }
            }
        }

        Parameters parameters = executionContext.getParams();
        parameters.setParams(OptimizerUtils.buildParam(p));
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

    private SqlNodeList buildDirectHint(String schemaName, ExecutionContext executionContext) {
        // Build direct hint with defaultDbIndex
        List<SqlNode> hints = new ArrayList<>();
        List<SqlNode> funcs = new ArrayList<>();
        List<SqlNode> group = new ArrayList<>();
        SqlNode[] args = new SqlNode[1];
        String defaultDbIndex = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
        args[0] = SqlLiteral.createCharString(defaultDbIndex, SqlParserPos.ZERO);
        SqlNode func = new SqlBasicCall(new SqlUnresolvedFunction(new SqlIdentifier("node", SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            SqlFunctionCategory.USER_DEFINED_FUNCTION), args, SqlParserPos.ZERO);
        group.add(func);
        funcs.add(new SqlNodeList(group, SqlParserPos.ZERO));
        hints.add(new SqlNodeList(funcs, SqlParserPos.ZERO));

        // Using hint
        executionContext.setUseHint(true);

        return new SqlNodeList(hints, SqlParserPos.ZERO);
    }

    private ExecutionPlan buildPlanWithHint(SqlNode ast, ExecutionContext ec) {
        final Map<Integer, ParameterContext> param = ec.getParams().getCurrentParameter();
        ExplainResult explain = ec.getExplain();
        boolean isExplain = explain != null;
        ExecutionPlan executionPlan;
        // init HINT
        final HintPlanner hintPlanner = HintPlanner.getInstance(ec.getSchemaName(), ec);
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(ec.getSchemaName(), new HashMap<>(),
            ec.getGroupHint());
        HintConverter.HintCollection hintCollection =
            hintPlanner.collectAndPreExecute(ast, cmdBean, ec.isTestMode(), ec);
        if (!hintCollection.errorMessages.isEmpty()) {
            hintCollection.errorMessages.stream()
                .map(text -> new ExecutionContext.ErrorMessage(0, "", text))
                .forEach(e -> ec.addMessage(ExecutionContext.FailedMessage, e));
        }
        ec.setGroupHint(cmdBean.getGroupHint().toString());
        ec.putAllHintCmds(cmdBean.getExtraCmd());
        cmdBean.setExtraCmd(ec.getExtraCmds());
        ec.setOriginSqlPushdownOrRoute(hintCollection.pushdownSqlOrRoute());

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
        if (cmdBean.isScan()) {
            //force close mpp.
            disableMpp(ec);
        }

        if (hintCollection.pushdownOriginSql()) {
            executionPlan = hintPlanner.direct(ast, cmdBean, hintCollection, param, ec.getSchemaName(), ec);
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
            PostPlanner.usePostPlanner(explain, ec.isUseHint()) || hintCollection.usePostPlanner());
        return executionPlan;
    }

    /**
     * deal with some specialQuery (need parse sql)
     * 1. system variable query
     */
    private SqlNode processSpecialQuery(SqlNode ast, ExecutionContext executionContext) {

        if (executionContext.getSqlType() == SqlType.GET_INFORMATION_SCHEMA && ast instanceof TDDLSqlSelect) {
            boolean enableLogicalInfoSchemaQuery =
                executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_LOGICAL_INFO_SCHEMA_QUERY);
            if (!enableLogicalInfoSchemaQuery) {
                TDDLSqlSelect selectSystemVariableAst = (TDDLSqlSelect) ast;
                selectSystemVariableAst.setHints(buildDirectHint(executionContext.getSchemaName(), executionContext));
            }
        }

        if (executionContext.getLoadDataContext() != null) {
            Preconditions.checkArgument(ast instanceof SqlInsert);

            //Replace the SqlDynamicParam's type for load data.
            List<SqlTypeName> typeNames = executionContext.getLoadDataContext().getValueTypes();

            SqlBasicVisitor<SqlNode> sqlShuttle = new SqlBasicVisitor<SqlNode>() {

                @Override
                public SqlCall visit(SqlCall call) {
                    for (int i = 0; i < call.operandCount(); i++) {
                        if (call.getOperandList().get(i) == null) {
                            continue;
                        }
                        SqlNode sqlNode = call.getOperandList().get(i).accept(this);
                        if (sqlNode != null) {
                            call.setOperand(i, sqlNode);
                        }
                    }
                    return call;
                }

                @Override
                public SqlNode visit(SqlDynamicParam param) {
                    SqlDynamicParam newDynamicParam = new SqlDynamicParam(
                        param.getIndex(), typeNames.get(param.getIndex()), param.getParserPosition());
                    return newDynamicParam;
                }
            };
            ast = ast.accept(sqlShuttle);
        }

        return ast;
    }

    public ExecutionPlan getPlan(SqlNode ast) {
        return getPlan(ast, new PlannerContext());
    }

    public ExecutionPlan getPlan(SqlNode ast, PlannerContext plannerContext) {
        plannerContext.setSqlKind(ast.getKind());
        // disable direct plan for CTE
        if (ast.getKind() == SqlKind.WITH) {
            plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_DIRECT_PLAN, false);
            plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_POST_PLANNER, false);
        }

        // validate
        SqlConverter converter =
            SqlConverter.getInstance(plannerContext.getSchemaName(), plannerContext.getExecutionContext());
        if (GeneralUtil
            .getPropertyBoolean(plannerContext.getExecutionContext().getUserDefVariables(), "auto_partition", false) ||
            GeneralUtil
                .getPropertyLong(plannerContext.getExecutionContext().getUserDefVariables(), "auto_partition", 0)
                != 0) {
            converter.enableAutoPartition();
            // Critical: Do unconditional rewrite on original sql(do this to make async ddl normal).
            final List<SQLStatement> statementList =
                SQLUtils.parseStatements(plannerContext.getExecutionContext().getOriginSql(), JdbcConstants.MYSQL);
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
            converter.enableAutoPartition();
        }

        // Dealing new partition database.
        if (DbInfoManager.getInstance().isNewPartitionDb(plannerContext.getSchemaName())) {
            boolean disableAutoPartitionOnNewPartitionDB = false;
            if (!GeneralUtil.getPropertyBoolean
                (plannerContext.getExecutionContext().getUserDefVariables(), "auto_partition", true)
                || 0 == GeneralUtil.getPropertyLong
                (plannerContext.getExecutionContext().getUserDefVariables(), "auto_partition", 1)) {
                disableAutoPartitionOnNewPartitionDB = true;
            }
            if (plannerContext.getExecutionContext().getParamManager().getProps()
                .containsKey(ConnectionProperties.AUTO_PARTITION) &&
                !plannerContext.getExecutionContext().getParamManager().getBoolean(ConnectionParams.AUTO_PARTITION)) {
                disableAutoPartitionOnNewPartitionDB = true;
            }
            // Use hint to disable auto partition on new partition table. Or it enable by default.
            if (!disableAutoPartitionOnNewPartitionDB) {
                converter.setAutoPartitionDatabase();
                converter.enableAutoPartition();
            }
        }

        SqlNode validatedNode = converter.validate(ast);
        // sqlNode to relNode
        RelNode relNode = converter.toRel(validatedNode, plannerContext);

        // relNode to drdsRelNode
        ToDrdsRelVisitor toDrdsRelVisitor = new ToDrdsRelVisitor(validatedNode, plannerContext);
        RelNode drdsRelNode = relNode.accept(toDrdsRelVisitor);
        if (plannerContext.getSqlKind().belongsTo(QUERY)) {
            Map<LogicalTableScan, RexNode> predicateMap = getRexNodeTableMap(drdsRelNode);
            if (predicateMap != null && !predicateMap.values().stream()
                .anyMatch(predicate -> !PlanManagerUtil.isSimpleCondition(predicate))) {
                plannerContext.setExprMap(predicateMap);
            }
        }
        RelMetadataQuery mq = drdsRelNode.getCluster().getMetadataQuery();
        if (Boolean
            .parseBoolean(plannerContext.getParamManager().getProps().get(ConnectionProperties.PREPARE_OPTIMIZE))) {
            return constructExecutionPlan(mq.getOriginalRowType(drdsRelNode), drdsRelNode, drdsRelNode, validatedNode,
                converter,
                toDrdsRelVisitor, plannerContext,
                false);
        }
        RelNode optimizedNode;
        boolean shouldDirect;
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
            shouldDirect = false;
        } else {
            // optimize
            shouldDirect = shouldDirect(toDrdsRelVisitor, validatedNode, plannerContext);
            optimizedNode = shouldDirect ? unoptimizedNode : optimize(unoptimizedNode, plannerContext);
        }
        ExecutionPlan executionPlan =
            constructExecutionPlan(mq.getOriginalRowType(drdsRelNode), unoptimizedNode, optimizedNode, validatedNode,
                converter,
                toDrdsRelVisitor, plannerContext,
                shouldDirect);

        checkModifyLimitation(executionPlan, validatedNode, plannerContext.getExecutionContext().isUseHint(),
            plannerContext);

        PostPlanner.getInstance().setSkipPostOptFlag(plannerContext, optimizedNode, shouldDirect);

        return executionPlan;
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
        } else if (relNode instanceof LogicalModifyView) {
            CheckModifyLimitation.check((LogicalModifyView) relNode, plannerContext.getExecutionContext());
        } else if (relNode instanceof DirectTableOperation) {
            CheckModifyLimitation.check((DirectTableOperation) relNode);
        }
    }

    /**
     * Optimize a Plan that support scale out
     */
    public RelNode optimizeScaleOutPlan(RelNode input, PlannerContext plannerContext) {
        // To be impl, now just use the method of optimize() instead
        return optimize(input, plannerContext);
    }

    private RelNode sqlRewriteAndPlanEnumerate(RelNode input, PlannerContext plannerContext) {
        CalcitePlanOptimizerTrace.getOptimizerTracer().get().addSnapshot("Start", input, plannerContext);
        RelNode logicalOutput = optimizeBySqlWriter(input, plannerContext);
        CalcitePlanOptimizerTrace.getOptimizerTracer().get()
            .addSnapshot("PlanEnumerate", logicalOutput, plannerContext);

        RelNode bestPlan = optimizeByPlanEnumerator(logicalOutput, plannerContext);

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

        CalcitePlanOptimizerTrace.getOptimizerTracer().get().addSnapshot("Start", input, plannerContext);

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

    public RelNode optimizeByPlanEnumerator(RelNode input, PlannerContext plannerContext) {
        VolcanoPlanner volcanoPlanner = (VolcanoPlanner) input.getCluster().getPlanner();
        volcanoPlanner.clear();

        ParamManager paramManager = plannerContext.getParamManager();
        CountVisitor countVisitor = new CountVisitor();
        input.accept(countVisitor);

        volcanoPlanner.setTopDownOpt(true);
        boolean enableBranchAndBoundOptimization =
            paramManager.getBoolean(ConnectionParams.ENABLE_BRANCH_AND_BOUND_OPTIMIZATION);
        int volcanoStartUpCostJoinLimit = paramManager.getInt(ConnectionParams.CBO_START_UP_COST_JOIN_LIMIT);
        volcanoPlanner.setStartUpCostOpt(countVisitor.getLimitCount() > 0
            && countVisitor.getJoinCount() <= volcanoStartUpCostJoinLimit);
        volcanoPlanner.setEnableBranchAndBound(enableBranchAndBoundOptimization);
        boolean enablePassThrough = paramManager.getBoolean(ConnectionParams.ENABLE_PASS_THROUGH_TRAIT);
        volcanoPlanner.setEnablePassThrough(enablePassThrough);
        boolean enableDerive = paramManager.getBoolean(ConnectionParams.ENABLE_DERIVE_TRAIT);
        volcanoPlanner.setEnableDerive(enableDerive);

        addCBORule(volcanoPlanner, countVisitor, paramManager, plannerContext);

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
        }

        output = optimizeByExpandTableLookup(output, plannerContext);

        if (countVisitor.getJoinCount() > 0) {
            plannerContext.enableSPM(true);
        }

        boolean enableMppCBO = MppPlanCheckers.supportsMppPlan(output, plannerContext, MppPlanCheckers.BASIC_CHECKERS,
            MppPlanCheckers.SIMPLE_QUERY_PLAN_CHECKER);

        // determine TP or AP
        String workloadType = plannerContext.getParamManager().getString(ConnectionParams.WORKLOAD_TYPE);
        if (workloadType == null) {
            plannerContext.setWorkloadType(determineWorkloadType(output, output.getCluster().getMetadataQuery()));
        } else {
            try {
                plannerContext.setWorkloadType(WorkloadType.valueOf(workloadType.toUpperCase()));
            } catch (Throwable t) {
                // ignore
            }
        }

        if (enableMppCBO && WorkloadType.AP == plannerContext.getWorkloadType() && plannerContext.getSqlKind()
            .belongsTo(QUERY)) {
            output = optimizeByMppPlan(output, plannerContext);
        } else {
            output = optimizeBySmpPlan(output, plannerContext);
        }

        output = optimizeByExpandViewPlan(output, plannerContext);

        // deal with subquery
        SubQueryPlanEnumerator subQueryPlanEnumerator = new SubQueryPlanEnumerator(plannerContext.isInSubquery());
        output = output.accept(subQueryPlanEnumerator);

        PlanManagerUtil.applyCache(output);
        return output;
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

        int joinCount = countVisitor.getJoinCount();
        plannerContext.setJoinCount(joinCount);

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
        boolean enableSortMergeJoin = paramManager.getBoolean(ConnectionParams.ENABLE_SORT_MERGE_JOIN);
        boolean enableSemiHashJoin = paramManager.getBoolean(ConnectionParams.ENABLE_SEMI_HASH_JOIN);
        boolean enableSemiNLJoin = paramManager.getBoolean(ConnectionParams.ENABLE_SEMI_NL_JOIN);
        boolean enableMaterializedSemiJoin = paramManager.getBoolean(ConnectionParams.ENABLE_MATERIALIZED_SEMI_JOIN);
        boolean enableSemiSortMergeJoin = paramManager.getBoolean(ConnectionParams.ENABLE_SEMI_SORT_MERGE_JOIN);
        boolean enableHashAGG = paramManager.getBoolean(ConnectionParams.ENABLE_HASH_AGG);
        boolean enableSortAgg = paramManager.getBoolean(ConnectionParams.ENABLE_SORT_AGG);
        ImmutableList<RelOptRule> rules = RuleToUse.CBO_BASE_RULE;

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

    private RelNode optimizeByMppPlan(RelNode input, PlannerContext plannerContext) {
        CalcitePlanOptimizerTrace.getOptimizerTracer().get().addSnapshot("MPP", input, plannerContext);
        RelOptPlanner relOptPlanner = input.getCluster().getPlanner();
        assert relOptPlanner instanceof VolcanoPlanner;
        relOptPlanner.clear();

        for (RelOptRule rule : RuleToUse.MPP_CBO_RULE) {
            relOptPlanner.addRule(rule);
        }

        // TODO: should we use collation as PlanEnumerator input?
        RelTraitSet newTraitSet = input.getTraitSet().simplify().replace(MppConvention.INSTANCE);
        RelNode newInput = relOptPlanner.changeTraits(input, newTraitSet);
        relOptPlanner.setRoot(newInput);

        RelNode output;
        try {
            output = relOptPlanner.findBestExp();
        } catch (RelOptPlanner.CannotPlanException e) {
            logger.error(e);
            throw new RuntimeException("MPP Sql could not be implemented");
        } finally {
            relOptPlanner.clear();
        }

        output = optimizeWithRuntimeFilter(output, plannerContext);
        return output;
    }

    private RelNode optimizeWithRuntimeFilter(RelNode input, PlannerContext plannerContext) {
        boolean enableRuntimeFilter = plannerContext.getParamManager().getBoolean(
            ConnectionParams.ENABLE_RUNTIME_FILTER);
        boolean pushRuntimeFilter = plannerContext.getParamManager().getBoolean(
            ConnectionParams.ENABLE_PUSH_RUNTIME_FILTER_SCAN);

        RelNode output = input;
        if (enableRuntimeFilter) {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addGroupBegin();
            builder.addRuleCollection(RuleToUse.RUNTIME_FILTER);
            if (pushRuntimeFilter) {
                // push filter
                builder.addRuleInstance(PushBloomFilterRule.LOGICALVIEW);
            }
            builder.addGroupEnd();
            HepPlanner planner = new HepPlanner(builder.build());
            planner.stopOptimizerTrace();
            planner.setRoot(input);
            output = planner.findBestExp();
        }

        return output;
    }

    private RelNode optimizeByExpandViewPlan(RelNode input, PlannerContext plannerContext) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addGroupBegin();
        builder.addRuleCollection(RuleToUse.EXPAND_VIEW_PLAN);
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
        builder.addGroupEnd();
        HepPlanner hepPlanner = new HepPlanner(builder.build(), plannerContext);
        hepPlanner.setRoot(input);
        RelNode output = hepPlanner.findBestExp();
        return output;
    }

    private RelNode optimizeByExpandTableLookup(RelNode input, PlannerContext plannerContext) {
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
        builder.addRuleInstance(DrdsLogicalViewConvertRule.INSTANCE);
        builder.addRuleInstance(LogicalJoinToBKAJoinRule.LOGICALVIEW_RIGHT_FOR_EXPAND);
        builder.addRuleInstance(LogicalJoinToBKAJoinRule.LOGICALVIEW_NOT_RIGHT_FOR_EXPAND);
        builder.addRuleInstance(DrdsProjectConvertRule.INSTANCE);
        builder.addRuleInstance(DrdsFilterConvertRule.INSTANCE);
        builder.addRuleInstance(DrdsCorrelateConvertRule.INSTANCE);
        builder.addRuleInstance(DrdsSortConvertRule.INSTANCE);
        builder.addRuleInstance(DrdsOutFileConvertRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(OptimizeLogicalViewRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner hepPlanner = new HepPlanner(builder.build(), plannerContext);
        hepPlanner.setRoot(input);
        //  we must enable BKAJoin to convert tableLookup to BKAJoin
        Object originValue = plannerContext.getExtraCmds().get(ConnectionProperties.ENABLE_BKA_JOIN);
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_BKA_JOIN, true);
        RelNode output = hepPlanner.findBestExp();
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_BKA_JOIN, originValue);
        return output;
    }

    private boolean shouldDirect(ToDrdsRelVisitor toDrdsRelVisitor, SqlNode sqlNode, PlannerContext plannerContext) {
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_DIRECT_PLAN)) {
            return false;
        }
        if (toDrdsRelVisitor.existsWindow()) {
            return false;
        }

        if (toDrdsRelVisitor.isExistsGroupingSets()) {
            return false;
        }

        //should not generate direct plan for any table with gsi
        if (toDrdsRelVisitor.isModifyGsiTable()) {
            return false;
        }

        final boolean onlySingleOrOnlyBroadcast = toDrdsRelVisitor.isAllTableBroadcast()
            || toDrdsRelVisitor.isAllTableSingleNoBroadcast();
        if (toDrdsRelVisitor.isDirect()
            && (sqlNode.getKind() != SqlKind.DELETE || ((SqlDelete) sqlNode).singleTable()
            || onlySingleOrOnlyBroadcast)
            && (sqlNode.getKind() != SqlKind.UPDATE || ((SqlUpdate) sqlNode).singleTable()
            || onlySingleOrOnlyBroadcast)
            && !toDrdsRelVisitor.isContainScaleOutWriableTable()
            && !toDrdsRelVisitor.isContainReplicateWriableTable()) {
            return true;
        }
        return false;
    }

    private ExecutionPlan constructExecutionPlan(RelDataType originalRowType,
                                                 RelNode unoptimizedNode,
                                                 RelNode optimizedNode,
                                                 SqlNode validatedNode,
                                                 SqlConverter converter,
                                                 ToDrdsRelVisitor toDrdsRelVisitor,
                                                 PlannerContext plannerContext,
                                                 boolean constructDirectPlan) {
        final List<String> tableNames = MetaUtils.buildTableNamesForNode(validatedNode);
        CursorMeta cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(originalRowType, tableNames));

        ExecutionPlan result = null;
        RelNode finalPlan = null;
        if (constructDirectPlan) {
            if (validatedNode.isA(DML)) {
                cursorMeta = CalciteUtils.buildDmlCursorMeta();
            }

            // 如果都是单库单表,直接下推,构建逻辑计划
            finalPlan = buildDirectPlan(toDrdsRelVisitor.getBaseLogicalView(),
                unoptimizedNode,
                validatedNode,
                toDrdsRelVisitor.isShouldRemoveSchemaName(), plannerContext);
            if ((validatedNode instanceof SqlSelect && ((SqlSelect) validatedNode).getOutFileParams() != null)) {
                optimizedNode = new LogicalOutFile(finalPlan.getCluster(), finalPlan.getTraitSet(), finalPlan,
                    ((SqlSelect) validatedNode).getOutFileParams());
                cursorMeta = CalciteUtils.buildDmlCursorMeta();
            } else {
                optimizedNode = finalPlan;
            }
        } else {
            if (validatedNode.isA(DML) || optimizedNode instanceof LogicalOutFile) {
                cursorMeta = CalciteUtils.buildDmlCursorMeta();
            }
        }

        // Copy user hint from AST to all logical view.
        if (validatedNode instanceof SqlSelect && !((SqlSelect) validatedNode).getOptimizerHint().getHints()
            .isEmpty()) {
            final UserHintPassThroughVisitor userHintPassthroughVisitor =
                new UserHintPassThroughVisitor(((SqlSelect) validatedNode).getOptimizerHint());
            optimizedNode = optimizedNode.accept(userHintPassthroughVisitor);
        }

        result = new ExecutionPlan(validatedNode, optimizedNode, cursorMeta);

        ExplainResult explainResult = plannerContext.getExecutionContext().getExplain();
        if (isExplainAdvisor(explainResult)) {
            plannerContext.getExecutionContext().setUnOptimizedPlan(unoptimizedNode);
        }

        updatePlanProperties(result, toDrdsRelVisitor, validatedNode);
        initPlanShardInfo(result, plannerContext.getExecutionContext());

        return result;
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

        PlanShardInfo planShardInfo = new PlanShardInfo();
        boolean needInitPlanShard = !(plan instanceof DirectTableOperation) && !(plan instanceof VirtualView) &&
            !((plan instanceof SingleRel) && (((SingleRel) plan).getInput()) instanceof VirtualView);
        needInitPlanShard = needInitPlanShard ? !needSkipInitPlanShardInfo(plan) : needInitPlanShard;

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
                executionPlan.setPlanShardInfo(planShardInfo);
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
        if (visitor.isWithForceIndex()) {
            plan.getPlanProperties().set(ExecutionPlanProperties.WITH_FORCE_INDEX);
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
        return plan;
    }

    /**
     * 查询的所有表都是单表,直接填充一个 LogicalView 返回即可
     */
    private RelNode buildDirectPlan(LogicalView logicalView, RelNode relNode, SqlNode sqlNode,
                                    boolean shouldRemoveSchema, PlannerContext pc) {
        // Remove SchemaName
        if (shouldRemoveSchema) {
            RemoveSchemaNameVisitor visitor = new RemoveSchemaNameVisitor(logicalView.getSchemaName());
            sqlNode = sqlNode.accept(visitor);
        }
        CollectorTableVisitor collectorTableVisitor =
            new CollectorTableVisitor(logicalView.getSchemaName(), pc.getExecutionContext());
        sqlNode = sqlNode.accept(collectorTableVisitor);
        List<String> tableName = collectorTableVisitor.getTableNames();
        sqlNode = sqlNode.accept(new ReplaceLogicalTableNameWithPhysicalTableNameVisitor(logicalView.getSchemaName(),
            pc.getExecutionContext()));
        String sqlTemplate = RelUtils.toNativeSql(sqlNode);

        String t;
        if (sqlNode.isA(DML)) {
            t = Util.last(relNode.getTable().getQualifiedName());
        } else {
            t = tableName.get(0);
        }
        final List<String> physicalTableNames;
        if (DbInfoManager.getInstance().isNewPartitionDb(logicalView.getSchemaName())) {
            physicalTableNames = tableName.stream()
                .map(e -> {
                    PartitionInfo partitionInfo = OptimizerContext.getContext(logicalView.getSchemaName())
                        .getPartitionInfoManager().getPartitionInfo(e);
                    if (partitionInfo != null) {
                        return partitionInfo.getPrefixTableName();
                    }
                    return e;
                }).collect(Collectors.toList());
        } else {
            physicalTableNames = tableName.stream()
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
        List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(sqlNode);

        DirectTableOperation directTableScan = new DirectTableOperation(logicalView,
            relNode.getRowType(),
            tableName,
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
        // Generate XPlan via raw relnode.
        // TODO: lock not supported now.
        // Always generate the XPlan in case of switching connection pool.
        if (sqlNode.getKind() == SqlKind.SELECT
            && ((SqlSelect) sqlNode).getLockMode() == SqlSelect.LockMode.UNDEF) {
            final RelToXPlanConverter converter = new RelToXPlanConverter();
            try {
                directTableScan.setXTemplate(converter.convert(RelXPlanOptimizer.optimize(relNode)));
            } catch (Exception e) {
                Throwable throwable = e;
                while (throwable.getCause() != null && throwable.getCause() instanceof InvocationTargetException) {
                    throwable = ((InvocationTargetException) throwable.getCause()).getTargetException();
                }
                logger.info("XPlan converter: " + throwable.getMessage());
            }
        }
        // Init sql digest.
        try {
            directTableScan.setSqlDigest(com.google.protobuf.ByteString
                .copyFrom(MessageDigest.getInstance("md5").digest(directTableScan.getNativeSql().getBytes())));
        } catch (Exception ignore) {
        }

        return directTableScan;
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
        processParameter(sqlParameterized.getParameters(), executionContext);
        processMsah(sqlParameterized, executionContext);
        executionContext.setSqlType(sqlType);
        processCpuProfileForSqlType(executionContext);
        long startOptimizePlan = isStatCpuForBuildPlan(executionContext) ? ThreadCpuStatUtil.getThreadCpuTimeNano() : 0;

        ExecutionPlan executionPlan;
        if (PlanManagerUtil.useSpm(sqlParameterized, executionContext)) {
            if (executionContext.isEnableFeedBackWorkload()) {
                // plan management with feedback
                executionPlan = PlaceHolderExecutionPlan.INSTANCE;
            } else {
                // plan management
                PlanManager planManager =
                    OptimizerContext.getContext(executionContext.getSchemaName()).getPlanManager();
                if (forPrepare) {
                    executionPlan = planManager.choosePlanForPrepare(sqlParameterized, sqlNodeList, executionContext);
                } else {
                    executionPlan = planManager.choosePlan(sqlParameterized, executionContext);
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

        // post planner
        if (executionPlan.isUsePostPlanner()) {
            ExecutionPlan executionPlanForPostPlanner = executionPlan.copy(executionPlan.getPlan());
            executionPlan = PostPlanner.getInstance().optimize(executionPlanForPostPlanner, executionContext);
        }

        PlannerContext.getPlannerContext(executionPlan.getPlan()).setExecutionContext(executionContext);
        executionContext.setFinalPlan(executionPlan);
        return executionPlan;
    }

    protected void processCpuProfileForSqlType(ExecutionContext executionContext) {

        SqlType sqlType = executionContext.getSqlType();
        if (!SqlTypeUtils.isDmlAndDqlSqlType(sqlType)) {
            // Optimize the performance for GSI
            executionContext.setOnlyUseTmpTblPool(true);
            executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, 0);
        } else {
            if (!SqlTypeUtils.isSelectSqlType(sqlType)) {
                executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, 1);
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

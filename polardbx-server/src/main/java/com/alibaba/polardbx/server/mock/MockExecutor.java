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

package com.alibaba.polardbx.server.mock;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import com.alibaba.polardbx.optimizer.config.table.Relationship;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.MockStatisticDatasource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.core.rel.RemoveSchemaNameVisitor;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator.CmdBean;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter.HintCollection;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.HintParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionBy;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionDefinition;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionOptions;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionBy;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.TableRuleUtil;
import com.alibaba.polardbx.optimizer.utils.newrule.IPartitionGen;
import com.alibaba.polardbx.optimizer.utils.newrule.ISubpartitionGen;
import com.alibaba.polardbx.optimizer.utils.newrule.RuleUtils;
import com.alibaba.polardbx.optimizer.utils.newrule.ShardFuncParamsChecker;
import com.alibaba.polardbx.optimizer.utils.newrule.TableRuleGenFactory;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import com.alibaba.polardbx.rule.impl.WrappedGroovyRule;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author desai
 */
public class MockExecutor {

    private final static Logger logger = LoggerFactory.getLogger(MockExecutor.class);

    public OptimizerContext context;
    private RuleUtil ruleUtil;

    public MockExecutor() {
        init();
        ruleUtil = new RuleUtil(context);
    }

    private static final String schemaName = "TEST_APP";

    public String getPlan(ByteString sql) {
        if ("clear_mock".equalsIgnoreCase(sql.toString().trim())) {
            clear();
            return "ok";
        }
        // set optimizer context
        OptimizerContext.loadContext(context);

        // judge if create table statement
        // all sqls should be parsed by fastsql
        SqlNodeList sqlNodes = new FastsqlParser().parse(sql);
        if (sqlNodes.get(0) instanceof SqlCreateTable) {
            SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNodes.get(0);
            String tableName = ((SqlIdentifier) sqlCreateTable.getName()).getLastName();
            TableMeta tableMeta = TableMetaParser.parse(tableName, sqlCreateTable);
            TableRule tableRule = ruleUtil.generateTableRule(sqlCreateTable, tableMeta);
            addTable(tableMeta, tableRule);
            return "ok";
        }

        // for old hint format
        int hintCount = HintParser.getInstance().getAllHintCount(sql);
        if (hintCount > 0) {
            sql = HintUtil.convertSimpleHint(sql, new HashMap<>());
        }

        return getPlanInner(sql).trim();
    }

    private String getPlanInner(ByteString testSql) {
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        SqlParameterized sqlParameterized =
            SqlParameterizeUtils.parameterize(testSql, currentParameter, new ExecutionContext(), false);

        if (sqlParameterized == null) {
            return "ok";
        }
        final Map<Integer, ParameterContext> param = new HashMap<>();
        ExecutionContext context = new ExecutionContext();
        final SqlNodeList astList = parse(testSql, sqlParameterized, param, context);

        context.setParams(new Parameters(param, false));
        final ExecutionPlan executionPlan = optimize(context, astList);
        return RelUtils.toString(executionPlan, param);
    }

    private ExecutionPlan optimize(ExecutionContext ec, SqlNodeList astList) {
        final Map<Integer, ParameterContext> param = ec.getParams().getCurrentParameter();
        final Map<String, Object> cmdExtra = new HashMap<>();
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);

        SqlNode ast = astList.get(0);
        // remove schema. if support multischema, schema name must kept
        MockRemoveSchemaNameVisitor visitor = new MockRemoveSchemaNameVisitor(schemaName);
        ast = ast.accept(visitor);

        ExecutionPlan executionPlan;
        HintCollection hintCollection = new HintCollection();
        if (ec.isUseHint()) {
            // init HINT
            final HintPlanner hintPlanner = HintPlanner.getInstance(schemaName, ec);
            final CmdBean cmdBean = new CmdBean(schemaName, cmdExtra, null);
            hintCollection = hintPlanner.collectAndPreExecute(ast, cmdBean, ec.isTestMode(), ec);

            if (hintCollection.pushdownOriginSql()) {
                executionPlan = hintPlanner.direct(ast, cmdBean, hintCollection, param, schemaName, ec);
            } else if (hintCollection.cmdOnly()) {
                if (ast instanceof SqlExplain) {
                    executionPlan = hintPlanner
                        .pushdown(Planner.getInstance().getPlan(
                                ((SqlExplain) ast).getExplicandum(), plannerContext),
                            ast,
                            cmdBean,
                            hintCollection,
                            param,
                            ec.getExtraCmds(), ec);
                } else {
                    executionPlan = hintPlanner.pushdown(Planner.getInstance().getPlan(ast, plannerContext),
                        ast,
                        cmdBean,
                        hintCollection,
                        param,
                        ec.getExtraCmds(), ec);
                }
            } else {
                PlannerContext pc = plannerContext;
                pc.setExecutionContext(ec);
                pc.setParams(new Parameters(param, false));
                executionPlan = hintPlanner.getPlan(ast, pc, ec);
            }

        } else {
            executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
        }

        /**
         * PostPlanner
         */
        if (!ec.isUseHint() || hintCollection.usePostPlanner()) {

            executionPlan = PostPlanner.getInstance().optimize(executionPlan, ec);

        }
        return executionPlan;
    }

    private SqlNodeList parse(ByteString testSql, SqlParameterized sqlParameterized,
                              Map<Integer, ParameterContext> param,
                              ExecutionContext context) {
        SqlNodeList astList;
        if (null != sqlParameterized) {
            param.putAll(OptimizerUtils.buildParam(sqlParameterized.getParameters(), context));
            astList = new FastsqlParser().parse(sqlParameterized.getSql(), sqlParameterized.getParameters(), context);
        } else {
            astList = new FastsqlParser().parse(testSql, context);
        }
        return astList;
    }

    public void clear() {
        context.getLatestSchemaManager().invalidateAll();
        // clear tddlRule's tableRules
        context.getRuleManager().getTddlRule().getCurrentRule().setTableRules(new HashMap<>());
    }

    public void addTable(TableMeta tableMeta, TableRule tableRule) {
        // SimpleTableStatisticSysTable simpleTableStatisticSysTable = new
        // SimpleTableStatisticSysTable();
        // tableMeta.setTableStatistics(simpleTableStatisticSysTable.getStatistic(tableMeta.getTableName()));
        context.getLatestSchemaManager().putTable(tableMeta.getTableName(), tableMeta);

        if (tableRule != null) {
            // update tddlRule's tableRules
            context.getRuleManager().getTddlRule().getCurrentRule().getTableRules()
                .put(tableMeta.getTableName(), tableRule);
        }
    }

    private void init() {
        // set default dbIndex
        TddlRule tddlRule = getTddlRule();
        tddlRule.setDefaultDbIndex("test_0000");

        PartitionInfoManager partInfoMgr = new PartitionInfoManager(schemaName, schemaName, true);
        TableGroupInfoManager tableGroupInfoManager = new TableGroupInfoManager(schemaName);

        TddlRuleManager rule = new TddlRuleManager(tddlRule, partInfoMgr, tableGroupInfoManager, schemaName);
        SchemaManager schemaManager = new MockSchemaManager();
        context = new OptimizerContext(schemaName);
        context.setSqlMock(true);
        context.setMatrix(getMatrix());
        context.setSchemaManager(schemaManager);
        context.setRuleManager(rule);
        StatisticManager.sds = MockStatisticDatasource.getInstance();
    }

    private Matrix getMatrix() {
        List<Group> groups = new LinkedList<>();
        groups.add(fakeGroup("test_0000"));
        groups.add(fakeGroup("test_0001"));
        groups.add(fakeGroup("test_0002"));
        groups.add(fakeGroup("test_0003"));

        Matrix matrix = new Matrix();
        matrix.setGroups(groups);

        return matrix;
    }

    private static Group fakeGroup(String name) {
        Group g = new Group();
        g.setName(name);
        return g;
    }

    private TddlRule getTddlRule() {
        TddlRule tr = new TddlRule();
        tr.setAllowEmptyRule(true);
        tr.init();

        return tr;
    }

    public static class MockSchemaManager implements SchemaManager {

        private Map<String, TableMeta> tableMetaMap = new HashMap<>();

        @Override
        public TableMeta getTable(String tableName) {
            if (tableName.equalsIgnoreCase(DUAL)) {
                return buildDualTable();
            }

            if (tableMetaMap.containsKey(tableName.toUpperCase())) {
                return tableMetaMap.get(tableName.toUpperCase());
            } else {
                String error = String.format("Table %s not exists, existing tables: %s, hashcode: %s",
                    tableName.toUpperCase(),
                    StringUtils.join(tableMetaMap.keySet().toArray(), " ,"),
                    this.hashCode());
                throw new RuntimeException(error);
            }
        }

        @Override
        public Collection<TableMeta> getAllTables() {
            return tableMetaMap.values();
        }

        public TableMeta buildDualTable() {
            IndexMeta index = new IndexMeta(SchemaManager.DUAL,
                new ArrayList<ColumnMeta>(),
                new ArrayList<ColumnMeta>(),
                IndexType.NONE,
                Relationship.NONE,
                false,
                true,
                true,
                "");

            return new TableMeta(schemaName, DUAL, new ArrayList<ColumnMeta>(), index, new ArrayList<IndexMeta>(), true,
                TableStatus.PUBLIC, 0, 0);
        }

        @Override
        public void putTable(String tableName, TableMeta tableMeta) {
            tableMetaMap.put(tableName.toUpperCase(), tableMeta);
        }

        @Override
        public void reload(String tableName) {

        }

        @Override
        public void invalidate(String tableName) {

        }

        @Override
        public void invalidateAll() {
            tableMetaMap.clear();
        }

        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public boolean isInited() {
            return true;
        }

        @Override
        public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
            return GsiMetaManager.GsiMetaBean.empty();
        }

        @Override
        public String getSchemaName() {
            return "test";
        }
    }

    public static class MockRemoveSchemaNameVisitor extends RemoveSchemaNameVisitor {

        public MockRemoveSchemaNameVisitor(String logicalSchemaName) {
            super(logicalSchemaName);
        }

        @Override
        public SqlNode visit(SqlCall call) {
            SqlKind kind = call.getKind();
            if (kind == SqlKind.SELECT) {
                SqlSelect select = (SqlSelect) call;

                /**
                 * Replace tableName at From.
                 */
                SqlNode from = select.getFrom();
                if (from != null) {
                    SqlKind fromKind = from.getKind();
                    // 单表查询
                    if (fromKind == SqlKind.IDENTIFIER) {
                        SqlIdentifier identifier = (SqlIdentifier) from;
                        select.setFrom(buildNewIdentifier(identifier));
                    } else if (fromKind == SqlKind.AS) {
                        select.setFrom(buildAsNode(from));
                    } else if (fromKind == SqlKind.JOIN) {
                        // 多表JOIN
                        select.setFrom(visit((SqlJoin) from));
                    }
                }

                /**
                 * Replace tableName at where subSelect
                 */
                SqlNode where = select.getWhere();
                if (where instanceof SqlCall) {
                    select.setWhere(visit((SqlCall) where));
                }

                /**
                 * Having
                 */
                SqlNode having = select.getHaving();
                if (having instanceof SqlCall) {
                    select.setHaving(visit((SqlCall) having));
                }
                return select;
            }

            if (kind == SqlKind.DELETE) {
                final SqlDelete delete = (SqlDelete) call;

                if (delete.singleTable()) {
                    Preconditions.checkArgument(delete.getTargetTable().getKind() == SqlKind.IDENTIFIER);
                    /**
                     * set TargetTable, DELETE DO NOT use alias for MYSQL.
                     */
                    SqlIdentifier targetTable = (SqlIdentifier) delete.getTargetTable();
                    delete.setOperand(0, buildNewIdentifier(targetTable));
                } else {
                    /**
                     * Multi table delete
                     */
                    SqlNode from = delete.getSourceTableNode();
                    if (from != null) {
                        SqlKind fromKind = from.getKind();
                        // 单表
                        if (fromKind == SqlKind.IDENTIFIER) {
                            SqlIdentifier identifier = (SqlIdentifier) from;
                            delete.setFrom(buildNewIdentifier(identifier));
                        } else if (fromKind == SqlKind.AS) {
                            delete.setFrom(buildAsNode(from));
                        } else if (fromKind == SqlKind.JOIN) {
                            // 多表
                            delete.setFrom(visit((SqlJoin) from));
                        }
                    }

                    /**
                     * Replace tableName at where subSelect
                     */
                    SqlNode where = delete.getCondition();
                    if (where instanceof SqlCall) {
                        delete.setCondition(visit((SqlCall) where));
                    }
                }

                return delete;
            }

            if (kind == SqlKind.UPDATE) {
                final SqlUpdate update = (SqlUpdate) call;
                final SqlNode targetTable = update.getTargetTable();

                if (update.singleTable()) {
                    if (targetTable.getKind() == SqlKind.IDENTIFIER) {
                        update.setOperand(0, buildNewIdentifier((SqlIdentifier) targetTable));
                    } else if (targetTable.getKind() == SqlKind.AS) {
                        update.setOperand(1, buildAsNode(targetTable));
                    } else {
                        throw new UnsupportedOperationException("Unsupported update syntax.");
                    }
                } else {
                    /**
                     * Multi table update
                     */
                    final SqlKind targetKind = targetTable.getKind();
                    // 单表
                    if (targetKind == SqlKind.IDENTIFIER) {
                        final SqlIdentifier identifier = (SqlIdentifier) targetTable;
                        update.setTargetTable(buildNewIdentifier(identifier));
                    } else if (targetKind == SqlKind.AS) {
                        update.setTargetTable(buildAsNode(targetTable));
                    } else if (targetKind == SqlKind.JOIN) {
                        // 多表
                        update.setTargetTable(visit((SqlJoin) targetTable));
                    }

                    /**
                     * Replace tableName at where subSelect
                     */
                    SqlNode where = update.getCondition();
                    if (where instanceof SqlCall) {
                        update.setCondition(visit((SqlCall) where));
                    }
                }

                return update;
            }

            if (kind == SqlKind.INSERT || kind == SqlKind.REPLACE) {
                return buildInsertNode(call);
            }

            if (kind == SqlKind.AS) {
                return buildAsNode(call);
            }

            if (kind == SqlKind.JOIN) {
                return buildJoinNode(call);
            }

            if (kind == SqlKind.DROP_TABLE) {
                SqlDropTable dropTalbe = (SqlDropTable) call;
                SqlNode targetTable = dropTalbe.getTargetTable();
                SqlKind targetKind = targetTable.getKind();
                // 单表
                if (targetKind == SqlKind.IDENTIFIER) {
                    final SqlIdentifier identifier = (SqlIdentifier) targetTable;
                    dropTalbe.setTargetTable(buildNewIdentifier(identifier));
                } else if (targetKind == SqlKind.AS) {
                    dropTalbe.setTargetTable(buildAsNode(targetTable));
                }

                return dropTalbe;
            }

            if (call instanceof SqlBasicCall) {
                for (int i = 0; i < call.operandCount(); i++) {
                    SqlNode child = call.operand(i);
                    if (child != null) {
                        call.setOperand(i, child.accept(this));
                    }
                }
            }

            return call;
        }

        @Override
        public SqlNode buildNewIdentifier(SqlIdentifier identifier) {
            List<String> names = identifier.names;
            if (names.size() == 1) {
                return identifier;
            } else if (names.size() == 2) {
                return new SqlIdentifier(names.get(1), SqlParserPos.ZERO);
            } else {
                // Impossible.
                throw new OptimizerException("Size of identifier names is not 1 or 2.");
            }
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            return literal;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            return id;
        }

        @Override
        public SqlNode visit(SqlDataTypeSpec type) {
            return type;
        }

        @Override
        public SqlNode visit(SqlDynamicParam param) {
            return param;
        }

        @Override
        public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
            return intervalQualifier;
        }
    }

    public static class RuleUtil {

        private OptimizerContext context;

        public RuleUtil(OptimizerContext context) {
            this.context = context;
        }

        public TableRule generateTableRule(SqlCreateTable sqlCreateTable, TableMeta tableMeta) {
            TableRule tableRule = null;

            if (sqlCreateTable.isBroadCast()) {
                if (sqlCreateTable.getDbpartitionBy() != null || sqlCreateTable.getTbpartitionBy() != null) {
                    throw new IllegalArgumentException("broadcast and dbpartition are exclusive!");
                }
                tableRule = processBroadcast(tableMeta.getTableName(), tableMeta);
            } else if (sqlCreateTable.getDbpartitionBy() != null || sqlCreateTable.getTbpartitionBy() != null) {
                // tableRule = process(((SqlIdentifier)
                // sqlCreateTable.getOperandList().get(0)).getSimple(),tableToSchema);
                tableRule = buildTableRule(tableMeta.getTableName(),
                    tableMeta,
                    sqlCreateTable.getDbpartitionBy(),
                    sqlCreateTable.getDbpartitions(),
                    sqlCreateTable.getTbpartitionBy(),
                    sqlCreateTable.getTbpartitions(),
                    sqlCreateTable.getMappingRules());
            } else {
                tableRule = new TableRule();
                /**
                 * 伪造一个topo,用于发规则
                 */
                String defaultDbIndex = context.getRuleManager().getDefaultDbIndex(null);
                Map<String, Set<String>> topo = new HashMap<String, Set<String>>();
                topo.put(defaultDbIndex,
                    new HashSet<String>(
                        Arrays.asList(Util.last(((SqlIdentifier) sqlCreateTable.getOperandList().get(0)).names))));
                tableRule.setActualTopology(topo);
                tableRule.setBroadcast(false);
                tableRule.setDbNamePattern(defaultDbIndex);
                tableRule.setTbNamePattern(Util.last(((SqlIdentifier) sqlCreateTable.getOperandList().get(0)).names));
            }

            tableRule.doInit();
            return tableRule;
        }

        private TableRule processBroadcast(String tableName, TableMeta tableMeta) {
            TableRule tableRule = new TableRule();

            tableRule.setBroadcast(true);

            int table_count_on_each_group = 1; /* 一定单表 */
            int group_count = 1; /* 广播规则生成只需要1 */

            String defaultDb = context.getRuleManager().getDefaultDbIndex(null);

            /* 用于检查 */
            List<String> selGroupList = new ArrayList<String>();
            String dbNamePattern = defaultDb;
            tableRule.setDbNamePattern(dbNamePattern);
            /* 这里需要跳过后面的检测 */
            selGroupList.add(defaultDb);

            /* 借用HASH的partition/subpartition生成法则 */
            IPartitionGen partitionGen = TableRuleGenFactory.getInstance()
                .createDBPartitionGenerator(context.getSchemaName(), PartitionByType.HASH);
            ISubpartitionGen subpartitionGen = TableRuleGenFactory.getInstance()
                .createTBPartitionGenerator(context.getSchemaName(), PartitionByType.HASH, PartitionByType.HASH);

            List<String> partitionParams = new ArrayList<String>();
            partitionGen.fillDbRuleStandAlone(tableRule, partitionParams, // 分库键
                tableMeta,
                group_count, // 分库数
                1,
                null);

            subpartitionGen.fillTbRuleStandAlone(tableRule, partitionParams, // 分表键
                tableMeta,
                group_count,
                table_count_on_each_group,
                tableName,
                null);

            /* 总是加上allowfulltablescan */
            tableRule.setAllowFullTableScan(true);

            /**
             * 这里可以直接做规则推导,因为来源是从SQL来的不存在 并发竞争的问题.
             */
            tableRule.init();

            /**
             * 这里特别的有一种情况，因为前面是通过真实的group列表来生成tableRule的
             * 但这里是通过这个抽象的tableRule来枚举出计算出的group列表的，这就要求
             * 真实的group必须是连续的，否则如果真实的是0,1,3,4，我这里计算的结果则会是
             * 0,1,2,3就会出错，而且这种错误是在执行的时候才会发现的，而且即使我在这里可以将
             * 真实的groupList带过来，但是之行的时候还是会shard到不存在的group中，这样还不如在
             * 建库的时候直接报错比较好，所以此处需要做预先校验工作
             */
            Map<String, Set<String>> topology = tableRule.getActualTopology();

            /* 只要选择的group能包含所有的推演出的group就代表OK */
            if (!RuleUtils.checkIfGroupMatch(new HashSet<String>(selGroupList), topology.keySet())) {
                throw new IllegalArgumentException("Selected physical group list is invalid:" + selGroupList);
            }

            /**
             * 因为前面的各种方式目的就是保证所有分表编号全局唯一，所以 这里进行最后的检查
             */
            String dbRule = (tableRule.getDbRuleStrs() == null || tableRule.getDbRuleStrs().length == 0) ? null :
                tableRule.getDbRuleStrs()[0];
            String tbRule = (tableRule.getTbRulesStrs() == null || tableRule.getTbRulesStrs().length == 0) ? null :
                tableRule.getTbRulesStrs()[0];

            /**
             * 检查最终生成的分表数与期望分表数是否相同
             */
            if (!RuleUtils.checkIfTableNumberOk(topology, group_count, table_count_on_each_group)) {
                throw new IllegalArgumentException("Generated table number mismatch" + " dbName: "
                    + tableRule.getDbNamePattern() + " tbName: "
                    + tableRule.getTbNamePattern() + " dbRule: " + dbRule + " tbRule: "
                    + tbRule);
            }

            return tableRule;
        }

        private TableRule buildTableRule(String tableName, TableMeta tableToSchema, SqlNode dbpartitionBy,
                                         SqlNode dbpartitions, SqlNode tbpartitionBy, SqlNode tbpartitions,
                                         List<MappingRule> mappingRules) {
            TableRule tableRule;
            DBPartitionBy dbPartitionByRule = new DBPartitionBy();
            TBPartitionBy tbPartitionByRule = new TBPartitionBy();
            DBPartitionOptions dbPartitionOptions = new DBPartitionOptions();
            if (dbpartitionBy != null) {
                final List<String> paramNames = new ArrayList<>();
                final SqlBasicCall dbFunBasicCall = (SqlBasicCall) dbpartitionBy;
                final SqlOperator operator = dbFunBasicCall.getOperator();
                String dbFunName = operator.getName();
                if (operator instanceof SqlBetweenOperator) {
                    final SqlNode sqlNode = dbFunBasicCall.getOperandList().get(0);
                    if (sqlNode instanceof SqlBasicCall) {
                        final SqlBasicCall sqlNode1 = (SqlBasicCall) sqlNode;
                        dbFunName = sqlNode1.getOperator().getName();
                        paramNames.add(((SqlIdentifier) sqlNode1.getOperandList().get(0)).getSimple());
                    }
                } else {
                    final List<SqlNode> operandList = dbFunBasicCall.getOperandList();
                    for (int i = 0; i < operandList.size(); i++) {
                        final SqlNode sqlNode = operandList.get(i);
                        if (sqlNode instanceof SqlIdentifier) {
                            final String simple = ((SqlIdentifier) sqlNode).getSimple();
                            paramNames.add(simple);
                        } else if (sqlNode instanceof SqlNumericLiteral) {
                            final Object value = ((SqlNumericLiteral) sqlNode).getValue();
                            if (value instanceof BigDecimal) {
                                paramNames.add(((BigDecimal) value).toPlainString());
                            } else {
                                paramNames.add(value.toString());
                            }
                        }
                    }
                }
                dbPartitionByRule.setColExpr(paramNames);
                dbPartitionByRule.setType(PartitionByType.valueOf(dbFunName.toUpperCase()));
                dbPartitionOptions.setDbpartitionBy(dbPartitionByRule);
            }
            if (dbpartitions != null) {
                Integer dbCounts = ((SqlLiteral) dbpartitions).intValue(false);
                dbPartitionOptions.setDbpartitions(dbCounts);
            }

            if (tbpartitionBy != null) {
                final List<String> paramNames = new ArrayList<>();
                final SqlBasicCall tbFunBasicCall = (SqlBasicCall) tbpartitionBy;
                final SqlOperator operator = tbFunBasicCall.getOperator();
                String tbFunName = operator.getName();
                if (operator instanceof SqlBetweenOperator) {
                    final SqlNode sqlNode = tbFunBasicCall.getOperandList().get(0);
                    if (sqlNode instanceof SqlBasicCall) {
                        final SqlBasicCall sqlNode1 = (SqlBasicCall) sqlNode;
                        tbFunName = sqlNode1.getOperator().getName();
                        paramNames.add(((SqlIdentifier) sqlNode1.getOperandList().get(0)).getSimple());
                    }
                    final List<SqlNode> operandList = tbFunBasicCall.getOperandList();
                    assert operandList.size() == 3;
                    final SqlNode between = operandList.get(1);
                    final SqlNode and = operandList.get(2);
                    if (between instanceof SqlNumericLiteral) {
                        final Object value = ((SqlNumericLiteral) between).getValue();
                        if (value instanceof BigDecimal) {
                            dbPartitionOptions.setStartWith(((BigDecimal) value).toBigInteger().intValue());
                        } else {
                            dbPartitionOptions.setStartWith(Integer.valueOf(value.toString()));
                        }
                    }

                    if (and instanceof SqlNumericLiteral) {
                        final Object value = ((SqlNumericLiteral) and).getValue();
                        if (value instanceof BigDecimal) {
                            dbPartitionOptions.setEndWith(((BigDecimal) value).toBigInteger().intValue());
                        } else {
                            dbPartitionOptions.setEndWith(Integer.valueOf(value.toString()));
                        }
                    }
                } else {
                    final List<SqlNode> operandList = tbFunBasicCall.getOperandList();
                    for (int i = 0; i < operandList.size(); i++) {
                        final SqlNode sqlNode = operandList.get(i);
                        if (sqlNode instanceof SqlIdentifier) {
                            final String simple = ((SqlIdentifier) sqlNode).getSimple();
                            paramNames.add(simple);
                        } else if (sqlNode instanceof SqlNumericLiteral) {
                            final Object value = ((SqlNumericLiteral) sqlNode).getValue();
                            if (value instanceof BigDecimal) {
                                paramNames.add(((BigDecimal) value).toPlainString());
                            } else {
                                paramNames.add(value.toString());
                            }
                        }
                    }
                }
                tbPartitionByRule.setColExpr(paramNames);
                tbPartitionByRule.setType(PartitionByType.valueOf(tbFunName));
                dbPartitionOptions.setTbpartitionBy(tbPartitionByRule);
            }

            if (tbpartitions != null) {
                Integer tbCounts = ((SqlLiteral) tbpartitions).intValue(false);
                dbPartitionOptions.setTbpartitions(tbCounts);
            }

            tableRule = processDBPartitionOptions(tableName, tableToSchema, dbPartitionOptions);

            // 热点映射
            if (mappingRules != null && mappingRules.size() > 0) {
                tableRule.setExtPartitions(mappingRules);
                tableRule.initExtTopology();

                for (Object dbRule : tableRule.getDbShardRules()) {
                    if (!(dbRule instanceof WrappedGroovyRule)) {
                        throw new UnsupportedOperationException("Not supported db rule type for hot mapping");
                    }
                }

                if (tableRule.getTbShardRules() != null) {
                    for (Object tbRule : tableRule.getTbShardRules()) {
                        if (!(tbRule instanceof WrappedGroovyRule)) {
                            throw new UnsupportedOperationException("Not supported table rule type for hot mapping");
                        }
                    }
                }
            }
            return tableRule;
        }

        public TableRule processDBPartitionOptions(String tableName, TableMeta tableMeta,
                                                   DBPartitionOptions dbpartitionOptions) {
            if (dbpartitionOptions == null) {
                /* 无dbpartition为单库单表 */
                return null;
            }

            DBPartitionBy dbpartitionBy = dbpartitionOptions.getDbpartitionBy();
            TBPartitionBy tbpartitionBy = dbpartitionOptions.getTbpartitionBy();
            List<DBPartitionDefinition> dbpartitionDefinitionList = dbpartitionOptions.getDbpartitionDefinitionList();

            DBPartitionDefinition dbpartitionDefinition = null;
            if (dbpartitionDefinitionList != null && dbpartitionDefinitionList.size() > 0) {
                /* only take item(0) of partitionDefinitionList */
                dbpartitionDefinition = dbpartitionDefinitionList.get(0);
            }
            TBPartitionDefinition tbpartitionDefinition = null;
            if (dbpartitionDefinition != null && dbpartitionDefinition.getTbpartitionDefinitionList() != null
                && dbpartitionDefinition.getTbpartitionDefinitionList().size() > 0) {
                /* only take item(0) of SubpartitionDefinitionList */
                tbpartitionDefinition = dbpartitionDefinition.getTbpartitionDefinitionList().get(0);
            }

            TableRule tableRule = new TableRule();
            boolean needCheckTable = true;
            /**
             * 判断partition by,如果为NULL，则没有规则，而是SQL直接下推到缺省DB Statement
             * node的PartitionBy作为后面是否存在partition部分的直接依据，也就是说不允许
             * 没有Partition的Subpartition关键字的形式。
             */
            if (dbpartitionBy == null && tbpartitionBy == null) {
                return null;
            }

            /**
             * 分库分表数处理 group数目 没有设置的时候则自动处理， 如果设置成0则报错 tablePerGroup数目
             * 没有设置的时候是1，如果设置成0则报错
             */
            int group_count;
            if (dbpartitionOptions.getDbpartitions() == null) {
                if (dbpartitionBy == null) {
                    /**
                     * 只分表不分库
                     */
                    group_count = 1;
                } else {
                    /**
                     * 正常的分库, 只是没写dbpartitions的情况
                     */
                    group_count = 0; /* 后面会自动进行替换 */
                }
            } else {
                if (dbpartitionOptions.getDbpartitions() < 1) {
                    throw new IllegalArgumentException("dbpartitions should > 0");
                }
                group_count = dbpartitionOptions.getDbpartitions(); // 分库数
            }

            int table_count_on_each_group;
            if (dbpartitionOptions.getStartWith() != null || dbpartitionOptions.getEndWith() != null) {
                table_count_on_each_group = 2;
                needCheckTable = false;
                // 对于noloop版本的spe time, 分表的创建由指定的范围决定, 在规则计算之前不确定每个分库要创建多少张分表
                // 每个分库的表数暂时写死为2,
                // 这个逻辑决定着tbpattern，如果为1的话，说明每个分库只创建一张表则默认为逻辑表，所以这里的值要比1大即可
            } else if (dbpartitionOptions.getTbpartitions() == null) {
                table_count_on_each_group = 1;
            } else {
                if (dbpartitionOptions.getTbpartitions() < 1) {
                    throw new IllegalArgumentException("tbpartitions should > 0");
                }
                table_count_on_each_group = dbpartitionOptions.getTbpartitions(); // 每个库的分表数
            }

            if (dbpartitionBy != null && dbpartitionBy.getColExpr().size() == 0) {
                throw new IllegalArgumentException("Can't set dbpartition key to empty!");
            }

            if (tbpartitionBy != null && tbpartitionBy.getColExpr().size() == 0) {
                throw new IllegalArgumentException("Can't set tbpartition key to empty!");
            }

            if (dbpartitionBy != null
                && (dbpartitionBy.getType() == PartitionByType.MM || dbpartitionBy.getType() == PartitionByType.DD
                || dbpartitionBy.getType() == PartitionByType.WEEK
                || dbpartitionBy.getType() == PartitionByType.MMDD)) {
                throw new IllegalArgumentException("Not support dbpartition method date");
            }

            /* 获得所有真实group的列表 */
            List<Group> dbList = context.getMatrix().getGroups();

            /* 用于检查 */
            List<String> selGroupList = new ArrayList<String>();

            /* 计算缺省的dbname pattern */
            if (dbpartitionDefinition == null || dbpartitionDefinition.getPartition_name() == null) {
                /* 没有指定partition的形式的时候，自动生成 */
                String defaultDb = context.getRuleManager().getDefaultDbIndex(null);

                String dbNamePattern = RuleUtils.genDBPatitionDefinition(group_count,
                    table_count_on_each_group,
                    dbList,
                    defaultDb,
                    selGroupList);

                if (group_count == 0) {
                    /* 根据自动推断取得最大相似的group大小 */
                    group_count = selGroupList.size();
                    if (group_count == 0) {
                        /* 如果group数没有指定,则设为默认8 */
                        group_count = 8;
                    }
                }

                dbpartitionDefinition = new DBPartitionDefinition();
                dbpartitionDefinition.setPartition_name(SqlLiteral.createCharString(dbNamePattern, SqlParserPos.ZERO));
                dbpartitionDefinition.setStartWith(dbpartitionOptions.getStartWith());
                dbpartitionDefinition.setEndWith(dbpartitionOptions.getEndWith());

                tbpartitionDefinition = new TBPartitionDefinition();
                tbpartitionDefinition.setStartWith(dbpartitionOptions.getStartWith());
                tbpartitionDefinition.setEndWith(dbpartitionOptions.getEndWith());
            } else {
                /**
                 * 如果已经设置了partition占位符，就需要根据占位符挑出合适的组，因为后面会对组进行合法性检测,
                 * 这里直接将所有的物理group加到选择的列表中，只要选择的group能包含所有的推演出的group就代表OK
                 */
                selGroupList.addAll(RuleUtils.groupToStringList(dbList));
            }

            /* 处理分库，分库dbNamePattern一定不为null */
            /* 但可以不是分库 */
            IPartitionGen partitionGen = TableRuleGenFactory.getInstance()
                .createDBPartitionGenerator(context.getSchemaName(), dbpartitionBy == null ? PartitionByType.HASH :
                    dbpartitionBy.getType());

            /**
             * <pre>
             * 分库分表键相同，并且方法也相同，使用全局唯一的分表表名
             *
             * 如果分库分表键不同，包括分表用时间都使用单独的分库和分表描述方式，分表在每个分库中唯一，分表表名跨库重复
             * </pre>
             */
            PartitionByType tbPartitionType;
            List<String> tbPartitionColExpr;
            if (tbpartitionBy == null) {

                /**
                 * DDL中没有指定分表键(即tbPartition)，则是分库不分表，
                 *
                 * <pre>
                 * 这时默认分库分表键相同, 使用useStandAlone=false模式;
                 * ， 因为如果这种情况下，默认分库分表键不相同，那么库表就会独立枚举，这里每个库的表名就会变成
                 *
                 *  xxx_tbl_0, 而不是xxx_tbl.
                 * </pre>
                 */
                tbPartitionType = dbpartitionBy.getType();
                tbPartitionColExpr = dbpartitionBy.getColExpr();
            } else {
                tbPartitionType = tbpartitionBy.getType();
                tbPartitionColExpr = tbpartitionBy.getColExpr();
            }

            ISubpartitionGen subpartitionGen = TableRuleGenFactory.getInstance()
                .createTBPartitionGenerator(context.getSchemaName(), dbpartitionBy == null ? PartitionByType.HASH :
                        dbpartitionBy.getType(),
                    tbPartitionType);

            /**
             * 默认使用全局唯一的方式生成规则
             */
            boolean useStandAlone = false;
            List<String> dbPartitionParams = new ArrayList<String>();
            List<String> tbPartitionParams = new ArrayList<String>();
            if (dbpartitionBy != null) {
                dbPartitionParams = dbpartitionBy.getColExpr();
            }
            if (tbpartitionBy != null) {
                tbPartitionParams = tbpartitionBy.getColExpr();
            }

            if (!comparePartitionParamList(dbPartitionParams, tbPartitionParams)) {
                /* 分库分表键不同 */
                useStandAlone = true;
            } else if (dbPartitionParams.isEmpty() || tbPartitionParams.isEmpty()) {
                useStandAlone = true;
            } else {

                /* 分库分表键相同 */
                if (dbpartitionBy.getType() != tbPartitionType) {
                    /* 分表键相同，但方式不同，也需要借用分库分表键不同的形式生成唯一的分表名 */
                    useStandAlone = true;
                }
                /* 或者其中有空字符串时，直接用原始逻辑表名当物理表名 */
            }

            // if ((!dbStr.isEmpty() && !tbStr.isEmpty()) &&
            // (!dbStr.equalsIgnoreCase(tbStr))) {
            // /* 分库分表键不同 */
            // useStandAlone = true;
            //
            // } else if (dbStr.isEmpty() || tbStr.isEmpty()) {
            // useStandAlone = true;
            // } else {
            // /* 分库分表键相同 */
            // if (dbpartitionBy.getType() != tbPartitionType) {
            // /* 分表键相同，但方式不同，也需要借用分库分表键不同的形式生成唯一的分表名 */
            // useStandAlone = true;
            // }
            // /* 或者其中有空字符串时，直接用原始逻辑表名当物理表名 */
            // }

            // LiteralString dbShardKey = null;
            // if (dbPartitionParams.size() > 1) {
            // dbShardKey = new LiteralString(null, dbPartitionParams.get(0),
            // false);
            // }
            // if (dbpartitionDefinition != null) {
            // dbpartitionDefinition.setPartitionParamList(dbPartitionParams);
            //
            // }
            //
            // LiteralString tbShardKey = null;
            // if (tbPartitionParams.size() > 1) {
            // tbShardKey = new LiteralString(null, tbPartitionParams.get(0),
            // false);
            // }
            // if (tbpartitionDefinition != null) {
            // tbpartitionDefinition.setPartitionParamList(tbPartitionParams);
            // }

            TableRuleUtil
                .populateExistingRandomSuffix(tableName, tableRule, OptimizerContext.getContext(schemaName), true);
            if (useStandAlone) {
                /**
                 * 使用独立方式生成分库分表规则
                 */
                // partitionGen.fillDbRuleStandAlone(tableRule,
                // dbpartitionBy.getColExpr(), // name
                // tableMeta,
                // group_count, // 分库数
                // dbpartitionDefinition /* dbNamePattern */);

                partitionGen.fillDbRuleStandAlone(tableRule,
                    dbPartitionParams,
                    tableMeta,
                    group_count,
                    table_count_on_each_group,
                    dbpartitionDefinition);

                // subpartitionGen.fillTbRuleStandAlone(tableRule,
                // tbPartitionColExpr,
                // tableMeta,
                // table_count_on_each_group,
                // tableName,
                // tbpartitionDefinition);

                subpartitionGen.fillTbRuleStandAlone(tableRule,
                    tbPartitionParams,
                    tableMeta,
                    group_count,
                    table_count_on_each_group,
                    tableName,
                    tbpartitionDefinition);
            } else {
                /**
                 * 使用全局唯一方式生成分库分表规则
                 */

                partitionGen.fillDbRule(tableRule, dbPartitionParams, // name
                    tableMeta,
                    group_count, // 分库数
                    table_count_on_each_group, // 每库分表数
                    dbpartitionDefinition /* dbNamePattern */);

                subpartitionGen.fillTbRule(tableRule, tbPartitionParams, // name
                    tableMeta,
                    group_count, // 分库数
                    table_count_on_each_group, // 每个库的物理表数
                    tableName,
                    tbpartitionDefinition /* tbNamePattern */);
            }

            if (dbpartitionBy != null && tbpartitionBy != null && dbpartitionBy.getType() != null
                && tbpartitionBy.getType() != null) {
                if (dbpartitionBy.getType().canCoverRule() && tbpartitionBy.getType().canCoverRule()) {
                    tableRule.setCoverRule(true);
                }
            }

            /* 总是加上allowfulltablescan */
            tableRule.setAllowFullTableScan(true);

            /**
             * 这里可以直接做规则推导,因为来源是从SQL来的不存在 并发竞争的问题.
             */
            tableRule.init();

            ShardFuncParamsChecker.validateShardFuncionForTableRule(tableRule);

            // ShardFuncParamsChecker.validateShardFuncionForTableRule(tableRule);

            /**
             * 这里特别的有一种情况，因为前面是通过真实的group列表来生成tableRule的
             * 但这里是通过这个抽象的tableRule来枚举出计算出的group列表的，这就要求
             * 真实的group必须是连续的，否则如果真实的是0,1,3,4，我这里计算的结果则会是
             * 0,1,2,3就会出错，而且这种错误是在执行的时候才会发现的，而且即使我在这里可以将
             * 真实的groupList带过来，但是之行的时候还是会shard到不存在的group中，这样还不如在
             * 建库的时候直接报错比较好，所以此处需要做预先校验工作
             */
            Map<String, Set<String>> topology = tableRule.getActualTopology();

            /* 只要选择的group能包含所有的推演出的group就代表OK */
            if (!RuleUtils.checkIfGroupMatch(new HashSet<String>(selGroupList), topology.keySet())) {
                throw new IllegalArgumentException("Selected physical group list is invalid:" + selGroupList);
            }

            /**
             * 因为前面的各种方式目的就是保证所有分表编号全局唯一，所以 这里进行最后的检查
             */
            String dbRule = (tableRule.getDbRuleStrs() == null || tableRule.getDbRuleStrs().length == 0) ? null :
                tableRule.getDbRuleStrs()[0];
            String tbRule = (tableRule.getTbRulesStrs() == null || tableRule.getTbRulesStrs().length == 0) ? null :
                tableRule.getTbRulesStrs()[0];

            /**
             * 检查最终生成的分表数与期望分表数是否相同
             */
            if (!RuleUtils.checkIfTableNumberOk(topology, group_count, table_count_on_each_group) && needCheckTable) {
                throw new IllegalArgumentException("Generated table number mismatch" + " dbName: "
                    + tableRule.getDbNamePattern() + " tbName: "
                    + tableRule.getTbNamePattern() + " dbRule: " + dbRule + " tbRule: "
                    + tbRule);
            }

            return tableRule;
        }

        private static boolean comparePartitionParamList(List<String> paramList1, List<String> paramList2) {

            if (paramList1 == null && paramList2 != null) {
                return false;
            }

            if (paramList1 != null && paramList2 == null) {
                return false;
            }

            if (paramList1 == null && paramList2 == null) {
                return true;
            }

            if (paramList1.size() != paramList2.size()) {
                return false;
            }

            int paramCount = paramList1.size();
            for (int i = 0; i < paramCount; i++) {
                if (!paramList1.get(i).equals(paramList2.get(i))) {
                    return false;
                }
            }

            return true;

        }
    }
}

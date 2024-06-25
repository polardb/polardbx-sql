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

package com.alibaba.polardbx.planner.common;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateJavaFunctionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionRecord;
import com.alibaba.polardbx.optimizer.config.table.statistic.MockStatisticDatasource;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.IndexRecord;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableRecord;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.SimpleSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ToDrdsRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.sharding.DataNodeChooser;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.GsiUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.TableRuleUtil;
import com.alibaba.polardbx.optimizer.variable.MockVariableManager;
import com.alibaba.polardbx.optimizer.view.MockViewManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.VirtualTableRoot;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlTableOptions;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean.mergeIndexRecords;
import static com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean.mergeTableRecords;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.convertTargetDB;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.fillGroup;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.filterGroup;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.getGroupIntersection;
import static org.junit.Assert.assertEquals;

/**
 * @author chenghui.lch 2018年1月3日 下午5:01:30
 * @since 5.0.0
 */
public abstract class BasePlannerTest {

    private Map<String, String> ddlMaps = new HashMap<>();

    private Map<String, String> javaUdfMaps = new HashMap<>();

    private Set<String> ddlFlag = Sets.newHashSet();

    // a map maps unit test name to it's sql, ddl statistics and config
    private static Map<Class, Map<String, Object>> totalMap = new HashMap<>();

    private Map<String, Object> statisticMaps = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    protected Map<String, Object> configMaps = new HashMap<>();

    protected String caseName;

    protected String expectedPlan;

    protected String sql;

    protected int sqlIndex;

    protected String lineNum;

    private String expect;

    private String actual;

    protected String appName = "optest";
    public static final long ROW_COUNT = 100;
    private static final boolean fixFlag = false;

    private Map<String, OptimizerContext> appNameOptiContextMaps = new HashMap<String, OptimizerContext>();
    private String nodetree;

    protected boolean enableParallelQuery = false;
    protected boolean enablePlanManagementTest = false;
    protected boolean enableJoinClustering = true;
    protected int partialAggBucketThreshold = -1;
    protected boolean enableMpp = false;
    protected boolean storageSupportsBloomFilter = false;
    protected boolean storageUsingXxHashInBloomFilter = false;
    protected boolean forceWorkloadTypeAP = false;
    protected int inValuesThread = -1;
    protected SqlType sqlType;
    protected boolean addForcePrimary = false;

    private FastsqlParser parser = new FastsqlParser();
    protected RelOptCluster cluster;
    protected boolean ignoreBaseTest = false;
    protected ExecutionContext ec = new ExecutionContext();
    protected boolean useNewPartDb = false;
    private int corMaxNum = 10;
    private String targetEnvFile = this.getClass().getSimpleName();

    public BasePlannerTest(String dbname) {
        appName = dbname;
        try {
            loadStatistic();
        } catch (Exception e) {
            e.printStackTrace();
        }
        initBasePlannerTestEnv();
        initAppNameConfig(dbname);
        ignoreBaseTest = true;
    }

    /**
     * for common test
     */
    public BasePlannerTest(String caseName, String targetEnvFile) {
        this.caseName = caseName;
        this.targetEnvFile = targetEnvFile;
        initTestEnv();
    }

    public BasePlannerTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        this.caseName = caseName;
        this.sqlIndex = sqlIndex;
        this.sql = sql;
        this.expectedPlan = expectedPlan;
        this.lineNum = lineNum;
        initTestEnv();
    }

    public BasePlannerTest(String caseName, String targetEnvFile, int sqlIndex, String sql, String expectedPlan,
                           String lineNum) {
        this.caseName = caseName;
        this.targetEnvFile = targetEnvFile;
        this.sqlIndex = sqlIndex;
        this.sql = sql;
        this.expectedPlan = expectedPlan;
        this.lineNum = lineNum;
        initTestEnv();
    }

    // TODO: `struct` should be removed
    public BasePlannerTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum,
                           String expect, String nodetree, String struct) {
        this.caseName = caseName;
        this.sqlIndex = sqlIndex;
        this.sql = sql;
        this.expectedPlan = expectedPlan;
        this.expect = expect;
        this.lineNum = lineNum;
        this.nodetree = nodetree;
        initTestEnv();
    }

    public String getAppName() {
        return appName;
    }

    public void initTestEnv() {
        loadConfig();
        initBasePlannerTestEnv();
        initExecutionContext();

        loadDdl();
        loadJavaUdf();
        try {
            loadStatistic();
        } catch (Exception e) {
            e.printStackTrace();
        }
        initAppNameConfig(getAppName());
        initAppNameConfig("information_schema");
        prepareSchemaByDdl();
        prepareJavaUdf();
    }

    protected void initBasePlannerTestEnv() {
    }

    protected static int caseNum = 0;

    public static List<Object[]> loadSqls(Class clazz) {

        URL url = clazz.getResource(clazz.getSimpleName() + ".class");
        File dir = new File(url.getPath().substring(0, url.getPath().indexOf(clazz.getSimpleName() + ".class")));
        File[] filesList = dir.listFiles();

        String targetTestcase = System.getProperty("case");
        if (targetTestcase != null) {
            targetTestcase = targetTestcase.trim();
        }

        List<Object[]> cases = new ArrayList<>();
        for (File file : filesList) {
            if (file.isFile()) {
                if (file.getName().startsWith(clazz.getSimpleName() + ".") && file.getName().endsWith(".yml")) {
                    if (file.getName().equals(clazz.getSimpleName() + ".ddl.yml")
                        || file.getName().equals(clazz.getSimpleName() + ".udf.yml")
                        || file.getName().equals(clazz.getSimpleName() + ".outline.yml")
                        || file.getName().equals(clazz.getSimpleName() + ".statistic.yml")
                        || file.getName().equals(clazz.getSimpleName() + ".config.yml")) {
                        continue;
                    }

                    boolean isNewFlag = false;
                    List<Map<String, String>> sqls = loadSqls(file.getName(), clazz);
                    int sqlIndex = 0;

                    try {
                        clazz.getDeclaredConstructor(String.class, int.class, String.class, String.class, String.class);
                    } catch (NoSuchMethodException e) {
                        isNewFlag = true;
                    }

                    for (Map<String, String> sql : sqls) {
                        String plan = sql.get("error");
                        if (plan == null) {
                            plan = sql.get("plan");
                        }

                        String tmpCaseName = String.format("%s#%s", file.getName(), sqlIndex);
                        if (targetTestcase != null && !targetTestcase.isEmpty()) {
                            if (targetTestcase.equals(tmpCaseName)) {
                                if (!isNewFlag) {
                                    cases.add(new Object[] {
                                        file.getName(), sqlIndex, sql.get("sql"), sql.get("plan"),
                                        sql.get("lineNum")});
                                } else {
                                    cases.add(new Object[] {
                                        file.getName(), sqlIndex, sql.get("sql"), sql.get("plan"),
                                        sql.get("lineNum"), sql.get("expect") == null ? "" : sql.get("expect"),
                                        sql.get("nodetree") == null ? "" : sql.get("nodetree"),
                                        sql.get("struct") == null ? "" : sql.get("struct")});
                                }

                            }
                        } else {
                            if (!isNewFlag) {
                                cases.add(new Object[] {
                                    file.getName(), sqlIndex, sql.get("sql"), sql.get("plan"),
                                    sql.get("lineNum")});
                            } else {
                                cases.add(new Object[] {
                                    file.getName(), sqlIndex, sql.get("sql"), sql.get("plan"),
                                    sql.get("lineNum"), sql.get("expect") == null ? "" : sql.get("expect"),
                                    sql.get("nodetree") == null ? "" : sql.get("nodetree"),
                                    sql.get("struct") == null ? "" : sql.get("struct")});
                            }
                        }

                        sqlIndex++;
                    }
                }
            }
        }
        caseNum += cases.size();
        return cases;
    }

    public OptimizerContext getContextByAppName(String appName) {
        return appNameOptiContextMaps.get(appName);
    }

    protected void initExecutionContext() {
    }

    public synchronized void initAppNameConfig(String appName) {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        OptimizerContext context = getContextByAppName(appName);
        if (context != null) {
            OptimizerContext.setContext(context);
            return;
        }
        String key = appName.equals(getAppName()) ? "defaltxxAPPName.isNew" : appName + ".isNew";
        if (configMaps.containsKey(key)) {
            useNewPartDb = (boolean) configMaps.get(key);
            //configMaps.remove(key);
        }
        key = appName.equals(getAppName()) ? "defaltxxAPPName.dbNumber" : appName + ".dbNumber";
        int dbNumber = 4;
        if (configMaps.containsKey(key)) {
            dbNumber = ((Number) configMaps.get(key)).intValue();
            //configMaps.remove(key);
        }
        context = initOptiContext(appName, dbNumber, useNewPartDb);
        appNameOptiContextMaps.put(appName, context);
        LocalityManager.setMockMode(true);
        OptimizerHelper.clear();
        OptimizerHelper.init(new IServerConfigManager() {
            @Override
            public Object getAndInitDataSourceByDbName(String dbName) {
                if (appNameOptiContextMaps.containsKey(dbName)) {
                    return new Object();
                }
                return null;
            }

            @Override
            public com.alibaba.polardbx.common.utils.Pair<String, String> findGroupByUniqueId(
                long uniqueId, Map<String, List<String>> schemaAndGroupsCache) {
                return null;
            }

            @Override
            public DdlContext restoreDDL(String schemaName, Long jobId) {
                return null;
            }

            @Override
            public void remoteExecuteDdlTask(String schemaName, Long jobId, Long taskId) {

            }

            @Override
            public long submitRebalanceDDL(String schemaName, String sql) {
                return 0;
            }

            @Override
            public long submitSubDDL(String schemaName, DdlContext parentDdlContext, long parentJobId,
                                     long parentTaskId, boolean forRollback,
                                     String sql, ParamManager paramManager) {
                return 0;
            }

        });
    }

    public static OptimizerContext initOptiContext(String appName, int dbNumber, boolean useNewPartDb) {
        OptimizerContext context = new OptimizerContext(appName);
        PartitionInfoManager partInfoMgr = new PartitionInfoManager(appName, appName, true);
        TableGroupInfoManager tableGroupInfoManager = new TableGroupInfoManager(appName);

        TddlRule tddlRule = new TddlRule();
        tddlRule.setAppName(appName);
        tddlRule.setAllowEmptyRule(true);
        tddlRule.setDefaultDbIndex(appName + "_0000");
        TddlRuleManager rule = new TddlRuleManager(tddlRule, partInfoMgr, tableGroupInfoManager, appName);

        List<Group> groups = new LinkedList<>();
        for (int i = 0; i < dbNumber; i++) {
            groups.add(fakeGroup(appName, appName + String.format("_%04d", i)));
        }

        Matrix matrix = new Matrix();
        matrix.setGroups(groups);

        SimpleSchemaManager sm = new SimpleSchemaManager(appName, rule);

        MockVariableManager mockVariableManager = new MockVariableManager(appName);
        mockVariableManager.init();
        context.setVariableManager(mockVariableManager);

        context.setViewManager(MockViewManager.getInstance());

        context.setMatrix(matrix);
        context.setRuleManager(rule);
        context.setSchemaManager(sm);
        context.setPartitionInfoManager(partInfoMgr);
        context.setTableGroupInfoManager(tableGroupInfoManager);
        context.setPartitioner(new Partitioner(tddlRule, context));

        OptimizerContext.loadContext(context);

        if (useNewPartDb) {
            DbInfoManager.getInstance().addNewMockPartitionDb(appName);
        } else {
            DbInfoManager.getInstance().removeMockPartitionDb(appName);
        }

        return context;
    }

    private static Group fakeGroup(String appname, String name) {
        Group g = new Group();
        g.setAppName(appname);
        g.setSchemaName(appname);
        g.setName(name);
        return g;
    }

    public void buildTable(String appName, String tableDDL) throws SQLSyntaxErrorException {
        buildTable(appName, tableDDL, ROW_COUNT);
    }

    public static GsiMetaBean initTableMeta(final List<IndexRecord> allIndexRecords,
                                            final List<TableRecord> allTableRecords) {
        final GsiMetaBean result = new GsiMetaBean();

        final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap =
            Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        final Builder<String, String> indexTableRelationBuilder = mergeIndexRecords(allIndexRecords, tmpTableIndexMap);
        final Builder<String, GsiTableMetaBean> tableMetaBuilder = mergeTableRecords(allTableRecords, tmpTableIndexMap);

        result.setIndexTableRelation(indexTableRelationBuilder.build());
        result.setTableMeta(tableMetaBuilder.build());

        return result;
    }

    private TableRule buildGsiTargetTable(TableMeta tableToSchema, String indexTableName, SqlIndexDefinition indexDef,
                                          Map<String, Map<String, List<List<String>>>> gsiTargetTables,
                                          Map<String, TableRule> gsiTableRules,
                                          Map<String, SqlIndexDefinition> gsiIndexDefs) {
        ExecutionContext ec = new ExecutionContext();
        ec.getExtraCmds().put(ConnectionProperties.ENABLE_RANDOM_PHY_TABLE_NAME, false);
        TableRule tableRule = TableRuleUtil.buildShardingTableRule(indexTableName,
            tableToSchema,
            indexDef.getDbPartitionBy(),
            indexDef.getDbPartitions(),
            indexDef.getTbPartitionBy(),
            indexDef.getTbPartitions(),
            OptimizerContext.getContext(appName), ec);

        List<List<TargetDB>> targetDBs = DataNodeChooser.shardCreateTable(this.appName,
            indexTableName,
            null,
            tableRule);
        final Set<String> groupIntersection = getGroupIntersection(targetDBs);
        targetDBs = filterGroup(targetDBs, groupIntersection, this.appName);
        final List<Group> groups = OptimizerContext.getContext(appName).getMatrix().getGroups();
        targetDBs = fillGroup(targetDBs, groups, tableRule);

        gsiTargetTables.put(indexTableName, convertTargetDB(targetDBs));
        gsiTableRules.put(indexTableName, tableRule);
        gsiIndexDefs.put(indexTableName, indexDef);

        return tableRule;
    }

    public void buildTable(String appName, String tableDDL, long defaultRowCount) {
        ec.setParams(new Parameters());
        ec.setSchemaName(appName);
        ec.setServerVariables(new HashMap<>());
        final MySqlCreateTableStatement stat = (MySqlCreateTableStatement) FastsqlUtils.parseSql(tableDDL).get(0);
        final TableMeta tm = new TableMetaParser().parse(stat, ec);
        tm.setSchemaName(appName);

        final SqlCreateTable sqlCreateTable = (SqlCreateTable) FastsqlParser.convertStatementToSqlNode(stat, null, ec);
        final String logicalTableName = RelUtils.stringValue(sqlCreateTable.getName());
        TableRule tr = null;
        LogicalCreateTable logicalCreateTable = null;

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);

        if (sqlCreateTable.getSqlPartition() != null) {
            logicalCreateTable =
                buildLogicalCreateTable(appName, tm, sqlCreateTable, logicalTableName,
                    PartitionTableType.PARTITION_TABLE, plannerContext);
        } else {
            if (useNewPartDb) {
                if (sqlCreateTable.isBroadCast()) {
                    logicalCreateTable =
                        buildLogicalCreateTable(appName, tm, sqlCreateTable, logicalTableName,
                            PartitionTableType.BROADCAST_TABLE, plannerContext);
                } else {
                    logicalCreateTable =
                        buildLogicalCreateTable(appName, tm, sqlCreateTable, logicalTableName,
                            PartitionTableType.SINGLE_TABLE, plannerContext);
                }
            } else {
                // init table rule
                tr = buildTableRule(appName, tm, sqlCreateTable, logicalTableName);
                logicalCreateTable =
                    buildLogicalCreateTable(appName, tm, sqlCreateTable, logicalTableName, plannerContext);
            }
        }

        final boolean useSequence = checkUseSequence(sqlCreateTable, tr);

        storeTable(appName, tr, tm, useSequence);

        // init gsi meta
        if (sqlCreateTable.createGsi()) {
            final String mainTableDefinition = sqlCreateTable.rewriteForGsi().toString();
            final MySqlCreateTableStatement astCreateIndexTable =
                (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(mainTableDefinition,
                        JdbcConstants.MYSQL)
                    .get(0);

            final List<IndexRecord> allIndexRecords = new ArrayList<>();
            final List<TableRecord> allTableRecords = new ArrayList<>();

            // global secondary index
            storeGsi(appName,
                sqlCreateTable,
                logicalTableName,
                tm,
                tr,
                OptimizerContext.getContext(appName).getPartitionInfoManager().getPartitionInfo(logicalTableName),
                logicalCreateTable,
                mainTableDefinition,
                astCreateIndexTable,
                allIndexRecords,
                allTableRecords,
                false, ec);

            // global unique secondary index
            storeGsi(appName,
                sqlCreateTable,
                logicalTableName,
                tm,
                tr,
                OptimizerContext.getContext(appName).getPartitionInfoManager().getPartitionInfo(logicalTableName),
                logicalCreateTable,
                mainTableDefinition,
                astCreateIndexTable,
                allIndexRecords,
                allTableRecords,
                true, ec);

            // store gsi meta for primary table
            final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap =
                Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            final ImmutableMap<String, String> indexTableRelation =
                mergeIndexRecords(allIndexRecords, tmpTableIndexMap).build();
            final ImmutableMap<String, GsiTableMetaBean> tableMetaBean = mergeTableRecords(allTableRecords,
                tmpTableIndexMap).build();
            tm.setGsiTableMetaBean(tableMetaBean.get(logicalTableName));
        }

        if (!sqlCreateTable.getAddedForeignKeys().isEmpty()) {
            final Map<String, ForeignKeyData> foreignKeys = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            final Map<String, ForeignKeyData> referencedForeignKeys = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            TableMeta refTm;

            for (ForeignKeyData foreignKey : sqlCreateTable.getAddedForeignKeys()) {
                foreignKeys.put(foreignKey.refTableName, foreignKey);

                refTm = OptimizerContext.getContext(appName).getLatestSchemaManager().getTable(foreignKey.refTableName);
                referencedForeignKeys.put(appName + "/" + foreignKey.refTableName + "/" + foreignKey.indexName,
                    foreignKey);
                refTm.getReferencedForeignKeys().putAll(referencedForeignKeys);
                storeTable(appName, tr, tm, useSequence);
            }

            tm.getForeignKeys().putAll(foreignKeys);
            storeTable(appName, tr, tm, useSequence);
        }

        StatisticManager statisticManager = appNameOptiContextMaps.get(appName).getStatisticManager();
        // for Forwards Compatibility where we have 'table.sampleRate=xxx'
        // sampleRate
        if (statisticMaps.get(logicalTableName + ".sampleRate") != null) {
            Number sampleRate = (Number) statisticMaps.get(logicalTableName + ".sampleRate");
            if (sampleRate != null) {
                statisticManager.getCacheLine(appName, logicalTableName)
                    .setSampleRate(sampleRate.floatValue());
            }
        }
    }

    protected LogicalCreateTable buildLogicalCreateTable(String appName, TableMeta tm,
                                                         SqlCreateTable sqlCreateTable,
                                                         String logicalTableName,
                                                         PartitionTableType tblType,
                                                         PlannerContext plannerContext) {
        LogicalCreateTable logicalCreateTable;
        SqlConverter converter = SqlConverter.getInstance(appName, ec);
        SqlNode validatedNode = converter.validate(sqlCreateTable);
        // sqlNode to relNode
        RelNode relNode = converter.toRel(validatedNode, plannerContext);

        // relNode to drdsRelNode
        ToDrdsRelVisitor toDrdsRelVisitor = new ToDrdsRelVisitor(validatedNode, plannerContext);
        RelNode drdsRelNode = relNode.accept(toDrdsRelVisitor);
        logicalCreateTable = (LogicalCreateTable) drdsRelNode;
        logicalCreateTable.prepareData(new ExecutionContext());

        PartitionInfo partitionInfo =
            buildPartitionInfoByLogCreateTbl(appName, logicalCreateTable, plannerContext.getExecutionContext());
        tm.setPartitionInfo(partitionInfo);

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(appName).getTableGroupInfoManager();
        tableGroupInfoManager.putMockEntry(partitionInfo);

        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(appName).getPartitionInfoManager();
        partitionInfoManager.putPartInfoCtx(logicalTableName.toLowerCase(),
            new PartitionInfoManager.PartInfoCtx(partitionInfoManager, logicalTableName.toLowerCase(),
                partitionInfo.getTableGroupId(),
                partitionInfo));
        return logicalCreateTable;
    }

    protected LogicalCreateTable buildLogicalCreateTable(String appName, TableMeta tm,
                                                         SqlCreateTable sqlCreateTable,
                                                         String logicalTableName,
                                                         PlannerContext plannerContext) {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        LogicalCreateTable logicalCreateTable;
        SqlConverter converter = SqlConverter.getInstance(appName, ec);
        SqlNode validatedNode = converter.validate(sqlCreateTable);
        // sqlNode to relNode
        RelNode relNode = converter.toRel(validatedNode, plannerContext);

        // relNode to drdsRelNode
        ToDrdsRelVisitor toDrdsRelVisitor = new ToDrdsRelVisitor(validatedNode, plannerContext);
        RelNode drdsRelNode = relNode.accept(toDrdsRelVisitor);
        logicalCreateTable = (LogicalCreateTable) drdsRelNode;
        logicalCreateTable.prepareData(new ExecutionContext());

        return logicalCreateTable;
    }

    public final boolean checkUseSequence(SqlCreateTable sqlCreateTable, TableRule tr) {
        final Set<String> pkSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Optional.ofNullable(sqlCreateTable.getPrimaryKey()).map(SqlIndexDefinition::getColumns)
            .orElse(ImmutableList.of()).stream().map(cn -> Util.last(cn.getColumnName().names))
            .forEach(pkSet::add);

        final boolean isPartitioned = tr != null && (GeneralUtil.isNotEmpty(tr.getTbShardRules()) || GeneralUtil
            .isNotEmpty(tr.getDbShardRules()));
        final boolean withAutoIncrement = sqlCreateTable.getColDefs().stream()
            .anyMatch(p -> pkSet.contains(Util.last(p.left.names)) && p.right.isAutoIncrement());
        return isPartitioned && withAutoIncrement;
    }

    public void storeGsi(String schema, SqlCreateTable sqlCreateTable, String logicalTableName, TableMeta tm,
                         TableRule tr, PartitionInfo partitionInfo, LogicalCreateTable logicalCreateTable,
                         String mainTableDefinition, MySqlCreateTableStatement astCreateIndexTable,
                         List<IndexRecord> allIndexRecords, List<TableRecord> allTableRecords, boolean gusi,
                         ExecutionContext ec) {
        final Map<String, TableRule> gsiTableRules = new HashMap<>();
        final Map<String, SqlIndexDefinition> gsiIndexDefs = new HashMap<>();
        final Map<String, Map<String, List<List<String>>>> gsiTargetTables = new HashMap<>();

        final List<Pair<SqlIdentifier, SqlIndexDefinition>> gsiList = new ArrayList<>();
        if (gusi) {
            if (sqlCreateTable.getGlobalUniqueKeys() != null) {
                gsiList.addAll(sqlCreateTable.getGlobalUniqueKeys());
            }
            if (sqlCreateTable.getClusteredUniqueKeys() != null) {
                gsiList.addAll(sqlCreateTable.getClusteredUniqueKeys());
            }
        } else {
            if (sqlCreateTable.getGlobalKeys() != null) {
                gsiList.addAll(sqlCreateTable.getGlobalKeys());
            }
            if (sqlCreateTable.getClusteredKeys() != null) {
                gsiList.addAll(sqlCreateTable.getClusteredKeys());
            }
        }
        buildIndexTargetTable(tm, gsiTargetTables, gsiTableRules, gsiIndexDefs, gsiList);

        for (Entry<String, Map<String, List<List<String>>>> entry : gsiTargetTables.entrySet()) {
            final String indexTableName = entry.getKey();
            final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
            final Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = new HashMap<>();
            final SqlTableOptions tableOptions = null;
            final List<SqlAlterSpecification> alters = new ArrayList<>();
            final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);
            final SqlIndexDefinition indexDef = gsiIndexDefs.get(indexTableName);
            final SqlIdentifier tableName = new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);

            alters.add(gusi ? new SqlAddUniqueIndex(SqlParserPos.ZERO, indexName, indexDef) :
                new SqlAddIndex(SqlParserPos.ZERO, indexName, indexDef));
            indexDef.setPrimaryTableDefinition(mainTableDefinition);

            final SqlAlterTable addIndex = new SqlAlterTable(null,
                tableName,
                columnOpts,
                "",
                tableOptions,
                alters,
                SqlParserPos.ZERO);

            final List<SqlIndexColumnName> covering =
                indexDef.getCovering() == null ? new ArrayList<>() : indexDef.getCovering();
            final Map<String, SqlIndexColumnName> coveringMap = Maps.uniqueIndex(covering,
                SqlIndexColumnName::getColumnNameStr);
            final Map<String, SqlIndexColumnName> indexColumnMap = Maps.uniqueIndex(indexDef.getColumns(),
                SqlIndexColumnName::getColumnNameStr);

            List<String> shardColumns = tr != null ? tr.getShardColumns() : partitionInfo.getPartitionColumns();

            TableRule indexTr = null;
            final TableMeta indexTm;

            if (partitionInfo == null) {
                indexTr = gsiTableRules.get(indexTableName);

                CreateGlobalIndexPreparedData preparedData =
                    logicalCreateTable.getCreateTableWithGsiPreparedData().getIndexTablePreparedData(indexTableName);
                preparedData.setIndexTableRule(indexTr);

                CreateGlobalIndexBuilder builder =
                    new CreateGlobalIndexBuilder(logicalCreateTable.relDdl, preparedData, ec);

                final SqlCreateTable sqlCreateIndexTable = (SqlCreateTable) builder
                    .createIndexTable(addIndex, indexColumnMap, coveringMap, astCreateIndexTable.clone(),
                        new HashSet<>(shardColumns), logicalCreateTable.relDdl, schema, ec);

                final MySqlCreateTableStatement indexStat =
                    (MySqlCreateTableStatement) FastsqlUtils.parseSql(sqlCreateIndexTable.getSourceSql()).get(0);
                indexTm = new TableMetaParser().parse(indexStat, ec);
                indexTm.setHasPrimaryKey(indexTm.isHasPrimaryKey());
                indexTm.setSchemaName(schema);
            } else {

                CreateGlobalIndexPreparedData createGlobalIndexPreparedData =
                    logicalCreateTable.getCreateTableWithGsiPreparedData()
                        .getIndexTablePreparedData(entry.getKey());
                TableMeta primaryTbMeta = logicalCreateTable.getCreateTablePreparedData().getTableMeta();
                List<ColumnMeta> allColMetas = primaryTbMeta.getAllColumns();
                List<ColumnMeta> pkColMetas = new ArrayList<>(primaryTbMeta.getPrimaryKey());
                primaryTbMeta.setSchemaName(schema);
                PartitionInfo indexPartitionInfo = PartitionInfoBuilder
                    .buildPartitionInfoByPartDefAst(schema, indexTableName, null, false, null,
                        (SqlPartitionBy) createGlobalIndexPreparedData.getIndexDefinition().getPartitioning(),
                        createGlobalIndexPreparedData.getPartBoundExprInfo(),
                        pkColMetas, allColMetas, PartitionTableType.GSI_TABLE,
                        ec);

                createGlobalIndexPreparedData.setPrimaryPartitionInfo(partitionInfo);
                createGlobalIndexPreparedData.setIndexPartitionInfo(indexPartitionInfo);
                CreatePartitionGlobalIndexBuilder builder =
                    new CreatePartitionGlobalIndexBuilder(logicalCreateTable.relDdl,
                        logicalCreateTable.getCreateTableWithGsiPreparedData()
                            .getIndexTablePreparedData(entry.getKey()), ec);
                builder.buildSqlTemplate();

                final MySqlCreateTableStatement indexStat =
                    (MySqlCreateTableStatement) FastsqlUtils
                        .parseSql(((SqlCreateTable) builder.sqlTemplate).getSourceSql())
                        .get(0);
                indexTm = new TableMetaParser().parse(indexStat, ec);
                indexTm.setHasPrimaryKey(true);
                indexTm.setSchemaName(schema);
            }

            final List<IndexRecord> indexRecords = new ArrayList<>();
            final List<TableRecord> tableRecords = new ArrayList<>();

            // FIXME for partition table tableRecords
            GsiUtils.buildIndexMeta(indexRecords,
                tableRecords,
                addIndex,
                indexTr,
                schema,
                sqlCreateTable,
                IndexStatus.PUBLIC, true);

            allIndexRecords.addAll(indexRecords);
            allTableRecords.addAll(tableRecords);

            final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap =
                Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            final ImmutableMap<String, String> indexTableRelation =
                mergeIndexRecords(indexRecords, tmpTableIndexMap).build();
            final ImmutableMap<String, GsiTableMetaBean> tableMetaBean = mergeTableRecords(tableRecords,
                tmpTableIndexMap).build();
            indexTm.setGsiTableMetaBean(tableMetaBean.get(indexTableName));

            if (((SqlAddIndex) addIndex.getAlters().get(0)).getIndexDef().getPartitioning() != null) {
                PartitionInfo indexPartitionInfo = logicalCreateTable.getCreateTableWithGsiPreparedData()
                    .getIndexTablePreparedData(indexTableName).getIndexPartitionInfo();

                indexPartitionInfo.setStatus(TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);
                indexTm.setPartitionInfo(indexPartitionInfo);

                TableGroupInfoManager tableGroupInfoManager =
                    OptimizerContext.getContext(schema).getTableGroupInfoManager();
                tableGroupInfoManager.putMockEntry(indexPartitionInfo);

                PartitionInfoManager partitionInfoManager =
                    OptimizerContext.getContext(schema).getPartitionInfoManager();
                partitionInfoManager.putPartInfoCtx(indexTableName.toLowerCase(),
                    new PartitionInfoManager.PartInfoCtx(partitionInfoManager, indexTableName.toLowerCase(),
                        indexPartitionInfo.getTableGroupId(),
                        indexPartitionInfo));
            }

            storeTable(schema, indexTr, indexTm, false);
        }
    }

    public void buildIndexTargetTable(TableMeta tm, Map<String, Map<String, List<List<String>>>> gsiTargetTables,
                                      Map<String, TableRule> gsiTableRules,
                                      Map<String, SqlIndexDefinition> gsiIndexDefs,
                                      List<Pair<SqlIdentifier, SqlIndexDefinition>> gsiList) {
        if (null != gsiList) {
            gsiList.forEach(gsi -> {

                final String indexTableName = RelUtils.lastStringValue(gsi.getKey());
                final SqlIndexDefinition indexDef = gsi.getValue();

                buildGsiTargetTable(tm, indexTableName, indexDef, gsiTargetTables, gsiTableRules, gsiIndexDefs);
            });
        }
    }

    public TableRule buildTableRule(String schema, TableMeta tm, SqlCreateTable sqlCreateTable,
                                    String logicalTableName) {
        TableRule tr;
        ExecutionContext ec = this.ec.copy();
        if (sqlCreateTable.isBroadCast()) {
            tr = TableRuleUtil.buildBroadcastTableRuleWithoutRandomPhyTableName(logicalTableName, tm);
        } else if (sqlCreateTable.getDbpartitionBy() != null || sqlCreateTable.getTbpartitionBy() != null) {
            ec.getExtraCmds().put(ConnectionProperties.ENABLE_RANDOM_PHY_TABLE_NAME, false);
            tr = TableRuleUtil.buildShardingTableRule(logicalTableName,
                tm,
                sqlCreateTable.getDbpartitionBy(),
                sqlCreateTable.getDbpartitions(),
                sqlCreateTable.getTbpartitionBy(),
                sqlCreateTable.getTbpartitions(),
                OptimizerContext.getContext(schema), ec);
        } else {
            tr = new TableRule();
            tr.setDbNamePattern(
                OptimizerContext.getContext(schema).getRuleManager().getTddlRule().getDefaultDbIndex());
            tr.setTbNamePattern(tm.getTableName());
            tr.init();
        }
        return tr;
    }

    private void storeTable(String appName, TableRule tableRule, TableMeta tableMeta, boolean withSequence) {
        OptimizerContext oc = getContextByAppName(appName);
        VirtualTableRoot.setTestRule(tableMeta.getTableName(), tableRule);
        SchemaManager sm = oc.getLatestSchemaManager();
        sm.putTable(tableMeta.getTableName(), tableMeta);
        if (withSequence) {
            SequenceManagerProxy.getInstance().updateValue(appName, tableMeta.getTableName(), 1);
        }
    }

    private boolean isDDLInit() {
        String fileName = String.format("%s.ddl.yml", targetEnvFile);
        return ddlFlag.contains(fileName);
    }

    protected void prepareSchemaByDdl() {
        if (isDDLInit()) {
            return;
        }
        try {
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.setSchemaName(appName);
            this.cluster = SqlConverter.getInstance(appName, executionContext).createRelOptCluster();
            for (Entry<String, String> ddlItem : ddlMaps.entrySet()) {
                String createTbDdl = ddlItem.getValue();
                if (ddlItem.getKey().contains(".")) {
                    String additionalSchema = ddlItem.getKey().split("\\.")[0];
                    initAppNameConfig(additionalSchema);
                    buildTable(additionalSchema, createTbDdl);
                    continue;
                }
                buildTable(getAppName(), createTbDdl);
            }
            // set current schema to default appname
            OptimizerContext context = getContextByAppName(appName);
            if (context != null) {
                OptimizerContext.loadContext(context);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table", e);
        }
        String fileName = String.format("%s.ddl.yml", targetEnvFile);
        ddlFlag.add(fileName);

    }

    private void prepareJavaUdf() {
        try {
            this.cluster = SqlConverter.getInstance(appName, new ExecutionContext()).createRelOptCluster();
            if (javaUdfMaps == null) {
                return;
            }
            for (Entry<String, String> funcItem : javaUdfMaps.entrySet()) {
                String funcName = funcItem.getKey();
                String createFuncDdl = funcItem.getValue();
                // drop first
                JavaFunctionManager.getInstance().dropFunction(funcName);
                // register function
                JavaFunctionRecord record = translateToRecord(createFuncDdl);
                JavaFunctionManager.getInstance().registerFunction(record);
                JavaFunctionManager.getInstance().compileJavaFunction(funcName, record.noState, record);
                // init function
                JavaFunctionManager.getInstance().getJavaFunction(funcName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create java udf", e);
        }
    }

    private JavaFunctionRecord translateToRecord(String createFunc) {
        SQLCreateJavaFunctionStatement statement =
            (SQLCreateJavaFunctionStatement) FastsqlUtils.parseSql(createFunc).get(0);
        JavaFunctionRecord record = new JavaFunctionRecord();
        record.codeLanguage = "JAVA";
        record.code = statement.getJavaCode();
        record.funcName = SQLUtils.normalize(statement.getName().getSimpleName()).toLowerCase();
        record.className = record.funcName.substring(0, 1).toUpperCase() + record.funcName.substring(1).toLowerCase();
        record.inputTypes = statement.getInputTypes().stream().map(Object::toString).collect(Collectors.joining(","));
        record.returnType = statement.getReturnType().toString();
        record.noState = statement.isNoState();
        return record;
    }

    private void loadConfig() {
        if (totalMap.get(this.getClass()) == null) {
            String fileName = String.format("%s.config.yml", this.getClass().getSimpleName());

            InputStream in = this.getClass().getResourceAsStream(fileName);
            if (in == null) {
                return;
            }
            Yaml yaml = new Yaml();
            this.configMaps = yaml.loadAs(in, Map.class);
            IOUtils.closeQuietly(in);
        } else {
            this.configMaps = (Map<String, Object>) totalMap.get(this.getClass()).get("CONFIG");
        }
    }

    @SuppressWarnings("unchecked")
    private void loadDdl() {
        if (totalMap.get(this.getClass()) == null) {
            String fileName = String.format("%s.ddl.yml", targetEnvFile);
            InputStream in = this.getClass().getResourceAsStream(fileName);
            Yaml yaml = new Yaml();
            this.ddlMaps = yaml.loadAs(in, Map.class);
            IOUtils.closeQuietly(in);
        } else {
            this.ddlMaps = (Map<String, String>) totalMap.get(this.getClass()).get("DDL");
        }

    }

    @SuppressWarnings("unchecked")
    private void loadJavaUdf() {
        if (totalMap.get(this.getClass()) == null) {
            String fileName = String.format("%s.udf.yml", targetEnvFile);
            InputStream in = this.getClass().getResourceAsStream(fileName);
            if (in == null) {
                return;
            }
            Yaml yaml = new Yaml();
            this.javaUdfMaps = yaml.loadAs(in, Map.class);
            IOUtils.closeQuietly(in);
        } else {
            this.javaUdfMaps = (Map<String, String>) totalMap.get(this.getClass()).get("UDF");
        }

    }

    protected void loadStatistic() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Map<String, Map<String, Map<String, List<Pair<String, Object>>>>> statisticsClassifier =
            MockStatisticDatasource.statisticsClassifier;
        try {
            if (totalMap.get(this.getClass()) == null) {
                String fileName = String.format("%s.statistic.yml", targetEnvFile);
                InputStream in = this.getClass().getResourceAsStream(fileName);

                Yaml yaml = new Yaml();
                Map<String, Object> m = yaml.loadAs(in, Map.class);
                this.statisticMaps = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                this.statisticMaps.putAll(m);
                IOUtils.closeQuietly(in);
            } else {
                this.statisticMaps = (Map<String, Object>) totalMap.get(this.getClass()).get("STATISTICS");
            }
        } catch (Exception e) {
            // pass
            // TODO: deal with the exception that the file is not in valid format
        }
        //classify statistics
        statisticsClassifier.put(getAppName(), new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
        for (Entry<String, String> ddlItem : ddlMaps.entrySet()) {
            // there are two cases 1:table 2:schema.table
            String[] nameSplit = ddlItem.getKey().split("\\.");
            switch (nameSplit.length) {
            case 1:
                statisticsClassifier.get(getAppName()).put(nameSplit[0],
                    new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
                break;
            case 2:
                if (!statisticsClassifier.containsKey(nameSplit[0])) {
                    statisticsClassifier.put(nameSplit[0], new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
                }
                statisticsClassifier.get(nameSplit[0]).put(nameSplit[1],
                    new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
                break;
            default:
                throw new Exception("unexpected ddl key: " + ddlItem.getKey());
            }
        }
        //record reserved keyword
        Set<String> keywords = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        keywords.add("cardinality");
        keywords.add("TOPN");
        keywords.add("histogram");
        keywords.add("null_count");
        keywords.add("sample_rate");
        keywords.add("composite_cardinality");
        keywords.add("extend_field");
        for (Map.Entry<String, Object> entry : statisticMaps.entrySet()) {
            // it is assumed four cases in the name, we classify the name according to the number of '.'
            // 1:table 2:schema.table 3:table.columns.xxx 4:schema.table.columns.xxx
            String name = entry.getKey();
            String[] nameSplit = name.split("\\.");
            // for Forwards Compatibility where we have 'table.column.nullCount=xxx'
            if (nameSplit[nameSplit.length - 1].equalsIgnoreCase("nullCount")) {
                nameSplit[nameSplit.length - 1] = "null_count";
            }
            List<Pair<String, Object>> infos = new ArrayList<>();
            Map<String, List<Pair<String, Object>>> infosMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            switch (nameSplit.length) {
            case 1:
                //table
                statisticsClassifier.get(getAppName()).putIfAbsent(name, infosMap);
                statisticsClassifier.get(getAppName()).get(name).putIfAbsent("rowCount", infos);
                statisticsClassifier.get(getAppName()).get(name).get("rowCount")
                    .add(new Pair<>(null, entry.getValue()));
                break;
            case 2:
                // ignore sample rate
                if (nameSplit[1].equals("sampleRate")) {
                    break;
                }
                //schema.table
                statisticsClassifier.get(nameSplit[0]).putIfAbsent(nameSplit[1], infosMap);
                statisticsClassifier.get(nameSplit[0]).get(nameSplit[1]).putIfAbsent("rowCount", infos);
                statisticsClassifier.get(nameSplit[0]).get(nameSplit[1]).get("rowCount")
                    .add(new Pair<>(null, entry.getValue()));
                break;
            case 3:
                //table.columns.xxx
                if (!keywords.contains(nameSplit[2])) {
                    throw new Exception("unexpected statistics key: " + name);
                }
                statisticsClassifier.get(getAppName()).putIfAbsent(nameSplit[0], infosMap);
                statisticsClassifier.get(getAppName()).get(nameSplit[0]).putIfAbsent(nameSplit[2], infos);
                statisticsClassifier.get(getAppName()).get(nameSplit[0]).get(nameSplit[2])
                    .add(new Pair<>(nameSplit[1], entry.getValue()));
                break;
            case 4:
                //schema.table.columns.xxx
                if (!keywords.contains(nameSplit[3])) {
                    throw new Exception("unexpected statistics key: " + name);
                }
                statisticsClassifier.get(nameSplit[0]).putIfAbsent(nameSplit[1], infosMap);
                statisticsClassifier.get(nameSplit[0]).get(nameSplit[1]).putIfAbsent(nameSplit[3], infos);
                statisticsClassifier.get(nameSplit[0]).get(nameSplit[1]).get(nameSplit[3])
                    .add(new Pair<>(nameSplit[2], entry.getValue()));
                break;
            default:
                throw new Exception("unexpected statistics key: " + name);
            }
        }
        StatisticManager.sds = MockStatisticDatasource.getInstance();
        StatisticManager.getInstance().clearAndReloadData();
    }

    protected static List<Map<String, String>> loadSqls(String fileName, Class clazz) {
        String content = readToString(clazz.getResource(fileName).getPath());
        caseContent.put(fileName, content);
        casePath.put(fileName, clazz.getResource(fileName).getPath());
        Yaml yaml = new Yaml();
        Object o = yaml.load(content);
        String shrinkContent = replaceBlank(content);
        if (o == null) {
            return Lists.newArrayList();
        }
        // explain statistics mode
        if (o instanceof Map) {
            if (((Map<?, ?>) o).containsKey("SQL") && ((Map<?, ?>) o).containsKey("DDL")
                && ((Map<?, ?>) o).containsKey("STATISTICS") && ((Map<?, ?>) o).containsKey("CONFIG")) {
                totalMap.put(clazz, (Map<String, Object>) o);
                o = ((Map<?, ?>) o).get("SQL");
            }
        }
        if (o instanceof List) {
            List<Map<String, String>> list = (List<Map<String, String>>) o;
            for (Map<String, String> map : list) {
                String s = map.get("sql");
                Integer lineNum = getNum(shrinkContent, replaceBlank(s));
                map.put("lineNum", lineNum + "");
            }
            return (List<Map<String, String>>) o;
        } else {
            Map<String, String> m = (Map<String, String>) o;
            String s = m.get("sql");
            Integer lineNum = getNum(shrinkContent, replaceBlank(s));
            m.put("lineNum", lineNum + "");

            List<Map<String, String>> sqls = new ArrayList<>(1);
            sqls.add(m);
            return sqls;
        }

    }

    private static String replaceBlank(String str) {
        String dest = null;
        if (str == null) {
            return dest;
        } else {
            Pattern p = Pattern.compile("[ \\t\\x0B\\f\\r]");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
            return dest;
        }
    }

    private static Integer getNum(String content, String matchStr) {
        return getNum(content.substring(0, content.indexOf(matchStr)));
    }

    private static Integer getNum(String substring) {
        int index = 0;
        int count = 0;
        while (index != -1) {
            index = substring.indexOf("\n", index + 1);
            count++;
        }
        return count;
    }

    public static String readToString(String fileName) {
        String encoding = "utf-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void doPlanTest() {
        execSqlAndVerifyPlan(this.caseName, this.sqlIndex, this.sql, this.expectedPlan, this.expect, this.nodetree);
    }

    private int count = 1;
    private final Map<String, List<BasePlannerTest>> commonCases = Maps.newHashMap();
    private static final Map<String, String> caseContent = Maps.newHashMap();
    private static final Map<String, String> casePath = Maps.newHashMap();

    public String replacePlanStr(String planStr) {
        return planStr;
    }

    protected void execSqlAndVerifyPlan(String testMethodName, Integer sqlIdx, String targetSql, String targetPlan,
                                        String expect, String nodetree) {
        String planStr;
        try {
            planStr = getPlan(targetSql);
        } catch (Throwable e) {
            e.printStackTrace();
            planStr = e.getMessage();
            if (TStringUtil.isBlank(planStr)) {
                StringWriter w = new StringWriter();
                PrintWriter pw = new PrintWriter(w);
                e.printStackTrace(pw);
                planStr = w.toString();
            }
        }
        planStr = planStr.trim();

        targetPlan = targetPlan.trim().replaceAll("\r\n", "\n").replaceAll("\n", "\r\n").toLowerCase();
        planStr = planStr.trim().replaceAll("\r\n", "\n").replaceAll("\n", "\r\n").toLowerCase();

        System.out.println("Running test " + testMethodName + " - " + sqlIndex);
        System.out.println("link: xx.xx(" + testMethodName + ":" + lineNum + ")");

        planStr = planStr.replaceAll("_\\$[0-9a-f]{4}", "");

        // customize replace
        planStr = replacePlanStr(planStr);

//        targetPlan = new RandomTableSuffixRemover(appName).replaceRealPhysicalTableNames(targetSql, targetPlan);
        final String[] targetPlanVal = new String[1];
        final String[] realPlanVal = new String[1];
        targetPlanVal[0] = String.format("sql (case=%s#%s):\r\n\r\n%s\r\nplan:\r\n%s",
            testMethodName,
            sqlIdx,
            targetSql,
            targetPlan);
        realPlanVal[0] = String.format("sql (case=%s#%s):\r\n\r\n%s\r\nplan:\r\n%s",
            testMethodName,
            sqlIdx,
            targetSql,
            planStr);

        if (commonCases.get(caseName) != null) {
            commonCases.get(caseName).add(this);
        } else {
            List<BasePlannerTest> list = Lists.newArrayList();
            list.add(this);
            commonCases.put(caseName, list);
        }
        this.actual = planStr;
        if (count++ == caseNum && fixFlag) {
            printFullCases(commonCases);
        }

        IntStream.range(0, corMaxNum).forEach(i -> targetPlanVal[0] = targetPlanVal[0].replace("$cor" + i, "$cor"));
        IntStream.range(0, corMaxNum).forEach(i -> realPlanVal[0] = realPlanVal[0].replace("$cor" + i, "$cor"));

        assertEquals("targetSql = \n " + targetSql + "\nrealPlanVal = \n" + planStr
            + "\n targetPlanVal = \n" + targetPlan + "\n", targetPlanVal, realPlanVal);
    }

    private void printFullCases(Map<String, List<BasePlannerTest>> cases) {

        for (Entry<String, List<BasePlannerTest>> entry : cases.entrySet()) {
            String content = caseContent.get(entry.getKey());
            Collections.sort(entry.getValue(), new Comparator<BasePlannerTest>() {

                @Override
                public int compare(BasePlannerTest o1, BasePlannerTest o2) {
                    return o2.sqlIndex - o1.sqlIndex;
                }
            });
            for (BasePlannerTest planTestCommon : entry.getValue()) {
                content = insertExpectPlan(planTestCommon.sqlIndex, planTestCommon.actual, content);
            }
            String currentMethodFile = entry.getKey();
            File file = new File(currentMethodFile);
            if (file.exists()) {
                file.deleteOnExit();
            }

            try {
                file.createNewFile();
                writeTxtFile(content, file);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean writeTxtFile(String content, File fileName) throws Exception {
        boolean flag = false;
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(fileName);
            fileOutputStream.write(content.getBytes("gbk"));
            fileOutputStream.close();
            flag = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

    private String insertExpectPlan(int sqlIndex, String actual, String content) {
        int caseIndexStart = 0;
        for (int i = 0; i <= sqlIndex; i++) {
            caseIndexStart = content.indexOf("plan: |", caseIndexStart + 1);
        }
        int caseIndexEnd = content.indexOf("\n-", caseIndexStart);
        StringBuilder rs = new StringBuilder(content.substring(0, caseIndexStart + "plan: |".length() + 1));
        actual = "    " + actual.replaceAll("\n", "\n    ");
        if (caseIndexEnd == -1) {
            rs.append("\n").append(actual).append("\n");
        } else {
            rs.append("\n").append(actual).append("\n").append(content.substring(caseIndexEnd));
        }
        return rs.toString();
    }

    protected abstract String getPlan(String testSql);

    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param) {
        return removeSubqueryHashCode(planStr, plan, param, CalcitePlanOptimizerTrace.DEFAULT_LEVEL);
    }

    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        StringBuilder stringBuilder = new StringBuilder();
        for (RexDynamicParam rexDynamicParam : OptimizerUtils.findSubquery(plan)) {
            if (rexDynamicParam.getIndex() == -2) {
                stringBuilder.append(System.lineSeparator());
                stringBuilder.append(">> individual scalar subquery :");
            } else {
                stringBuilder.append(System.lineSeparator());
                stringBuilder.append(">> individual correlate subquery :");
            }
            planStr = planStr.replaceAll(rexDynamicParam.getRel().hashCode() + "", "");
            String subLogicalPlanString = RelUtils.toString(sqlExplainLevel, rexDynamicParam.getRel(), param);
            subLogicalPlanString = stripHashCode(subLogicalPlanString, rexDynamicParam.getRel());
            for (String row : StringUtils.split(subLogicalPlanString, "\r\n")) {
                stringBuilder.append(System.lineSeparator());
                stringBuilder.append(row);
            }
            stringBuilder.append(System.lineSeparator());
        }

        // show cache
        if (PlannerContext.getPlannerContext(plan).getCacheNodes().size() > 0) {
            stringBuilder.append("cache node:").append(System.lineSeparator());
            ;
            for (RelNode relNode : PlannerContext.getPlannerContext(plan).getCacheNodes()) {

                String subLogicalPlanString = RelUtils.toString(sqlExplainLevel, relNode, param);
                for (String row : StringUtils.split(subLogicalPlanString, "\r\n")) {
                    stringBuilder.append(System.lineSeparator());
                    stringBuilder.append(row);
                }
            }
        }

        return planStr + stringBuilder.toString();
    }

    private String stripHashCode(String planStr, RelNode root) {
        if (root.getInputs() != null && root.getInputs().size() > 0) {
            for (RelNode r : root.getInputs()) {
                planStr = planStr.replaceAll(r.hashCode() + "", "");
                planStr = stripHashCode(planStr, r);
            }
        }
        return planStr;
    }

    @Test
    public void testSql() {
        if (ignoreBaseTest) {
            return;
        }
        doPlanTest();
    }

    public SqlToRelConverter.Config buildConfig() {
        return SqlToRelConverter.configBuilder()
            .withConvertTableAccess(false)
            .withExpand(false)
            .build();
    }

    protected PartitionInfo buildPartitionInfoByLogCreateTbl(String schema, LogicalCreateTable logicalCreateTable,
                                                             ExecutionContext executionContext) {

        logicalCreateTable.prepareData(new ExecutionContext(schema));
        PartitionTableType tblType = PartitionTableType.SINGLE_TABLE;
        if (logicalCreateTable.isPartitionTable()) {
            tblType = PartitionTableType.PARTITION_TABLE;
        } else if (logicalCreateTable.isBroadCastTable()) {
            tblType = PartitionTableType.BROADCAST_TABLE;
        }
        DDL relDdl = logicalCreateTable.relDdl;
        CreateTablePreparedData preparedData = logicalCreateTable.getCreateTablePreparedData();

        String tbName = null;
        TableMeta tableMeta = null;
        List<ColumnMeta> allColMetas = null;
        List<ColumnMeta> pkColMetas = null;
        String tableGroupName = null;
        PartitionInfo partitionInfo = null;
        tableMeta = preparedData.getTableMeta();
        tableMeta.setSchemaName(schema);
        tbName = preparedData.getTableName();
        allColMetas = tableMeta.getAllColumns();
        pkColMetas = new ArrayList<>(tableMeta.getPrimaryKey());
        partitionInfo =
            PartitionInfoBuilder.buildPartitionInfoByPartDefAst(preparedData.getSchemaName(), tbName, tableGroupName,
                false, null,
                (SqlPartitionBy) preparedData.getPartitioning(), preparedData.getPartBoundExprInfo(), pkColMetas,
                allColMetas, tblType, executionContext);

        if (preparedData.getLocality() != null) {
            partitionInfo.setTableGroupId(Long.valueOf(preparedData.getLocality().toString().hashCode()));
            partitionInfo.setLocality(preparedData.getLocality().toString());
        }

        // Set auto partition flag only on primary table.
        if (tblType == PartitionTableType.PARTITION_TABLE) {
            assert relDdl.sqlNode instanceof SqlCreateTable;
            partitionInfo.setPartFlags(
                ((SqlCreateTable) relDdl.sqlNode).isAutoPartition() ? TablePartitionRecord.FLAG_AUTO_PARTITION : 0);
        }

        return partitionInfo;
    }
}

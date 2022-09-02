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

package com.alibaba.polardbx.qatest.ddl.sharding.repartition;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public abstract class RepartitionBaseTest extends DDLBaseNewDBTestCase {

    /**
     * 主要使用的2个主表
     */
    protected static final String DEFAULT_PRIMARY_TABLE_NAME = "alter_partition_ddl_primary_table";
    protected static final String MULTI_PK_PRIMARY_TABLE_NAME = "alter_partition_ddl_primary_table_multi_pk";
    protected static final String LOCAL_PARTITION_TABLE_NAME = "local_partition_ddl_primary_table";

    /**
     * 拆分规则模板
     */
    protected static final String PARTITIONING_TEMPLATE =
        "DBPARTITION BY {0}({1}) TBPARTITION BY {2}({3}) TBPARTITIONS {4}";

    /**
     * 拆分规则模板
     */
    protected static final String PARTITIONING_TEMPLATE2 =
        "DBPARTITION BY {0}({1})";

    /**
     * 拆分规则模板
     */
    protected static final String PARTITIONING_TEMPLATE3 =
        "DBPARTITION BY {0}({1}) TBPARTITION BY {2}({3})";

    protected static final FastsqlParser PARSER = new FastsqlParser();

    /**
     * 当前执行的单元测试使用的表名
     */
    protected String primaryTableName;

    /**
     * 禁用cach、禁用pk重复检查优化的hint
     */
    protected final String dmlHintStr =
        " /*+TDDL:cmd_extra(PLAN_CACHE=false,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_USE_RETURNING=FALSE)*/ ";

    protected final String failPointHint = "";

    protected boolean printExecutedSqlLog = false;

    protected Map<String, List<String>> storageGroupInfo = new HashMap<>();

    protected Set<String> groupNames = new HashSet<>();

    protected String repartitonBaseDb;

    @Before
    public void beforeRepartitionBaseTest() {
        this.repartitonBaseDb = tddlDatabase1;
    }

    public String getRepartitonBaseDB() {
        return repartitonBaseDb;
    }

    public boolean usingNewPartDb() {
        return false;
    }

    public void executeDml(String sql) throws SQLException {
        String sqlWithHint = sql;
        if (printExecutedSqlLog) {
            System.out.println(LocalTime.now().toString() + ":" + sqlWithHint);
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlWithHint);
    }

    public void initDatasourceInfomation() {
        String tddlSql = "show datasources";
        storageGroupInfo.clear();
        groupNames.clear();
        try {
            PreparedStatement stmt = JdbcUtil.preparedStatement(
                tddlSql, getPolardbxDirectConnection(getRepartitonBaseDB()));
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String sid = rs.getString("STORAGE_INST_ID");
                String groupName = rs.getString("GROUP");
                if (groupName.equalsIgnoreCase("MetaDB")
                    || groupName.equalsIgnoreCase("INFORMATION_SCHEMA_SINGLE_GROUP")) {
                    continue;
                }
                int writeWeight = rs.getInt("WRITE_WEIGHT");
                if (writeWeight <= 0) {
                    continue;
                }
                groupNames.add(groupName);
                if (!storageGroupInfo.containsKey(sid)) {
                    storageGroupInfo.put(sid, new ArrayList<>());
                }
                storageGroupInfo.get(sid).add(groupName);
            }
            rs.close();
            stmt.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        List<String> storageIDs = new ArrayList<>();
        storageGroupInfo.forEach((k, v) -> storageIDs.add(k));
    }

    protected Map<String, List<String>> getTableTopology(String logicalTable) {
        Map<String, List<String>> groupAndTbMap = new HashMap<>();
        // Get topology from logicalTableName
        String showTopology = String.format("show topology from %s", logicalTable);
        PreparedStatement stmt = JdbcUtil.preparedStatement(showTopology, tddlConnection);
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
            while (rs.next()) {
                String groupName = (String) rs.getObject(2);
                String phyTbName = (String) rs.getObject(3);
                if (groupAndTbMap.containsKey(groupName)) {
                    groupAndTbMap.get(groupName).add(phyTbName);
                } else {
                    groupAndTbMap.put(groupName, Lists.newArrayList(phyTbName));
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return groupAndTbMap;
    }

    protected Map<String, List<String>> getAllPhysicalTables(String[] logicalTables) {
        Map<String, List<String>> groupAndTables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String tableName : logicalTables) {
            Map<String, List<String>> groupAndPhyTablesForThisTable = getTableTopology(tableName);
            for (Map.Entry<String, List<String>> entry : groupAndPhyTablesForThisTable.entrySet()) {
                if (groupAndTables.containsKey(entry.getKey())) {
                    groupAndTables.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    groupAndTables.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return groupAndTables;
    }

    protected void doTestGsiInsertWhilePartition(
        PartitionParam originPartitionRule,
        PartitionParam newPartitionRule,
        String hint) throws SQLException {

        String insertSqlTemplate1 = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_datetime, c_datetime_1, c_datetime_3, c_datetime_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        String insertSqlTemplate2 = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_datetime, c_datetime_1, c_datetime_3, c_datetime_6) \n"
                + "VALUES (2, now(), now(), now(), now())", primaryTableName);
        int batchCount1 = 3;
        int batchCount2 = 3;
        final ExecutorService dmlPool = new ThreadPoolExecutor(
            batchCount1 + batchCount2,
            batchCount1 + batchCount2,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        final AtomicBoolean allStop1 = new AtomicBoolean(false);
        final AtomicBoolean allStop2 = new AtomicBoolean(false);
        Function<Connection, Integer> call1 = connection -> {
            // List<Pair< sql, error_message >>
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            try {
                return gsiExecuteUpdate(connection, mysqlConnection, insertSqlTemplate1, failedList, true, true);
            } catch (SQLSyntaxErrorException e) {
                if (StringUtils.contains(e.toString(), "ERR_TABLE_NOT_EXIST")) {
                    //do nothing
                    return 0;
                } else {
                    throw GeneralUtil.nestedException(e);
                }
            } catch (AssertionError e) {
                if (!StringUtils.contains(e.toString(), "Communications link failure")) {
                    throw GeneralUtil.nestedException(e);
                } else {
                    return 0;
                }
            }
        };
        Function<Connection, Integer> call2 = connection -> {
            // List<Pair< sql, error_message >>
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            try {
                return gsiExecuteUpdate(connection, mysqlConnection, insertSqlTemplate2, failedList, true, true);
            } catch (SQLSyntaxErrorException e) {
                if (StringUtils.contains(e.toString(), "ERR_TABLE_NOT_EXIST")) {
                    //do nothing
                    return 0;
                } else {
                    throw GeneralUtil.nestedException(e);
                }
            } catch (AssertionError e) {
                if (!StringUtils.contains(e.toString(), "Communications link failure")) {
                    throw GeneralUtil.nestedException(e);
                } else {
                    return 0;
                }
            }
        };
        List<Future> inserts = new ArrayList<>();

        createPrimaryTable(primaryTableName, originPartitionRule, true);

        if (StringUtils.containsIgnoreCase(originPartitionRule.broadcastOrSingle, "SINGLE")) {
            JdbcUtil.executeUpdate(tddlConnection, "drop sequence AUTO_SEQ_" + primaryTableName);
            JdbcUtil.executeUpdate(tddlConnection, "create sequence AUTO_SEQ_" + primaryTableName);
        }

        SqlCreateTable originPrimary = showCreateTable(primaryTableName);

        /**
         * when:
         * 持续执行insert的同时发生拆分键变更
         */
        //启动 batchCount 个 insert 任务
        IntStream.range(0, batchCount1).forEach(
            i -> inserts.add(dmlPool.submit(new InsertRunner(allStop1, call1, getRepartitonBaseDB())))
        );
        //执行拆分键变更
        String ddl = createAlterPartitionKeySQL(primaryTableName, newPartitionRule);
        executeDDL(hint + ddl);
        //启动 batchCount 个 insert 任务
        IntStream.range(0, batchCount2).forEach(
            i -> inserts.add(dmlPool.submit(new InsertRunner(allStop2, call2, getRepartitonBaseDB())))
        );
        try {
            TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
        }
        allStop1.set(true);
        allStop2.set(true);
        dmlPool.shutdown();
        for (Future future : inserts) {
            try {
                future.get();
            } catch (Exception e) {
                if (StringUtils.contains(e.toString(), "ERR_TABLE_NOT_EXIST")
                    || StringUtils.contains(e.toString(), "doesn't exist")) {
                    //do nothing
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
        dmlPool.shutdown();

        /**
         * then:
         * 做各种校验
         * 最主要校验的是主表和GSI表的数据一致性
         */
        generalAssert(originPrimary, ddl);
    }

    protected void executeSimpleTestCase(
        String primaryTableName,
        PartitionParam originPartitionRule,
        PartitionParam newPartitionRule
    ) throws SQLException {
        executeSimpleTestCase(primaryTableName, originPartitionRule, newPartitionRule, "");
    }

    protected void executeSimpleTestCase(
        String primaryTableName,
        PartitionParam originPartitionRule,
        PartitionParam newPartitionRule,
        String hint
    ) throws SQLException {
        executeSimpleTestCase(primaryTableName, originPartitionRule, newPartitionRule, hint, false);
    }

    protected void executeSimpleTestCase(
        String primaryTableName,
        PartitionParam originPartitionRule,
        PartitionParam newPartitionRule,
        String hint,
        boolean skipGeneralAssert
    ) throws SQLException {
        //given: a primary table
        createPrimaryTable(primaryTableName, originPartitionRule, true);
        SqlCreateTable originPrimary = showCreateTable(primaryTableName);
        //when: execute an "Alter Partition Key DDL"
        String ddl = createAlterPartitionKeySQL(primaryTableName, newPartitionRule);
        if (StringUtils.isNotEmpty(hint)) {
            ddl = hint + ddl;
        }
        executeDDL(ddl);
        //then
        if (!skipGeneralAssert) {
            generalAssert(originPrimary, ddl);
        }
    }

    /**
     * 创建主表(不包含局部索引)
     */
    protected void createPrimaryTable(String primaryTableName, PartitionParam p, boolean createMysqlTable)
        throws SQLException {
        String partitionRule;
        if (StringUtils.equalsIgnoreCase(p.broadcastOrSingle, "SINGLE")) {
            partitionRule = "";
        } else if (StringUtils.equalsIgnoreCase(p.broadcastOrSingle, "BROADCAST")) {
            partitionRule = "BROADCAST";
        } else {
            partitionRule = MessageFormat
                .format(PARTITIONING_TEMPLATE, p.dbOperator, p.dbOperand, p.tbOperator, p.tbOperand, p.tbCount);
        }

        String createTableSql;
        if (StringUtils.equals(MULTI_PK_PRIMARY_TABLE_NAME, primaryTableName)) {
            createTableSql = ExecuteTableSelect.getFullTypeMultiPkTableDef(primaryTableName, partitionRule);
        } else {
            createTableSql = ExecuteTableSelect.getFullTypeTableDef(primaryTableName, partitionRule);
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        final ResultSet resultSet = JdbcUtil.executeQuery(
            "SELECT COUNT(1) FROM " + primaryTableName,
            tddlConnection);
        assertThat(resultSet.next(), is(true));

        if (createMysqlTable) {
            String createMysqlTableSql;
            if (StringUtils.equals(MULTI_PK_PRIMARY_TABLE_NAME, primaryTableName)) {
                createMysqlTableSql = ExecuteTableSelect.getFullTypeMultiPkTableDef(primaryTableName, "");
            } else {
                createMysqlTableSql = ExecuteTableSelect.getFullTypeTableDef(primaryTableName, "");
            }
            JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysqlTableSql);
        }

    }

    /**
     * 生成拆分键变更DDL语句
     */
    protected String createAlterPartitionKeySQL(String primaryTableName, PartitionParam p) {
        String partitionRule;
        if (StringUtils.equalsIgnoreCase(p.broadcastOrSingle, "SINGLE")) {
            partitionRule = "SINGLE";
        } else if (StringUtils.equalsIgnoreCase(p.broadcastOrSingle, "BROADCAST")) {
            partitionRule = "BROADCAST";
        } else if (StringUtils.isEmpty(p.tbOperator)) {
            partitionRule = MessageFormat
                .format(PARTITIONING_TEMPLATE2, p.dbOperator, p.dbOperand);
        } else if (p.tbCount <= 1) {
            partitionRule = MessageFormat
                .format(PARTITIONING_TEMPLATE3, p.dbOperator, p.dbOperand, p.tbOperator, p.tbOperand);
        } else {
            partitionRule = MessageFormat
                .format(PARTITIONING_TEMPLATE, p.dbOperator, p.dbOperand, p.tbOperator, p.tbOperand, p.tbCount);
        }
        return MessageFormat
            .format(failPointHint + "ALTER TABLE {0}.{1} {2}", repartitonBaseDb, primaryTableName,
                partitionRule);
    }

    protected void executeDDL(String ddl) {
        logger.info("EXECUTE DDL: " + ddl);
        JdbcUtil.executeUpdateSuccess(tddlConnection, ddl);
    }

    protected void executeDDLIgnoreErr(String ddl, String err) {
        logger.info("EXECUTE DDL: " + ddl);
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, ddl, Sets.newHashSet(err));
    }

    protected SqlCreateTable showCreateTable(String tableName) throws SQLException {
        String sql = "show create table " + tableName;

        Statement stmt = null;
        ResultSet resultSet = null;
        String createTableString = null;
        try {
            stmt = tddlConnection.createStatement();
            resultSet = stmt.executeQuery(sql);
            resultSet.next();
            createTableString = resultSet.getString("Create Table");
        } finally {
            // close(stmt);
            JdbcUtil.close(resultSet);
        }
        return (SqlCreateTable) PARSER.parse(createTableString).get(0);
    }

    /**
     * 校验trace中含有特定的string出现n次
     */
    protected void assertTraceContains(List<List<String>> trace, String targetStr, int count) {
        int c = 0;
        for (List<String> item : trace) {
            for (String s : item) {
                if (StringUtils.containsIgnoreCase(s, targetStr)) {
                    c++;
                }
            }
        }
        //make sure now() is pushed down, instead of logical execution
        org.junit.Assert.assertEquals(count, c);
    }

    /**
     * 校验trace中含有特定的string
     */
    protected void assertTraceContains(List<List<String>> trace, String targetStr) {
        boolean containsNow = false;
        for (List<String> item : trace) {
            for (String s : item) {
                containsNow |= StringUtils.containsIgnoreCase(s, targetStr);
            }
        }
        //make sure now() is pushed down, instead of logical execution
        org.junit.Assert.assertTrue(containsNow);
    }

    /**
     * 1. 校验表结构
     * 2. 校验分库分表规则
     */
    protected void generalAssert(SqlCreateTable originAst, String apkDDL, String logicalTableName) throws SQLException {
//        try {
//            //因为原物理表是异步删除的，所以等待一下
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        SqlCreateTable newAst = showCreateTable(logicalTableName);

        SqlAlterTablePartitionKey ddl = (SqlAlterTablePartitionKey) PARSER.parse(apkDDL).get(0);

        List<Pair<SqlIdentifier, SqlIndexDefinition>> originGlobalKeys = originAst.getGlobalKeys();
        Assert.assertTrue(CollectionUtils.isEmpty(originGlobalKeys));

        List<Pair<SqlIdentifier, SqlIndexDefinition>> newGlobalKeys = newAst.getGlobalKeys();
        Assert.assertTrue(CollectionUtils.isEmpty(newGlobalKeys));

        //检查2个逻辑表的列是否全部相同
        Assert.assertTrue(identicalColumnDefList(originAst.getColDefs(), newAst.getColDefs()));

        //检查ddl的operands和gsi的index是否全部相同
        assertPartitionKeyEquals(ddl, newAst);
        assertPartitionCountEquals(newAst, ddl);
    }

    /**
     * 1. 校验表结构
     * 2. 校验分库分表规则
     */
    protected void generalAssert(SqlCreateTable originAst, String apkDDL) throws SQLException {
        generalAssert(originAst, apkDDL, primaryTableName);
    }

    /**
     * 获取table的所有拆分列
     */
    protected static void assertPartitionKeyEquals(SqlAlterTablePartitionKey ddl, SqlCreateTable newAst) {
        SqlBasicCall dbPartitionBy = (SqlBasicCall) ddl.getDbPartitionBy();
        SqlBasicCall tbPartitionBy = (SqlBasicCall) ddl.getTablePartitionBy();

        if (dbPartitionBy != null) {
            dbPartitionBy.equalsDeep(newAst.getDbpartitionBy(), Litmus.THROW);
        }
        if (tbPartitionBy != null) {
            tbPartitionBy.equalsDeep(newAst.getTbpartitionBy(), Litmus.THROW);
        }
    }

    /**
     * 获取table的所有拆分列
     */
    protected static void assertPartitionKeyEquals(SqlCreateTable originAst, SqlIndexDefinition gsiAst) {
        SqlBasicCall dbPartitionBy = (SqlBasicCall) originAst.getDbpartitionBy();
        SqlBasicCall tbPartitionBy = (SqlBasicCall) originAst.getTbpartitionBy();

        if (dbPartitionBy != null) {
            dbPartitionBy.equalsDeep(gsiAst.getDbPartitionBy(), Litmus.THROW);
        }
        if (tbPartitionBy != null) {
            tbPartitionBy.equalsDeep(gsiAst.getTbPartitionBy(), Litmus.THROW);
        }
    }

    protected static void assertPartitionCountEquals(SqlCreateTable newAst, SqlAlterTablePartitionKey ddl) {
        SqlNumericLiteral ddlTbCount = (SqlNumericLiteral) ddl.getTbpartitions();
        SqlNumericLiteral newTbCount = (SqlNumericLiteral) newAst.getTbpartitions();

        if (ddlTbCount != null) {
            ddlTbCount.equalsDeep(newTbCount, Litmus.THROW);
        }
    }

    /**
     * identical columns with identical column order
     */
    protected boolean identicalColumnDefList(List<Pair<SqlIdentifier, SqlColumnDeclaration>> expected,
                                             List<Pair<SqlIdentifier, SqlColumnDeclaration>> actual) {
        if (!GeneralUtil.sameSize(expected, actual)) {
            System.err.println("unmatched column def size: ");
            System.err.println("expected: " + (expected == null ? 0 : expected.size()));
            System.err.println("actually: " + (actual == null ? 0 : actual.size()));
            return false;
        }

        if (null == expected) {
            return true;
        }

        for (int i = 0; i < expected.size(); i++) {
            if (!identicalColumnDef(expected.get(i), actual.get(i))) {
                System.err.println("unmatched column def: ");
                System.err.println("expected: " + expected.get(i).toString());
                System.err.println("actually: " + actual.get(i).toString());
                return false;
            }
        }

        return true;
    }

    /**
     * identical column def
     */
    protected boolean identicalColumnDef(Pair<SqlIdentifier, SqlColumnDeclaration> expected,
                                         Pair<SqlIdentifier, SqlColumnDeclaration> actual) {
        return expected.getKey().equalsDeep(actual.getKey(), Litmus.IGNORE)
            && expected.getValue().equalsDeep(actual.getValue(), Litmus.IGNORE);
    }

    protected static final PartitionParam ruleOf(String dbOperator, String dbOperand, String tbOperator,
                                                 String tbOperand,
                                                 int tbCount) {
        return new PartitionParam(dbOperator, dbOperand, tbOperator, tbOperand, tbCount);
    }

    protected static final PartitionParam ruleOf(String broadcastOrSingle) {
        return new PartitionParam(broadcastOrSingle);
    }

    public static class PartitionParam {

        public String broadcastOrSingle;

        public String dbOperator;
        public String dbOperand;
        public String tbOperator;
        public String tbOperand;
        public int tbCount;

        public String operator;
        public String func;
        public String operand;
        public int partCount;

        public PartitionParam(String dbOperator, String dbOperand, String tbOperator, String tbOperand,
                              int tbCount) {
            this.dbOperator = dbOperator;
            this.dbOperand = dbOperand;
            this.tbOperator = tbOperator;
            this.tbOperand = tbOperand;
            this.tbCount = tbCount;
        }

        public PartitionParam(String operator, String func, String operand, int partCount) {
            this.operator = operator;
            this.func = func;
            this.operand = operand;
            this.partCount = partCount;
        }

        public PartitionParam(String broadcastOrSingle) {
            this.broadcastOrSingle = broadcastOrSingle;
        }
    }

    /******************************************INSERT 逻辑*************************************************/

    /**
     * 启动多个任务，执行插入语句
     *
     * @param insertSqlTemplate insert语句
     * @param batchParams insert语句的参数
     * @param batchCount 启动的任务数量
     */
    protected void doInsert(
        final String insertSqlTemplate,
        final List<Map<Integer, ParameterContext>> batchParams,
        final int batchCount) {

        final ExecutorService dmlPool = new ThreadPoolExecutor(
            batchCount,
            batchCount,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        final AtomicBoolean stop = new AtomicBoolean(false);

        Function<Connection, Integer> call = connection -> {
            // List<Pair< sql, error_message >>
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            try {
                int[] result =
                    gsiBatchUpdate(connection, mysqlConnection, insertSqlTemplate, batchParams, failedList, true, true);
                return Optional.ofNullable(result).map(r -> Arrays.stream(r).map(v -> v / -2).sum()).orElse(0);
            } catch (SQLSyntaxErrorException e) {
                throw GeneralUtil.nestedException(e);
            }
        };

        List<Future> inserts = new ArrayList<>();

        //启动 batchCount 个 insert 任务
        IntStream.range(0, batchCount).forEach(
            i -> inserts.add(dmlPool.submit(new InsertRunner(stop, call, getRepartitonBaseDB())))
        );

        try {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
            // ignore exception
        }

        stop.set(true);
        dmlPool.shutdown();

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected static final Consumer<Exception> THROW_EXCEPTION = (e) -> {
        throw GeneralUtil.nestedException(e);
    };

    protected static class InsertRunner implements Runnable {

        protected final AtomicBoolean stop;
        protected final Function<Connection, Integer> call;
        protected final Consumer<Exception> errHandler;
        protected final String schemaName;

        public InsertRunner(AtomicBoolean stop, Function<Connection, Integer> call, String schemaName) {
            this(stop, call, THROW_EXCEPTION, schemaName);
        }

        public InsertRunner(AtomicBoolean stop, Function<Connection, Integer> call, Consumer<Exception> errHandler,
                            String schemaName) {
            this.stop = stop;
            this.call = call;
            this.errHandler = errHandler;
            this.schemaName = schemaName;
        }

        @Override
        public void run() {
            final long startTime = System.currentTimeMillis();
            int count = 0;
            do {
                try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
                    JdbcUtil.useDb(conn, schemaName);
                    count += call.apply(conn);
                } catch (Exception e) {
                    errHandler.accept(e);
                }

                if (System.currentTimeMillis() - startTime > 10000) {
                    break; // 10s timeout, because we check after create GSI(which makes create GSI far more slower.).
                }
            } while (!stop.get());

            System.out.println(Thread.currentThread().getName() + " quit after " + count + " records inserted");

        }

    }

}
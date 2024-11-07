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

package com.alibaba.polardbx.qatest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.cdc.CdcTableUtil;
import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.IndexColumnType;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.IndexRecord;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.qatest.entity.TestSequence;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.truth.Truth;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlColumnDeclaration.SpecialIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlIndexDefinition.SqlIndexResiding;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.resultSetContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

public class DDLBaseNewDBTestCase extends BaseTestCase {
    protected static final Logger logger = LoggerFactory.getLogger(DDLBaseNewDBTestCase.class);

    private static final String DB_HINT = "/*+TDDL:cmd_extra(SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT=1)*/";
    private static final FastsqlParser fastsqlParser;
    protected static final String HINT_CREATE_GSI =
        "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true)*/ ";
    protected static final String HINT_ALLOW_ALTER_GSI_INDIRECTLY =
        "/*+TDDL:cmd_extra(ALLOW_ALTER_GSI_INDIRECTLY=true)*/ ";
    protected static final String GSI_ALLOW_ADD_HINT =
        "/*+TDDL:cmd_extra(ALLOW_ADD_GSI=true GSI_CHECK_AFTER_CREATION=false)*/ ";
    protected static final String GSI_ALLOW_ADD_HINT_NO_RESTRICTION =
        "/*+TDDL:cmd_extra(ALLOW_ADD_GSI=true GSI_IGNORE_RESTRICTION=true GSI_CHECK_AFTER_CREATION=false)*/ ";
    protected static final String SKIP_WAIT_CCI_CREATION_HINT =
        "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/ ";
    protected static final String HINT_PURE_MODE = "/*+TDDL:CMD_EXTRA(PURE_ASYNC_DDL_MODE=TRUE)*/";
    protected static final String HINT_DDL_FAIL_POINT_TMPL = "/*+TDDL:CMD_EXTRA(%s='%s')*/";

    protected static String META_DB_HINT = "/*TDDL:NODE='__META_DB__'*/";

    private static final String DDL_DB_CUSTOM_PREFIX = getDdlDbCustomPrefix();
    private static final String DDL1_DB_PREFIX = DDL_DB_CUSTOM_PREFIX + "ddl1_";
    private static final String DDL2_DB_PREFIX = DDL_DB_CUSTOM_PREFIX + "ddl2_";

    protected String tddlDatabase1;
    protected String mysqlDatabase1;
    protected Connection tddlConnection;
    protected Connection mysqlConnection;
    protected Connection infomationSchemaDbConnection;

    protected boolean crossSchema;

    protected boolean auto;
    protected String tddlDatabase2;
    protected String schemaPrefix;
    protected String mysqlDatabase2;
    protected String mysqlSchemaPrefix;
    private Connection tddlConnection2;
    protected Connection mysqlConnection2;

    static {
        fastsqlParser = new FastsqlParser();

        try (Connection tddlCon = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeQuery("set enable_set_global=true", tddlCon);
            JdbcUtil.executeQuery("set global AUTO_PARTITION_PARTITIONS=3", tddlCon);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        try (Connection tddlCon = ConnectionManager.getInstance().newPolarDBXConnection()) {
            List<String> result = new ArrayList<>();
            try (ResultSet rs = JdbcUtil.executeQuery("show databases;", tddlCon)) {

                while (rs.next()) {
                    String dbName = rs.getString(1);
                    if (dbName.startsWith(DDL1_DB_PREFIX) || dbName.startsWith(DDL2_DB_PREFIX)) {
                        result.add(dbName);
                    }
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
            for (String db : result) {
                JdbcUtil.dropDatabase(tddlCon, db);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        try (Connection mysqlConn = ConnectionManager.getInstance().newMysqlConnection()) {
            List<String> result = new ArrayList<>();
            try (ResultSet rs = JdbcUtil.executeQuery("show databases;", mysqlConn)) {

                while (rs.next()) {
                    String dbName = rs.getString(1);
                    if (dbName.startsWith(DDL1_DB_PREFIX) || dbName.startsWith(DDL2_DB_PREFIX)) {
                        result.add(dbName);
                    }
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
            for (String db : result) {
                JdbcUtil.dropDatabase(mysqlConn, db);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private Connection connection;

    private static String getDdlDbCustomPrefix() {
        String value = System.getProperty("ddl_db_custom_prefix");
        String result = StringUtils.isBlank(value) ? "" : (value.endsWith("_") ? value : value + "_");
        logger.info("ddl db custom prefix is " + result);
        return result;
    }

    @Override
    public synchronized Connection getPolardbxConnection() {
        return super.getPolardbxConnection(tddlDatabase1);
    }

    @Override
    public synchronized Connection getPolardbxConnection2() {
        return super.getPolardbxConnection(tddlDatabase2);
    }

    @Override
    public synchronized Connection getPolardbxDirectConnection() {
        return super.getPolardbxDirectConnection(tddlDatabase1);
    }

    @Override
    public synchronized Connection getMysqlConnection() {
        return super.getMysqlConnection(mysqlDatabase1);
    }

    @Override
    public synchronized Connection getMysqlDirectConnection() {
        return super.getMysqlDirectConnection(mysqlDatabase1);
    }

    @Before
    public void beforeDDLBaseNewDBTestCase() {
        this.tddlConnection = getTddlConnection1();
        if (crossSchema) {
            getTddlConnection2();
        } else {
            tddlDatabase2 = "";
            schemaPrefix = "";
            mysqlDatabase2 = "";
            mysqlSchemaPrefix = "";
        }
    }

//    @After
//    public void afterDDLBaseNewDBTestCase() {
//        cleanDataBase();
//    }

    protected Connection createTddlDb(String db) {
        Connection connection = null;
        connection = super.getPolardbxConnection();
        String dbName = "";
        //sometime create db will be faile when get metaDataLock timeout
        int retryCount = 3;
        try {
            while (!dbName.equalsIgnoreCase(db) && retryCount > 0) {
                retryCount--;
                try {
                    if (!usingNewPartDb()) {
                        JdbcUtil.createDatabase(connection, db, DB_HINT);
                    } else {
                        JdbcUtil.createPartDatabase(connection, db);
                    }
                    dbName = connection.getSchema();
                } catch (Throwable ex) {
                    if (ex.getMessage().toLowerCase().contains("not finished dropping")) {
                        try {
                            JdbcUtil.dropDatabase(connection, db);
                        } catch (Throwable e2) {
                            logger.warn(e2.getMessage());
                        }

                    } else {
                        throw ex;
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return connection;
    }

    protected Connection createMysqlDb(String db) {
        Connection connection = null;
        try {
            connection = super.getMysqlConnection();
            JdbcUtil.createDatabase(connection, db, "");
        } catch (Throwable t) {
            //ignore
        }
        return connection;
    }

    protected Connection getTddlConnection1() {
        if (tddlConnection == null) {
            String database1 = getTestDBName(DDL1_DB_PREFIX);
            databaseForDDLTest = database1;
            String myDatabase1 = database1 + "_mysql";
            this.tddlConnection = createTddlDb(database1);
            this.mysqlConnection = createMysqlDb(myDatabase1);
            this.tddlDatabase1 = database1;
            this.mysqlDatabase1 = myDatabase1;
            this.infomationSchemaDbConnection = getMysqlConnection("information_schema");
        }
        return tddlConnection;
    }

    private String databaseForDDLTest;

    protected String getDdlSchema() {
        return databaseForDDLTest;
    }

    protected Connection getNewTddlConnection1() throws SQLException {
        Connection newConn = getPolardbxConnection();
        try (Statement stmt = newConn.createStatement()) {
            stmt.execute("use " + databaseForDDLTest);
        }
        return newConn;
    }

    protected String getTestDBName(String schemaPrefix) {
        String database1 = schemaPrefix + Math.abs(Thread.currentThread().getName().hashCode());
        if (usingNewPartDb()) {
            database1 = database1 + "_new";
        }
        return database1;
    }

    protected Connection getTddlConnection2() {
        if (tddlConnection2 == null) {
            String database2 = getTestDBName(DDL2_DB_PREFIX);
            this.tddlConnection2 = createTddlDb(database2);
            this.tddlDatabase2 = database2;
            this.schemaPrefix = tddlDatabase2 + ".";

            String myDatabase2 = tddlDatabase2 + "_mysql";
            this.mysqlConnection2 = createMysqlDb(myDatabase2);
            this.mysqlDatabase2 = myDatabase2;
            this.mysqlSchemaPrefix = mysqlDatabase2 + ".";
        }
        return tddlConnection2;
    }

    protected void cleanDataBase() {
        if (!StringUtils.isEmpty(tddlDatabase1)) {
            //retry when drop db error
            JdbcUtil.dropDatabaseWithRetry(tddlConnection, tddlDatabase1, 3);
            JdbcUtil.dropDatabase(mysqlConnection, mysqlDatabase1);
            this.tddlDatabase1 = null;
        }

        if (!StringUtils.isEmpty(tddlDatabase2)) {
            JdbcUtil.dropDatabaseWithRetry(tddlConnection2, tddlDatabase2, 3);
            JdbcUtil.dropDatabase(mysqlConnection2, mysqlDatabase2);
            this.tddlDatabase2 = null;
        }
    }

    public List<Connection> getMySQLPhysicalConnectionList(String db) {
        List<Connection> physicalDbConnList = new ArrayList<>();
        DefaultDBInfo.ShardGroupInfo groupInfos =
            DefaultDBInfo.getInstance().getShardGroupListByMetaDb(db, usingNewPartDb()).getValue();
        List<String> grpNameList = new ArrayList<>(groupInfos.groupAndPhyDbMaps.keySet());
        Collections.sort(grpNameList);

        if (PropertiesUtil.dnCount > 1) {
            for (String grpName : grpNameList) {
                String storageAddress = getStorageAddressByGroupName(grpName);
                String phyDbName = groupInfos.groupAndPhyDbMaps.get(grpName);
                Connection shardDbConn = getMysqlConnectionByAddress(storageAddress, phyDbName);
                physicalDbConnList.add(shardDbConn);
            }
        } else {
            for (String grpName : grpNameList) {
                String phyDbName = groupInfos.groupAndPhyDbMaps.get(grpName);
                Connection shardDbConn = getMysqlConnection(phyDbName);
                physicalDbConnList.add(shardDbConn);
            }
        }
        return physicalDbConnList;
    }

    public Connection getMySQLPhysicalConnectionByGroupName(String db, String grpName) {
        DefaultDBInfo.ShardGroupInfo groupInfos =
            DefaultDBInfo.getInstance().getShardGroupListByMetaDb(db, usingNewPartDb()).getValue();
        String storageAddress = getStorageAddressByGroupName(grpName);
        String phyDbName = groupInfos.groupAndPhyDbMaps.get(grpName);
        Connection shardDbConn = getMysqlConnectionByAddress(storageAddress, phyDbName);
        return shardDbConn;
    }

    private String getStorageAddressByGroupName(String grpName) {
        try (Connection metaDbConn = ConnectionManager.getInstance().getDruidMetaConnection()) {
            JdbcUtil.useDb(metaDbConn, PropertiesUtil.getMetaDB);
            String instanceId = PropertiesUtil.configProp.getProperty("instanceId");
            try (Statement stmt = metaDbConn.createStatement()) {
                stmt.execute(String.format("select s.ip,s.port from group_detail_info d,storage_info s where "
                        + "d.storage_inst_id = s.storage_inst_id and  d.group_name = '%s' and d.inst_id = '%s'"
                        + " and is_vip = 1",
                    grpName, instanceId));
                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next()) {
                        String ip = rs.getString("ip");
                        String port = rs.getString("port");
                        return ip + ":" + port;
                    }

                } catch (Throwable ex) {
                    throw ex;
                }
            } catch (Throwable ex) {
                throw ex;
            }
        } catch (Throwable ex) {
            throw new RuntimeException(ex);
        }
        throw new RuntimeException("can`t find storage info for group " + grpName);
    }

    protected static int[] gsiBatchUpdate(Connection tddlConnection, Connection mysqlConnection, String insert,
                                          List<Map<Integer, ParameterContext>> batchParams,
                                          List<Pair<String, Exception>> failedList, boolean insertMySql,
                                          boolean compareWithMySql)
        throws SQLSyntaxErrorException {
        int[] result = null;

        Pair<String, Exception> failed = null;
        try (PreparedStatement ps = tddlConnection.prepareStatement(insert)) {
            batchParams.forEach(params -> {
                try {
                    ParameterMethod.setParameters(ps, params);
                    ps.addBatch();
                } catch (SQLException e) {
                    throw GeneralUtil.nestedException(e);
                }
            });
            result = ps.executeBatch();
        } catch (SQLSyntaxErrorException msee) {
            throw msee;
        } catch (SQLException e) {
            failed = Pair.of(insert, e);
            failedList.add(failed);
        }

        if (!insertMySql) {
            return result;
        }

        try (PreparedStatement ps = mysqlConnection.prepareStatement(insert)) {
            batchParams.forEach(params -> {
                try {
                    ParameterMethod.setParameters(ps, params);
                    ps.addBatch();
                } catch (SQLException e) {
                    throw GeneralUtil.nestedException(e);
                }
            });

            if (compareWithMySql || failed == null) {
                ps.executeBatch();
            }

            if (null != failed && compareWithMySql) {
                assertWithMessage(
                    "DRDS 报错，MySQL 正常, Sql：\n " + failed.left + "\nCause: " + failed.right.getMessage())
                    .fail();
            }
        } catch (SQLSyntaxErrorException msee) {
            throw msee;
        } catch (SQLException e) {
            if (null != failed) {
                assertWithMessage("DRDS/MySQL 错误不一致, Sql：\n " + failed.left).that(failed.right.getMessage())
                    .contains(e.getMessage());
            } else {
                assertWithMessage("DRDS 正常，MySQL 报错, Sql：" + insert + "\n cause: " + e.getMessage()).fail();
            }
        }

        return result;
    }

    protected static int gsiExecuteUpdate(Connection tddlConnection, Connection mysqlConnection, String insert,
                                          List<Pair<String, Exception>> failedList, boolean insertMySql,
                                          boolean compareWithMySql)
        throws SQLSyntaxErrorException {
        return gsiExecuteUpdate(tddlConnection, mysqlConnection, insert, insert, failedList, insertMySql,
            compareWithMySql, true);
    }

    protected static int gsiExecuteUpdate(Connection tddlConnection, Connection mysqlConnection, String insert,
                                          String tddlInsert, List<Pair<String, Exception>> failedList,
                                          boolean insertMySql, boolean compareWithMySql, boolean compareWithMySqlErr)
        throws SQLSyntaxErrorException {
        int result = 0;

        Pair<String, Exception> failed = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            result = stmt.executeUpdate(tddlInsert);
        } catch (SQLSyntaxErrorException msee) {
            throw msee;
        } catch (SQLException e) {
            failed = Pair.of(tddlInsert, e);
            failedList.add(failed);
        }

        if (!insertMySql) {
            return result;
        }

        try (Statement stmt = mysqlConnection.createStatement()) {
            if (compareWithMySql || failed == null) {
                stmt.executeUpdate(insert);
            }

            if (null != failed && compareWithMySql) {
                assertWithMessage(
                    "DRDS 报错，MySQL 正常, Sql：\n " + failed.left + "\nCause: " + failed.right.getMessage())
                    .fail();
            }
        } catch (SQLSyntaxErrorException msee) {
            throw msee;
        } catch (SQLException e) {
            if (null != failed) {
                if (!failed.right.getMessage().contains("java.lang.AssertionError")) {
                    final String prefix = "Data truncation: ";
                    String msg = e.getMessage();
                    if (msg.startsWith(prefix)) {
                        msg = msg.substring(prefix.length());
                    }
                    if (compareWithMySqlErr) {
                        assertWithMessage("DRDS/MySQL 错误不一致, Sql：\n " + failed.left).that(
                                failed.right.getMessage())
                            .contains(msg);
                    }
                }
            } else {
                assertWithMessage("DRDS 正常，MySQL 报错, Sql：" + insert + "\n cause: " + e.getMessage()).fail();
            }
        }

        return result;
    }

    public void enableFailPoint(String key, String value) {
        String sql = String.format("set @%s='%s'", key, value);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public void disableFailPoint(String key) {
        String sql = String.format("set @%s=null", key);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public void clearFailPoints() {
        String sql = "set @fp_clear=true";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public List<Pair<String, String>> showFailPoints() {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select @fp_show");
        List<Pair<String, String>> result = new ArrayList<>();
        try {
            String str = JdbcUtil.resultsStr(rs);
            if (StringUtils.isEmpty(str) || StringUtils.equalsIgnoreCase(str, "null")) {
                return result;
            }
            Splitter.on(",").splitToList(str).stream().forEach(e -> {
                List<String> kv = Splitter.on("=").splitToList(e);
                result.add(Pair.of(kv.get(0), kv.get(1)));
            });
        } finally {
            JdbcUtil.close(rs);
        }
        return result;
    }

    public void dropTableIfExists(String tableName) {
        dropTableIfExists(tddlConnection, tableName);
    }

    public void dropTableIfExists(Connection conn, String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    public void dropTableIfExistsInMySql(String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
    }

    public void dropTableGroupIfExists(String tgName) {
        dropTableGroupIfExists(tddlConnection, tgName);
    }

    public void dropTableGroupIfExists(Connection conn, String tgName) {
        String sql = String.format("drop tablegroup if exists %s", tgName);
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    public void dropFunctionIfExists(String funcName) {
        dropFunctionIfExists(tddlConnection, funcName);
    }

    public void dropFunctionIfExistsInMySql(String funcName) {
        dropFunctionIfExists(mysqlConnection, funcName);
    }

    public void dropFunctionIfExists(Connection conn, String funcName) {
        String sql = "drop function if exists " + funcName;
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    public void clearIndex(String tableName, String index) {
        String sql = String.format("drop index %s on %s", index, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public void clearIndexByAlterTable(String tableName, String index) {
        String sql = String.format("alter table %s drop index %s", tableName, index);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public void dropTableWithGsi(String primary, List<String> indexNames) {
        final String finalPrimary = quoteSpecialName(primary);
        try (final Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + finalPrimary);

            for (String gsi : Optional.ofNullable(indexNames).orElse(ImmutableList.of())) {
                stmt.execute("DROP TABLE IF EXISTS " + quoteSpecialName(gsi));
            }
            return;
        } catch (Exception e) {
            logger.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public static String quoteSpecialName(String primary) {
        if (!TStringUtil.contains(primary, ".")) {
            if (primary.contains("`")) {
                primary = "`" + primary.replaceAll("`", "``") + "`";
            } else {
                primary = "`" + primary + "`";
            }
        }
        return primary;
    }

    public void clearForeignTable(String tableName) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FOREIGN_KEY_CHECKS = 0");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FOREIGN_KEY_CHECKS = 1");

    }

    public void dropTablesIfExists(String[] tableNames) {
        for (String tableName : tableNames) {
            dropTableIfExists(tableName);
        }
    }

    public void dropGsi(String tableName, String gsiName) {
        String sql = MessageFormat.format("DROP INDEX {0} on {1}", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public void dropTable(String tableName) {
        String sql = "drop table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public void dropTableFaild(String tableName) {
        String sql = "drop table " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    public long getLastInsertId(Connection conn) throws SQLException {
        String sql = "select last_insert_id() as  a";
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getLong("a");
        } finally {
            JdbcUtil.close(rs);
        }

    }

    public long getIdentity(Connection conn) throws SQLException {
        String sql = "select @@identity a";
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getLong("a");
        } finally {
            JdbcUtil.close(rs);
        }

    }

    /**
     * assert表存在，通过select * 是否出错来判断
     */
    public static void assertExistsTable(String tableName, Connection conn) {
        String sql = String.format("select * from %s limit 1", tableName);
        ResultSet rs = null;
        PreparedStatement prepareStatement = null;
        try {
            prepareStatement = conn.prepareStatement(sql);
            rs = prepareStatement.executeQuery();
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        } finally {

            try {
                if (prepareStatement != null) {
                    prepareStatement.close();
                }
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                logger.error("rs close error", e);
            }

        }
    }

    /**
     * assert表不存在
     */
    public static void assertNotExistsTable(String tableName, Connection conn) {
        String sql = String.format("select * from %s limit 1", tableName);
        ResultSet rs = null;
        PreparedStatement prepareStatement = null;
        try {
            prepareStatement = conn.prepareStatement(sql);
            rs = prepareStatement.executeQuery();
            Assert.fail("table exist : " + tableName);
        } catch (Exception ex) {
            // 因为表不存在，所以要报错
            Assert.assertTrue(ex != null);
        } finally {
            try {
                if (prepareStatement != null) {
                    prepareStatement.close();
                }
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                logger.error("rs close error", e);
            }

        }
    }

    public String showCreateTable(Connection conn, String tbName) {
        String sql = "show create table " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            if (isMySQL80()) {
                return rs.getString("Create Table").replaceAll(" CHARACTER SET \\w+", "")
                    .replaceAll(" COLLATE \\w+", "")
                    .replaceAll(" DEFAULT COLLATE = \\w+", "")
                    .replaceAll(" int ", " int(11) ")
                    .replaceAll(" bigint ", " bigint(11) ")
                    .replaceAll(" int,", " int(11),")
                    .replaceAll(" bigint,", " bigint(11),")
                    .replaceAll(" int\n", " int(11)\n")
                    .replaceAll(" bigint\n", " bigint(11)\n");
            }
            return rs.getString("Create Table").replace(" DEFAULT COLLATE = utf8mb4_0900_ai_ci", "");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return null;
    }

    public List<Pair<String, String>> showTopologyWithGroup(Connection conn, String tbName) {
        List<Pair<String, String>> phyTables = new ArrayList<>();
        String sql = "show topology " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            while (rs.next()) {
                String group = rs.getString("GROUP_NAME");
                String table = rs.getString("TABLE_NAME");
                phyTables.add(Pair.of(group, table));
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return phyTables;
    }

    public List<String> showTopology(Connection conn, String tbName) {
        List<String> phyTables = new ArrayList<>();
        String sql = "show topology " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            while (rs.next()) {
                String table = rs.getString("TABLE_NAME");
                phyTables.add(table);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return phyTables;
    }

    public List<String> showTables(Connection conn) {
        List<String> tables = new ArrayList<>();
        ResultSet resultSet = JdbcUtil.executeQuery("show tables", conn);
        try {
            while (resultSet.next()) {
                String table = resultSet.getString(1);
                tables.add(table);
            }
        } catch (Throwable e) {
            com.alibaba.polardbx.common.utils.Assert.fail();
        }

        return tables;
    }

    public int getDataNumFromTable(Connection conn, String tableName) {
        String sql = "select count(*) as cnt from " + tableName;
        int cnt = 0;
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            assertThat(rs.next()).isTrue();
            return rs.getInt("cnt");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return cnt;
    }

    public List<Integer> getAllDataNumFromTable(Connection conn, String tableName, String columnTobeSelect) {
        String sql = String.format("select %s from %s order by %s asc", columnTobeSelect, tableName, columnTobeSelect);
        List<Integer> nums = new ArrayList<>();
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            while (rs.next()) {
                nums.add(rs.getInt(1));
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return nums;
    }

    // drop 某个分库中得某个表，主要用于异常测试
    public void dropOneDbInMysql(List<Connection> physicalConnectionList, String tableName, int index) {
        String sql = "drop table if exists " + tableName;
        if (index >= physicalConnectionList.size()) {
            return;
        }
        PreparedStatement ps = null;
        try {
            ps = JdbcUtil.preparedStatement(sql, physicalConnectionList.get(index));
            JdbcUtil.executeUpdate(ps);
        } finally {
            JdbcUtil.close(ps);
        }
    }

    public TestSequence showSequence(String seqName) {
        TestSequence testSequence = null;
        ResultSet rs;
        String curSchema = null;

        if (seqName.contains(".")) {
            curSchema = seqName.split("\\.")[0];
        }

        String sql = "show sequences";
        if (curSchema != null && curSchema.equalsIgnoreCase(tddlDatabase2)) {
            rs = JdbcUtil.executeQuerySuccess(getTddlConnection2(), sql);
        } else {
            curSchema = tddlDatabase1;
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        }
        seqName = getSimpleTableName(seqName);
        try {
            while (rs.next()) {
                if (rs.getString("name").equalsIgnoreCase(seqName)) {
                    if (!rs.getString("schema_name").equalsIgnoreCase(curSchema)) {
                        // polarx为实例级别
                        continue;
                    }
                    testSequence = new TestSequence();
                    testSequence.setName(rs.getString("name"));
                    testSequence.setValue(parseLongWithNA(rs.getString("value")));
                    testSequence.setIncrementBy(parseLongWithNA(getSeqAttrWithoutEx(rs, "increment_by")));
                    testSequence.setStartWith(parseLongWithNA(getSeqAttrWithoutEx(rs, "start_with")));
                    testSequence.setMaxValue(parseLongWithNA(getSeqAttrWithoutEx(rs, "max_value")));
                    testSequence.setCycle(getSeqAttrWithoutEx(rs, "cycle"));
                    testSequence.setUnitCount(parseLongWithNA(getSeqAttrWithoutEx(rs, "unit_count")));
                    testSequence.setUnitIndex(parseLongWithNA(getSeqAttrWithoutEx(rs, "unit_index")));
                    testSequence.setInnerStep(parseLongWithNA(getSeqAttrWithoutEx(rs, "inner_step")));
                    break;
                }
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail(e.getMessage());
            testSequence = null;

        } finally {
            JdbcUtil.close(rs);
        }
        return testSequence;
    }

    private String getSeqAttrWithoutEx(ResultSet rs, String colName) {
        String result = SequenceAttribute.STR_NA;
        try {
            result = rs.getString(colName);
        } catch (SQLException e) {
            // Ignore and use default value.
        }
        return result.trim();
    }

    private long parseLongWithNA(String longWithNA) {
        if (longWithNA.equalsIgnoreCase(SequenceAttribute.STR_NA)) {
            return 0L;
        }
        return Long.parseLong(longWithNA);
    }

    public long getSequenceNextVal(String seqName) throws Exception {
        long nextVal = -1;
        String sql = "select " + seqName + ".nextval";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            while (rs.next()) {
                nextVal = rs.getLong(seqName + ".nextval");
            }
        } finally {
            JdbcUtil.close(rs);
        }
        return nextVal;
    }

    public void dropSequence(String seqName) {
        String sql = "drop sequence " + seqName;
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    /**
     * 利用full hint到所有分库执行show index, 得到index的个数
     */
    public int getIndexNumInAllDb(String tableName, String indexName) {
        String sql = String
            .format("/*+TDDL({'type':'full','vtab':'%s','extra':{'MERGE_CONCURRENT':'FALSE'}})*/show index from %s",
                getSimpleTableName(tableName),
                tableName);
        int num = 0;
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                if (rs.getString("Key_name").equals(indexName)) {
                    num++;
                }
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }
        return num;
    }

    public void showDataBase(String tableName) {
        String sql = "drop table " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    public int getPrimaryIndexNumInAllDb(String tableName, String columnName) {
        String sql = String
            .format("/*+TDDL({'type':'full','vtab':'%s','extra':{'MERGE_CONCURRENT':'FALSE'}})*/show index from %s",
                getSimpleTableName(tableName),
                tableName);
        int num = 0;
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                String column = rs.getString("Column_name");

                if (rs.getString("Key_name").equals("PRIMARY") && column.equals(columnName)) {
                    num++;
                }
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }
        return num;
    }

    public int getIndexCount(String tableName) {
        String sql = String.format("show index from %s", tableName);
        int count = 0;
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                count++;
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }

        return count;
    }

    public boolean checkSeqExists(String schema, String seqName, boolean isNewSeq) {
        boolean seqExists = false;

        String schemaName = schema;
        String seq = seqName;
        if (seqName.contains(".")) {
            schemaName = TStringUtil.remove(seqName.split("\\.")[0], "`");
            seq = TStringUtil.remove(seqName.split("\\.")[1], "`");
        }

        String sql = "select count(*) from ";
        sql = META_DB_HINT + sql;

        if (isNewSeq) {
            sql += "sequence_opt ";
        } else {
            sql += "sequence ";
        }

        sql += "where name = '" + seq + "'";
        sql += " and schema_name = '" + schemaName + "'";

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);

        try {
            if (rs.next()) {
                seqExists = rs.getInt(1) == 1;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail(e.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return seqExists;
    }

    /**
     * 判断sequence或者sequence_opt标有无这条记录
     */
    public boolean isExistInSequence(String seqName, String sequenceTable) {
        Connection conn = seqName.contains(".") ? getPolardbxConnection2() : tddlConnection;

        String schema = tddlDatabase1;
        String simpleSeqName = seqName;
        if (seqName.contains(".")) {
            schema = TStringUtil.remove(seqName.split("\\.")[0], "`");
            simpleSeqName = TStringUtil.remove(seqName.split("\\.")[1], "`");
        }

        String sql = String.format(
            "%s select * from %s where schema_name='%s' and name='%s'", META_DB_HINT, sequenceTable,
            schema, simpleSeqName);

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);

        try {
            return rs.next();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * 正常的非边界值的情况下对不同类型的sequence做粗粒度的检测
     */
    public void simpleCheckSequence(String seqName, String seqType) throws Exception {
        TestSequence sequence = showSequence(seqName);

        String schemaPrefix = "";
        String simpleSeqName = seqName;
        if (seqName.contains(".")) {
            schemaPrefix = seqName.split("\\.")[0] + ".";
            simpleSeqName = seqName.split("\\.")[1];
        }

        if (seqType.equalsIgnoreCase("simple") || seqType.equalsIgnoreCase("by simple")) {
            // 先判断表结构
            assertThat(isExistInSequence(seqName, "sequence")).isFalse();
            assertThat(isExistInSequence(seqName, "sequence_opt")).isTrue();
            //assertNotExistsTable(schemaPrefix + "sequence_opt_mem_" + simpleSeqName, tddlConnection);

            // 粗判断show sequence结果
            assertThat(sequence.getStartWith()).isNotEqualTo(0);
            assertThat(sequence.getMaxValue()).isNotEqualTo(0);
            assertThat(sequence.getIncrementBy()).isNotEqualTo(0);
            assertThat(sequence.getValue()).isAtLeast(sequence.getStartWith());
            assertThat(sequence.getCycle()).isAnyOf(SequenceAttribute.STR_YES, SequenceAttribute.STR_NO);

            // 取下一个值,判断值正常变化
            long nextVal = getSequenceNextVal(seqName);
            assertThat(sequence.getValue()).isEqualTo(nextVal);
            nextVal = getSequenceNextVal(seqName);
            assertThat(nextVal).isEqualTo(sequence.getValue() + sequence.getIncrementBy());
            sequence = showSequence(seqName);
            assertThat(sequence.getValue()).isEqualTo(nextVal + sequence.getIncrementBy());

        } else if (seqType.equalsIgnoreCase("") || seqType.contains("group")) {
            assertThat(isExistInSequence(seqName, "sequence")).isTrue();
            assertThat(isExistInSequence(seqName, "sequence_opt")).isFalse();
            //assertNotExistsTable(schemaPrefix + "sequence_opt_mem_" + simpleSeqName, tddlConnection);

            // 粗判断sequence
            assertThat(sequence.getStartWith()).isEqualTo(0);
            assertThat(sequence.getMaxValue()).isEqualTo(0);
            assertThat(sequence.getIncrementBy()).isEqualTo(0);

            // 取下一个值,再判断sequence
            getSequenceNextVal(seqName);
            sequence = showSequence(seqName);
            assertThat(sequence.getStartWith()).isEqualTo(0);
            assertThat(sequence.getCycle()).isEqualTo(SequenceAttribute.STR_NA);
            assertThat(sequence.getValue()).isAtLeast(100000L);

        } else if (seqType.contains("simple with cache")) {
            assertThat(isExistInSequence(seqName, "sequence")).isFalse();
            assertThat(isExistInSequence(seqName, "sequence_opt")).isTrue();
            //assertExistsTable(schemaPrefix + "sequence_opt_mem_" + simpleSeqName, tddlConnection);

            assertThat(sequence.getStartWith()).isNotEqualTo(0);
            assertThat(sequence.getMaxValue()).isNotEqualTo(0);
            assertThat(sequence.getIncrementBy()).isNotEqualTo(0);
            assertThat(sequence.getValue()).isAtLeast(sequence.getStartWith());
            assertThat(sequence.getCycle()).isAnyOf(SequenceAttribute.STR_YES, SequenceAttribute.STR_NO);

            // //取下一个值,判断值正常变化
            // long nextVal = getSequenceNextVal(seqName);
            // assertThat(sequence.getValue()).isAnyOf(nextVal,
            // sequence.getStartWith() + 100000);
            // nextVal = getSequenceNextVal(seqName);
            // assertThat(nextVal).isEqualTo(sequence.getValue() +
            // sequence.getIncrementBy());
            // sequence = showSequence(seqName);
            // assertThat(sequence.getValue()).isEqualTo(sequence.getStartWith()
            // + 100000);

        } else if (seqType.contains("time")) {
            assertThat(isExistInSequence(seqName, "sequence")).isFalse();
            assertThat(isExistInSequence(seqName, "sequence_opt")).isTrue();
            //assertNotExistsTable(schemaPrefix + "sequence_opt_mem_" + simpleSeqName, tddlConnection);

            assertThat(sequence.getStartWith()).isEqualTo(0);
            assertThat(sequence.getMaxValue()).isEqualTo(0);
            assertThat(sequence.getIncrementBy()).isEqualTo(0);
            assertThat(sequence.getCycle()).isEqualTo(SequenceAttribute.STR_NA);

            long nextVal1 = getSequenceNextVal(seqName);
            long nextVal2 = getSequenceNextVal(seqName);
            assertThat(nextVal1).isLessThan(nextVal2);

        } else {

            assertWithMessage("sequence 模式不正确,无法判断").fail();
        }

    }

    public boolean isSpecialSequence(String seqType) {
        return seqType.toLowerCase().contains("time") || seqType.toLowerCase().contains("group");
    }

    public void assertNoSequenceExist(String seqName) {
        assertThat(isExistInSequence(seqName, "sequence")).isFalse();
        assertThat(isExistInSequence(seqName, "sequence_opt")).isFalse();

        String tableName = "sequence_opt_mem_" + seqName;
        if (seqName.contains(".")) {
            String schemaPrefix = seqName.split("\\.")[0] + ".";
            String simpleSeqName = seqName.split("\\.")[1];
            tableName = schemaPrefix + "sequence_opt_mem_" + simpleSeqName;
        }
        assertNotExistsTable(tableName, tddlConnection);
    }

    /**
     * 通过explain语句获取分库分表的总个数，分库*每个分库上分表的个数, 默认用select * 语句
     */
    public int getShardNum(String tableName) {
        return getResultNum("show topology from " + tableName);
    }

    public int getResultNum(String sql) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            return JdbcUtil.resultsSize(rs);
        } finally {
            JdbcUtil.close(rs);
        }

    }

    private static final String GSI_SCHEMA = "test";

    protected static ShowIndexChecker getShowIndexGsiChecker(Connection conn, String tableName) {
        final String sql = String.format("show index from %s", tableName);
        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(conn, sql)) {
            final List<IndexRecord> rows = new ArrayList<>();
            while (resultSet.next()) {
                final String indexType = resultSet.getString(11);
                // check gsi only
                if (!TStringUtil.equalsIgnoreCase(indexType, SqlIndexResiding.GLOBAL.name())) {
                    continue;
                }
                final String comment = resultSet.getString(12);
                final IndexColumnType indexColumnType = IndexColumnType.valueOf(comment);
                final SqlIndexResiding indexResiding = SqlIndexResiding.valueOf(indexType);
                final String indexName = resultSet.getString(3);

                rows.add(new IndexRecord(-1,
                    "def",
                    GSI_SCHEMA,
                    resultSet.getString(1),
                    resultSet.getInt(2) == 1,
                    GSI_SCHEMA,
                    indexName,
                    resultSet.getLong(4),
                    resultSet.getString(5),
                    resultSet.getString(6),
                    resultSet.getInt(7),
                    Optional.ofNullable(resultSet.getString(8)).map(Long::valueOf).orElse(null),
                    resultSet.getString(9),
                    resultSet.getString(10),
                    null,
                    comment,
                    resultSet.getString(13),
                    indexColumnType.getValue(),
                    indexResiding.getValue(),
                    indexName,
                    IndexStatus.PUBLIC.getValue(),
                    0l, 0l,
                    IndexVisibility.VISIBLE.getValue()));
            }
            Assert.assertFalse(rows.isEmpty());
            return new ShowIndexChecker(rows);
        } catch (Exception e) {
            throw new RuntimeException("show create table failed!", e);
        }
    }

    private static void initCreateTableWithGsi(SqlCreateTable expected) {
        // add pk and sharding key to covering
        final Set<String> tmpExtraColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        SqlCreateTable.getShardingKeys(expected.getDbpartitionBy(), tmpExtraColumns, false);
        SqlCreateTable.getShardingKeys(expected.getTbpartitionBy(), tmpExtraColumns, false);

        if (null != expected.getPrimaryKey()) {
            // primary key specified with key definition
            expected.getPrimaryKey().getColumns().forEach(s -> tmpExtraColumns.add(s.getColumnNameStr()));
        } else {
            // primary key specified in column definition
            expected.getColDefs()
                .stream()
                .filter(s -> s.getValue().getSpecialIndex() == SpecialIndex.PRIMARY)
                .forEach(t -> tmpExtraColumns.add(t.getKey().getLastName()));
        }

        final Set<String> extraColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        expected.getColDefs().forEach(s -> {
            if (tmpExtraColumns.contains(s.getKey().getLastName())) {
                extraColumns.add(s.getKey().getLastName().toLowerCase());
            }
        });

        if (null != expected.getGlobalKeys()) {
            expected.setGlobalKeys(expected.getGlobalKeys()
                .stream()
                .map(s -> {
                    final Set<String> tmpCovering = mergeAndSortCovering(expected, extraColumns, s.getValue()
                        .getCovering(), s.getValue().getColumns());
                    return Pair.of(s.getKey(), s.getValue().replaceCovering(tmpCovering));
                })
                .collect(Collectors.toList()));
        }
        if (null != expected.getGlobalUniqueKeys()) {
            expected.setGlobalUniqueKeys(expected.getGlobalUniqueKeys()
                .stream()
                .map(s -> {
                    final Set<String> tmpCovering = mergeAndSortCovering(expected, extraColumns, s.getValue()
                        .getCovering(), s.getValue().getColumns());
                    return Pair.of(s.getKey(), s.getValue().replaceCovering(tmpCovering));
                })
                .collect(Collectors.toList()));
        }
        // Clustered.
        if (null != expected.getClusteredKeys()) {
            expected.setClusteredKeys(expected.getClusteredKeys()
                .stream()
                .map(s -> {
                    final Set<String> tmpCovering = mergeAndSortCovering(expected, extraColumns, s.getValue()
                        .getCovering(), s.getValue().getColumns());
                    return Pair.of(s.getKey(), s.getValue().replaceCovering(tmpCovering));
                })
                .collect(Collectors.toList()));
        }
        if (null != expected.getClusteredUniqueKeys()) {
            expected.setClusteredUniqueKeys(expected.getClusteredUniqueKeys()
                .stream()
                .map(s -> {
                    final Set<String> tmpCovering = mergeAndSortCovering(expected, extraColumns, s.getValue()
                        .getCovering(), s.getValue().getColumns());
                    return Pair.of(s.getKey(), s.getValue().replaceCovering(tmpCovering));
                })
                .collect(Collectors.toList()));
        }
    }

    private static Set<String> mergeAndSortCovering(SqlCreateTable expected, Set<String> extraColumns,
                                                    List<SqlIndexColumnName> currentCovering,
                                                    List<SqlIndexColumnName> indexColumns) {
        final Set<String> ic = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumns.stream().map(SqlIndexColumnName::getColumnNameStr).forEach(ic::add);

        final Set<String> tmpCovering = new LinkedHashSet<>();
        if (null != currentCovering) {
            // keep all origin covering
            final Set<String> coveringSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            currentCovering.stream().map(SqlIndexColumnName::getColumnNameStr).forEach(coveringSet::add);
            expected.getColDefs().forEach(t -> {
                final String columnName = t.getKey().getLastName();
                if (coveringSet.contains(columnName)
                    || (!ic.contains(columnName) && extraColumns.contains(columnName))) {
                    tmpCovering.add(columnName.toLowerCase());
                }
            });
        } else {
            expected.getColDefs().forEach(t -> {
                final String columnName = t.getKey().getLastName();
                if (!ic.contains(columnName) && extraColumns.contains(columnName)) {
                    tmpCovering.add(columnName.toLowerCase());
                }
            });
        }
        return tmpCovering;
    }

    protected static void checkCreateTableExecute(Connection tddlConnection, final String sqlCreateTable,
                                                  String primaryTable) {
        checkCreateTableExecute(tddlConnection, sqlCreateTable, primaryTable, true, Litmus.THROW);
    }

    protected static void checkCreateTableExecute(Connection tddlConnection, final String sqlCreateTable,
                                                  String primaryTable, boolean doCreate, Litmus litmus) {
        if (doCreate) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable);
        }

        final TableChecker tableChecker = getTableChecker(tddlConnection, primaryTable);

        tableChecker.identicalTableDefinitionTo(sqlCreateTable, true, litmus);
    }

    protected static TableChecker getTableChecker(Connection conn, String tableName) {
        final String sql = String.format("show create table %s", tableName);
        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(conn, sql)) {
            Assert.assertTrue(resultSet.next());
            final String createTableStr = resultSet.getString(2);
            return TableChecker.buildTableChecker(createTableStr);
        } catch (Exception e) {
            throw new RuntimeException("show create table failed!", e);
        }
    }

    protected static TableChecker getTableChecker(SqlCreateTable sqlCreateTable) {
        return new TableChecker(sqlCreateTable);
    }

    public static class TableChecker {

        private final SqlCreateTable createTable;

        private TableChecker(SqlCreateTable createTable) {
            this.createTable = createTable;
        }

        public static TableChecker buildTableChecker(String createTableStr) {
            final SqlCreateTable createTable = (SqlCreateTable) fastsqlParser.parse(createTableStr).get(0);
            return new TableChecker(createTable);
        }

        public boolean columnNotExistsInGsi(String columnName) {
            return !columnExistsInGsi(columnName);
        }

        public boolean columnExistsInGsi(String columnName) {
            return Optional.ofNullable(createTable.getGlobalKeys())
                .orElse(ImmutableList.of())
                .stream()
                .anyMatch(s -> columnExists(columnName, s.getValue().getColumns())
                    || columnExists(columnName, s.getValue().getCovering()));
        }

        public boolean columnNotExistsInGsiIndex(String columnName) {
            return !columnExistsInGsiIndex(columnName);
        }

        public boolean columnExistsInGsiIndex(String columnName) {
            return Optional.ofNullable(createTable.getGlobalKeys())
                .orElse(ImmutableList.of())
                .stream()
                .anyMatch(s -> columnExists(columnName, s.getValue().getColumns()));
        }

        public boolean columnNotExistsInGsiCovering(String columnName) {
            return !columnExistsInGsiCovering(columnName);
        }

        public boolean columnExistsInGsiCovering(String columnName) {
            return Optional.ofNullable(createTable.getGlobalKeys())
                .orElse(ImmutableList.of())
                .stream()
                .anyMatch(s -> columnExists(columnName, s.getValue().getCovering()));
        }

        public boolean columnNotExistsInGsiIndex(String indexName, String columnName) {
            return !columnExistsInGsiIndex(indexName, columnName);
        }

        public boolean columnExistsInGsiIndex(String indexName, String columnName) {
            return columnExistsInGsiIndex(indexName, columnName, createTable.getGlobalKeys());
        }

        public boolean columnNotExistsInGusiIndex(String indexName, String columnName) {
            return !columnExistsInGusiIndex(indexName, columnName);
        }

        public boolean columnExistsInGusiIndex(String indexName, String columnName) {
            return columnExistsInGsiIndex(indexName, columnName, createTable.getGlobalUniqueKeys());
        }

        public boolean columnNotExistsInGsiCovering(String indexName, String columnName) {
            return !columnExistsInGsiCovering(indexName, columnName);
        }

        public boolean columnExistsInGsiCovering(String indexName, String columnName) {
            return columnExistsInGsiCovering(indexName, columnName, createTable.getGlobalKeys());
        }

        public boolean columnNotExistsInGusiCovering(String indexName, String columnName) {
            return !columnExistsInGusiCovering(indexName, columnName);
        }

        public boolean columnExistsInGusiCovering(String indexName, String columnName) {
            return columnExistsInGsiCovering(indexName, columnName, createTable.getGlobalUniqueKeys());
        }

        private boolean columnExistsInGsiCovering(String indexName, String columnName,
                                                  List<Pair<SqlIdentifier, SqlIndexDefinition>> gsis) {
            return Optional.ofNullable(gsis)
                .orElse(ImmutableList.of())
                .stream()
                .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName)
                    && columnExists(columnName, s.getValue().getCovering()));
        }

        private boolean columnExistsInGsiIndex(String indexName, String columnName,
                                               List<Pair<SqlIdentifier, SqlIndexDefinition>> gsis) {
            return Optional.ofNullable(gsis)
                .orElse(ImmutableList.of())
                .stream()
                .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName)
                    && columnExists(columnName, s.getValue().getColumns()));
        }

        private boolean columnExists(String columnName, List<SqlIndexColumnName> indexColumnNames) {
            return Optional.ofNullable(indexColumnNames)
                .orElse(ImmutableList.of())
                .stream()
                .anyMatch(s -> TStringUtil.equalsIgnoreCase(s.getColumnNameStr(), columnName));
        }

        public boolean columnNotExists(String columnName) {
            return !columnExists(columnName);
        }

        public boolean columnExists(String columnName) {
            return createTable.getColDefs()
                .stream()
                .anyMatch(s -> TStringUtil.equalsIgnoreCase(s.getKey().getLastName(), columnName));
        }

        public boolean indexNotExists(String indexName, boolean isGlobal) {
            return !indexExists(indexName, isGlobal);
        }

        public boolean indexExists(String indexName, boolean isGlobal) {
            if (isGlobal) {
                return Optional.ofNullable(createTable.getGlobalKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName))
                    || Optional.ofNullable(createTable.getGlobalUniqueKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName))
                    || Optional.ofNullable(createTable.getClusteredKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName))
                    || Optional.ofNullable(createTable.getClusteredUniqueKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName));
            } else {
                if (Optional.ofNullable(createTable.getUniqueKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName))) {
                    return true;
                }

                if (Optional.ofNullable(createTable.getKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName))) {
                    return true;
                }

                if (Optional.ofNullable(createTable.getFullTextKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName))) {
                    return true;
                }

                if (Optional.ofNullable(createTable.getSpatialKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName))) {
                    return true;
                }

                return Optional.ofNullable(createTable.getForeignKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> TStringUtil.equals(s.getKey().getLastName(), indexName));
            }
        }

        public boolean indexNotExists(Set<String> columns, Set<String> coverings, boolean isGlobal) {
            return !indexExists(columns, coverings, isGlobal);
        }

        public boolean indexExists(Set<String> columns, Set<String> coverings, boolean isGlobal) {
            if (isGlobal) {
                return Optional.ofNullable(createTable.getGlobalKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> identicalColumns(columns, coverings, s))
                    || Optional.ofNullable(createTable.getGlobalUniqueKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(t -> identicalColumns(columns, coverings, t))
                    || Optional.ofNullable(createTable.getClusteredKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(t -> identicalColumns(columns, coverings, t))
                    || Optional.ofNullable(createTable.getClusteredUniqueKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(t -> identicalColumns(columns, coverings, t));
            } else {
                if (Optional.ofNullable(createTable.getUniqueKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> identicalColumns(columns, s.getValue().getColumns()))) {
                    return true;
                }

                if (Optional.ofNullable(createTable.getKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> identicalColumns(columns, s.getValue().getColumns()))) {
                    return true;
                }

                if (Optional.ofNullable(createTable.getFullTextKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> identicalColumns(columns, s.getValue().getColumns()))) {
                    return true;
                }

                if (Optional.ofNullable(createTable.getSpatialKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> identicalColumns(columns, s.getValue().getColumns()))) {
                    return true;
                }

                return Optional.ofNullable(createTable.getForeignKeys())
                    .orElse(ImmutableList.of())
                    .stream()
                    .anyMatch(s -> identicalColumns(columns, s.getValue().getColumns()));
            }
        }

        private boolean identicalColumns(Set<String> columns, Set<String> coverings,
                                         Pair<SqlIdentifier, SqlIndexDefinition> s) {
            return identicalColumns(columns, s.getValue().getColumns())
                && identicalColumns(coverings, s.getValue().getCovering());
        }

        private boolean identicalColumns(Set<String> columns, List<SqlIndexColumnName> columnNames) {
            columnNames = Optional.ofNullable(columnNames).orElse(ImmutableList.of());
            final Set<String> finalColumns = Optional.ofNullable(columns).orElse(ImmutableSet.of());
            return columnNames.size() == finalColumns.size()
                && columnNames.stream().allMatch(t -> finalColumns.contains(t.getColumnNameStr()));
        }

        public boolean identicalTableDefinitionTo(String expectedCreateTable, boolean doInit, Litmus litmus) {
            final SqlCreateTable expected = (SqlCreateTable) fastsqlParser.parse(expectedCreateTable).get(0);
            final SqlCreateTable actual = this.createTable;

            if (doInit) {
                initCreateTableWithGsi(expected);
            }

            litmus.check(identicalColumnDefList(expected.getColDefs(), actual.getColDefs()),
                "unmatched column definition list");

            litmus.check(identicalIndexDefList(expected.getGlobalKeys(), actual.getGlobalKeys()),
                "unmatched gsi definition list");

            litmus.check(identicalIndexDefList(expected.getGlobalUniqueKeys(), actual.getGlobalUniqueKeys()),
                "unmatched gusi definition list");

            litmus.check(identicalIndexDefList(expected.getClusteredKeys(), actual.getClusteredKeys()),
                "unmatched cgsi definition list");

            litmus.check(identicalIndexDefList(expected.getClusteredUniqueKeys(), actual.getClusteredUniqueKeys()),
                "unmatched cgusi definition list");
            return true;
        }

        public boolean identicalTableDefinitionAndKeysTo(String expectedCreateTable, boolean doInit, Litmus litmus) {
            final SqlCreateTable expected = (SqlCreateTable) fastsqlParser.parse(expectedCreateTable).get(0);
            final SqlCreateTable actual = this.createTable;

            if (doInit) {
                initCreateTableWithGsi(expected);
            }

            litmus.check(identicalColumnDefList(expected.getColDefs(), actual.getColDefs()),
                "unmatched column definition list");

            litmus.check(identicalIndexDefList(expected.getGlobalKeys(), actual.getGlobalKeys()),
                "unmatched gsi definition list");

            litmus.check(identicalIndexDefList(expected.getGlobalUniqueKeys(), actual.getGlobalUniqueKeys()),
                "unmatched gusi definition list");

            litmus.check(identicalIndexDefList(expected.getClusteredKeys(), actual.getClusteredKeys()),
                "unmatched cgsi definition list");

            litmus.check(identicalIndexDefList(expected.getClusteredUniqueKeys(), actual.getClusteredUniqueKeys()),
                "unmatched cgusi definition list");

            litmus.check(identicalIndexDefList(expected.getKeys(), actual.getKeys()),
                "unmatched keys definition list");

            return true;
        }

        /**
         * identical index with identical index order
         */
        public boolean identicalIndexDefList(List<Pair<SqlIdentifier, SqlIndexDefinition>> expected,
                                             List<Pair<SqlIdentifier, SqlIndexDefinition>> actual) {
            if (!GeneralUtil.sameSize(expected, actual)) {
                System.out.println("unmatched index def size: ");
                System.out.println("expected: " + (expected == null ? 0 : expected.size()));
                System.out.println("actually: " + (actual == null ? 0 : actual.size()));
                return false;
            }

            if (null == expected) {
                return true;
            }

            for (int i = 0; i < expected.size(); i++) {
                if (!identicalIndexDef(expected.get(i), actual.get(i))) {
                    System.out.println("unmatched index def: ");
                    System.out.println("expected: " + expected.get(i).toString());
                    System.out.println("actually: " + actual.get(i).toString());
                    return false;
                }
            }

            return true;
        }

        /**
         * identical index def
         */
        private boolean identicalIndexDef(Pair<SqlIdentifier, SqlIndexDefinition> expected,
                                          Pair<SqlIdentifier, SqlIndexDefinition> actual) {
            return expected.getKey().equalsDeep(actual.getKey(), Litmus.IGNORE, EqualsContext.DEFAULT_EQUALS_CONTEXT)
                && expected.getValue()
                .equalsDeep(actual.getValue(), Litmus.IGNORE, EqualsContext.DEFAULT_EQUALS_CONTEXT);
        }

        /**
         * identical columns with identical column order
         */
        private boolean identicalColumnDefList(List<Pair<SqlIdentifier, SqlColumnDeclaration>> expected,
                                               List<Pair<SqlIdentifier, SqlColumnDeclaration>> actual) {
            if (!GeneralUtil.sameSize(expected, actual)) {
                System.out.println("unmatched column def size: ");
                System.out.println("expected: " + (expected == null ? 0 : expected.size()));
                System.out.println("actually: " + (actual == null ? 0 : actual.size()));
                return false;
            }

            if (null == expected) {
                return true;
            }

            for (int i = 0; i < expected.size(); i++) {
                if (!identicalColumnDef(expected.get(i), actual.get(i))) {
                    System.out.println("unmatched column def: ");
                    System.out.println("expected: " + expected.get(i).toString());
                    System.out.println("actually: " + actual.get(i).toString());
                    return false;
                }
            }

            return true;
        }

        /**
         * identical column def
         */
        private boolean identicalColumnDef(Pair<SqlIdentifier, SqlColumnDeclaration> expected,
                                           Pair<SqlIdentifier, SqlColumnDeclaration> actual) {
            return expected.getKey().equalsDeep(actual.getKey(), Litmus.IGNORE, EqualsContext.DEFAULT_EQUALS_CONTEXT)
                && expected.getValue()
                .equalsDeep(actual.getValue(), Litmus.IGNORE, EqualsContext.DEFAULT_EQUALS_CONTEXT);
        }
    }

    public static class ShowIndexChecker {

        final List<IndexRecord> rows;

        public ShowIndexChecker(List<IndexRecord> rows) {
            this.rows = rows;
        }

        public boolean identicalToTableDefinition(String expectedCreateTable, boolean doInit, Litmus litmus) {
            final SqlCreateTable createTable = (SqlCreateTable) fastsqlParser.parse(expectedCreateTable).get(0);

            if (doInit) {
                initCreateTableWithGsi(createTable);
            }

            final Map<String, List<IndexRecord>> actual = rows.stream()
                .filter(row -> row.getIndexLocation() == SqlIndexResiding.GLOBAL.getValue())
                .collect(Collectors.groupingBy(IndexRecord::getIndexName));

            final int expectedSize = Optional.ofNullable(createTable.getGlobalKeys()).map(List::size).orElse(0)
                + Optional.ofNullable(createTable.getGlobalUniqueKeys()).map(List::size).orElse(0)
                + Optional.ofNullable(createTable.getClusteredKeys()).map(List::size).orElse(0)
                + Optional.ofNullable(createTable.getClusteredUniqueKeys()).map(List::size).orElse(0);
            final int actualSize = actual.size();
            Assert.assertEquals(MessageFormat.format("unmatched index meta count. expected: {0} actually: {1}",
                expectedSize,
                actualSize), expectedSize, actualSize);

            identicalIndexDefinition(createTable, createTable.getGlobalKeys(), actual, true, litmus);

            identicalIndexDefinition(createTable, createTable.getGlobalUniqueKeys(), actual, false, litmus);

            identicalIndexDefinition(createTable, createTable.getClusteredKeys(), actual, true, litmus);

            identicalIndexDefinition(createTable, createTable.getClusteredUniqueKeys(), actual, false, litmus);

            return true;
        }

        public void identicalIndexDefinition(SqlCreateTable createTable,
                                             List<Pair<SqlIdentifier, SqlIndexDefinition>> expectedGsiList,
                                             Map<String, List<IndexRecord>> actual, boolean nonUnique, Litmus litmus) {
            Optional.ofNullable(expectedGsiList).ifPresent(gsiList -> gsiList.forEach(gsi -> {
                litmus.check(actual.containsKey(gsi.getKey().getLastName()), "lack of gsi {0}", gsi.getKey()
                    .getLastName());

                final List<IndexRecord> actualIndexRecords = actual.get(gsi.getKey().getLastName())
                    .stream()
                    .sorted(Comparator.comparingLong(IndexRecord::getSeqInIndex))
                    .collect(Collectors.toList());

                final List<IndexRecord> expectedIndexRecords = buildIndexRecords(createTable, gsi, nonUnique).stream()
                    .sorted(Comparator.comparingLong(IndexRecord::getSeqInIndex))
                    .collect(Collectors.toList());

                Assert.assertEquals(MessageFormat.format("unmatched index meta for {0}. expected: {1} actually: {2}",
                    gsi.getKey().getLastName(),
                    expectedIndexRecords.size(),
                    actualIndexRecords.size()), expectedIndexRecords.size(), actualIndexRecords.size());

                final Iterator<IndexRecord> actualIt = actualIndexRecords.iterator();
                final Iterator<IndexRecord> expectedIt = expectedIndexRecords.iterator();

                while (actualIt.hasNext() && expectedIt.hasNext()) {
                    final IndexRecord actualRecord = actualIt.next();
                    final IndexRecord expectedRecord = expectedIt.next();

                    if (!equalTo(expectedRecord, actualRecord)) {
                        System.out.println("unmatched index meta: ");
                        System.out.println("expected: " + JSON.toJSONString(expectedIndexRecords));
                        System.out.println("actually: " + JSON.toJSONString(actualIndexRecords));
                        Assert.fail(MessageFormat.format("unmatched index meta for [{0}, {1}, {2}]",
                            expectedRecord.getTableName(),
                            expectedRecord.getIndexName(),
                            expectedRecord.getColumnName()));
                    }
                }
            }));
        }

        private boolean equalTo(IndexRecord ir1, IndexRecord ir2) {
            return Comparator.comparing(IndexRecord::getTableName)
                .thenComparing(IndexRecord::isNonUnique)
                .thenComparing(IndexRecord::getIndexName)
                .thenComparing(IndexRecord::getSeqInIndex, Comparator.nullsFirst(Long::compareTo))
                .thenComparing(IndexRecord::getColumnName)
                .thenComparing(IndexRecord::getCollation, Comparator.nullsFirst(String::compareTo))
                .thenComparing(IndexRecord::getSubPart, Comparator.nullsFirst(Long::compareTo))
                .thenComparing(IndexRecord::getPacked, Comparator.nullsFirst(String::compareTo))
                .thenComparing(IndexRecord::getNullable, Comparator.nullsFirst(String::compareTo))
                // cannot get index type by SHOW INDEX
                //.thenComparing(IndexRecord::getIndexType, Comparator.nullsFirst(String::compareTo))
                .thenComparing(IndexRecord::getComment, Comparator.nullsFirst(String::compareTo))
                .thenComparing(IndexRecord::getIndexComment, Comparator.nullsFirst(String::compareTo))
                .thenComparing(IndexRecord::getIndexColumnType, Comparator.nullsFirst(Long::compareTo))
                .thenComparing(IndexRecord::getIndexLocation, Comparator.nullsFirst(Long::compareTo))
                .thenComparing(IndexRecord::getIndexTableName, Comparator.nullsFirst(String::compareTo))
                .compare(ir1, ir2) == 0;
        }

        private List<IndexRecord> buildIndexRecords(SqlCreateTable createTable,
                                                    Pair<SqlIdentifier, SqlIndexDefinition> gsi, boolean nonUnique) {
            final Map<String, SqlColumnDeclaration> columnDefMap = Optional.ofNullable(createTable)
                .map(SqlCreateTable::getColDefs)
                .map(colDefs -> colDefs.stream().collect(Collectors.toMap(colDef -> colDef.getKey().getLastName(),
                    Pair::getValue)))
                .orElse(ImmutableMap.of());

            final SqlIndexDefinition indexDef = gsi.getValue();

            final String catalog = "def";
            // table name is case insensitive
            final String tableName = RelUtils.lastStringValue(createTable.getName()).toLowerCase();
            final String indexTableName = RelUtils.lastStringValue(indexDef.getIndexName());
            final List<SqlIndexColumnName> covering = GsiUtils.buildCovering(createTable, indexDef.getCovering());
            final String indexComment = indexDef.getOptions()
                .stream()
                .filter(option -> null != option.getComment())
                .findFirst()
                .map(option -> RelUtils.stringValue(option.getComment()))
                .orElse("");

            return GsiUtils.buildIndexRecord(indexDef,
                catalog,
                GSI_SCHEMA,
                tableName,
                indexTableName,
                covering,
                nonUnique,
                indexComment,
                columnDefMap,
                IndexStatus.PUBLIC);
        }

    }

    public static String getSimpleTableName(String tableName) {
        if (tableName.contains(".")) {
            return tableName.split("\\.")[1];
        }
        return tableName;
    }

    /**
     * "show tables like 'table_name'" can't do cross-schema query by
     * "show table like 'schema.table_name'" so here we just use different
     * connection
     */
    public boolean isShowTableExist(String tableName, String schema) {
        if (StringUtils.isBlank(schema)) {
            return DataValidator.isShowTableExist(tableName, tddlConnection);
        } else {
            return DataValidator.isShowTableExist(getSimpleTableName(tableName), getPolardbxConnection2());
        }
    }

    protected void gsiIntegrityCheck(String primary, String index, String drdsColumnList, String mysqlColumnList,
                                     boolean compareWithMySql) {

        /*
         * c_time, c_timestamp, c_year 中插入非法数据后，读取会出错（返回的值和预期不符或者因为 DRDS 调用了 getObject
         * 导致报错）, 需要使用 HINT 直接下推的方式来读取
         */
        final List<List<Object>> primaryData = JdbcUtil.loadDataFromPhysical(tddlConnection,
            primary,
            physical -> "/*+TDDL:node(" + physical.left + ")*/ select " + drdsColumnList + " from " + physical.right);
        final List<List<Object>> indexData = JdbcUtil.loadDataFromPhysical(tddlConnection,
            index,
            physical -> "/*+TDDL:node(" + physical.left + ")*/ select " + drdsColumnList + " from " + physical.right);
        List<String> cols = Arrays.stream(drdsColumnList.split(",")).map(String::trim).collect(Collectors.toList());
        resultSetContentSameAssert(cols, indexData, primaryData, false);

        if (!compareWithMySql) {
            return;
        }

        /*
         * 与 MySQL 对比，避免 DRDS 干扰
         */
        final ResultSet mysqlRs =
            JdbcUtil.executeQuery("select " + mysqlColumnList + " from " + primary, mysqlConnection);
        final List<List<Object>> mysqlData = JdbcUtil.getAllResult(mysqlRs, false, null, false, false);
        final List<List<Object>> indexData1 = JdbcUtil.loadDataFromPhysical(tddlConnection,
            index,
            physical -> "/*+TDDL:node(" + physical.left + ")*/ select " + mysqlColumnList + " from " + physical.right);
        cols = Arrays.stream(mysqlColumnList.split(",")).map(String::trim).collect(Collectors.toList());
        resultSetContentSameAssert(cols, indexData1, mysqlData, false);
    }

    protected List<Map<String, String>> showFullDDL() {
        List<Map<String, String>> result = new ArrayList<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show full ddl")) {
            while (rs.next()) {
                Map<String, String> row = new HashMap<>();
                int ncol = rs.getMetaData().getColumnCount();
                for (int i = 0; i < ncol; ++i) {
                    row.put(rs.getMetaData().getColumnName(i + 1), rs.getString(i + 1));
                }
                result.add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("show full ddl failed!", e);
        }
        return result;
    }

    public static void checkGsi(Connection tddlConnection, String gsiName) throws SQLException {
        checkGsi(tddlConnection, "", gsiName);
    }

    public static void checkGsi(Connection tddlConnection, String hint, String gsiName) throws SQLException {
        try (final ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            hint + " CHECK GLOBAL INDEX " + gsiName)) {
            int count = 0;
            while (rs.next()) {
                count++;

                final String details = rs.getString("DETAILS");

                if (count > 1 || !TStringUtil.startsWith(details, "OK")) {
                    com.alibaba.polardbx.common.utils.Assert
                        .fail("Unexpected check result: " + details + "\nExecute CHECK GLOBAL INDEX " + gsiName
                            + " for more information");
                }
            }
        }
    }

    /**
     * Assert that all selected data are routed correctly in this table.
     * Approach: select by its sharding keys and primary keys, if it has a
     * result, then it's right.
     *
     * @param tableName may be the base table or the index
     * @param selectedData result of "select *"
     * @param columnNames column names corresponding to selectedData
     */
    protected void assertRouteCorrectness(String tableName, List<List<Object>> selectedData, List<String> columnNames,
                                          List<String> shardingKeys) throws Exception {
        List<Integer> shardingColumnIndexes = shardingKeys.stream()
            .map(columnNames::indexOf)
            .collect(Collectors.toList());
        List<List<Object>> shardingValues = new ArrayList<>(selectedData.size());
        for (List<Object> row : selectedData) {
            List<Object> shardingValuesInRow = new ArrayList<>(shardingColumnIndexes.size());
            for (int index : shardingColumnIndexes) {
                Object value = row.get(index);
                if (value instanceof JdbcUtil.MyDate) {
                    value = ((JdbcUtil.MyDate) value).getDate();
                } else if (value instanceof JdbcUtil.MyNumber) {
                    value = ((JdbcUtil.MyNumber) value).getNumber();
                }
                shardingValuesInRow.add(value);
            }
            shardingValues.add(shardingValuesInRow);
        }

        String sql = String.format(Optional.ofNullable(hint).orElse("") + "select * from %s where ", tableName);
        for (int i = 0; i < shardingKeys.size(); i++) {
            String shardingKeyName = shardingKeys.get(i);
            if (i == shardingKeys.size() - 1) {
                sql += shardingKeyName + "=?";
            } else {
                sql += shardingKeyName + "=? and ";
            }
        }

        for (List<Object> row : shardingValues) {
            PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, row, tddlConnection);
            ResultSet tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
            Assert.assertTrue(tddlRs.next());
        }
    }

    public void assertTableMetaNotExists(String schemaName, String tableName, Connection metaDbConnection) {
        List<String> sqlTemplate = Lists.newArrayList(
            "select count(1) from tables where table_schema='%s' and table_name='%s'",
            "select count(1) from tables_ext where table_schema='%s' and table_name='%s'",
            "select count(1) from partitions where table_schema='%s' and table_name='%s'",
            "select count(1) from table_partitions where table_schema='%s' and table_name='%s'",
            "select count(1) from table_local_partitions where table_schema='%s' and table_name='%s'",
            "select count(1) from indexes where table_schema='%s' and table_name='%s'",
            "select count(1) from scheduled_jobs where table_schema='%s' and table_name='%s'",
            "select count(1) from fired_scheduled_jobs where table_schema='%s' and table_name='%s'"
        );

        List<String> sqlList =
            sqlTemplate.stream().map(e -> String.format(e, schemaName, tableName)).collect(Collectors.toList());
        String sql = Joiner.on(" union all ").join(sqlList);

        ResultSet rs = JdbcUtil.executeQuerySuccess(metaDbConnection, sql);
        try {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(0)).isEqualTo(0);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    protected String getRealGsiName(Connection tddlConnection, String tableName, String gsiName) {
        String sql = "show full create table " + tableName;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            if (rs.next()) {
                String fullCreateTable = rs.getString(2);
                for (String line : fullCreateTable.split("\n")) {
                    if (line.contains("`" + gsiName + "`")) {
                        return line.substring(line.indexOf("/*") + 2, line.indexOf("*/")).trim();
                    }
                }

            }
        } catch (SQLException e) {

        }
        return null;
    }

    public static void rollbackDdl(String schemaName, String tableName, Connection conn) {
        try {
            String state = findDdlStateByTable(schemaName, tableName, conn);
            Assert.assertEquals("PAUSED", state);
            String jobId = findDdlByTable(schemaName, tableName, conn);
            JdbcUtil.executeUpdateSuccess(conn, "rollback ddl " + jobId);
            state = findDdlStateByTable(schemaName, tableName, conn);
            Assert.assertTrue(state.equalsIgnoreCase("") || state.equalsIgnoreCase("ROLLBACK_COMPLETED"));
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    public static void execDdlWithRetry(String schemaName, String tableName, String ddl, Connection conn) {
        try {
            try {
                Statement stmt = conn.createStatement();
                stmt.execute(ddl);
            } catch (SQLException e) {
                String msg = e.getMessage();
                System.out.println(msg);
                if (!(msg.contains("Deadlock found") || msg.contains("check status") || msg.contains("Query timeout")
                    || msg.contains("ALGORITHM=INSTANT") || msg.contains(
                    "update Global Secondary Index meta failed!"))) {
                    throw e;
                }
                // retry 5 times
                int retryCnt = 0;
                while (true) {
                    try {
                        // get ddl job id
                        String jobId = findDdlByTable(schemaName, tableName, conn);
                        if (jobId.isEmpty()) {
                            if (!checkDdlResultByTable(schemaName, tableName, conn)) {
                                // Could have been rollback
                                JdbcUtil.executeUpdateSuccess(conn, ddl);
                            }
                        } else {
                            JdbcUtil.executeUpdateSuccess(conn, "continue ddl " + jobId);
                        }
                        break;
                    } catch (Throwable e1) {
                        if (StringUtils.containsIgnoreCase(e1.getMessage(), "in RUNNING state")) {
                            // already running, just wait to finish
                            while (true) {
                                String state = findDdlStateByTable(schemaName, tableName, conn);
                                if (state.isEmpty()) {
                                    // finish
                                    if (checkDdlResultByTable(schemaName, tableName, conn)) {
                                        // success
                                        return;
                                    } else {
                                        // rollback, retry
                                        break;
                                    }
                                } else if (!state.equalsIgnoreCase("RUNNING")) {
                                    // pause for some reason, retry
                                    break;
                                }
                                System.out.println("wait ddl " + ddl + " to finish");
                                Thread.sleep(1000);
                            }
                        }
                        retryCnt++;
                        if (retryCnt > 5) {
                            throw e1;
                        }
                        System.out.println("retry " + retryCnt + " " + e1.getMessage());
                        Thread.sleep(1000);
                    }
                }

            }
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }
    }

    private static boolean checkDdlResultByTable(String schemaName, String tableName, Connection conn)
        throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("show ddl result", conn);
        SortedMap<Long, String> results = new TreeMap<>();
        while (rs.next()) {
            if (rs.getString("SCHEMA_NAME").equalsIgnoreCase(schemaName) && rs.getString("OBJECT_NAME")
                .equalsIgnoreCase(tableName)) {
                results.put(rs.getLong("JOB_ID"), rs.getString("RESULT_TYPE"));
                break;
            }
        }
        if (!results.isEmpty()) {
            return results.get(results.lastKey()).equalsIgnoreCase("SUCCESS");
        }
        return false;
    }

    private static String findDdlByTable(String schemaName, String tableName, Connection conn) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("show ddl", conn);
        String jobId = "";
        while (rs.next()) {
            if (rs.getString("OBJECT_SCHEMA").equalsIgnoreCase(schemaName) && rs.getString("OBJECT_NAME")
                .equalsIgnoreCase(tableName)) {
                jobId = rs.getString("JOB_ID");
                break;
            }
        }
        return jobId;
    }

    private static String findDdlStateByTable(String schemaName, String tableName, Connection conn)
        throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("show ddl", conn);
        while (rs.next()) {
            if (rs.getString("OBJECT_SCHEMA").equalsIgnoreCase(schemaName) && rs.getString("OBJECT_NAME")
                .equalsIgnoreCase(tableName)) {
                return rs.getString("STATE");
            }
        }
        return "";
    }

    protected boolean useXproto() {
        return useXproto(tddlConnection);
    }

    protected static String getFirstColumnarIndexNameWithSuffix(Connection conn, String tableName) {
        final List<String> indexNameList = new ArrayList<>();
        try {
            final ResultSet rs =
                JdbcUtil.executeQuery(String.format("show columnar index from %s", tableName), conn);
            while (rs.next()) {
                indexNameList.add(rs.getString("INDEX_NAME"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        Truth.assertThat(indexNameList).isNotEmpty();
        Truth.assertThat(indexNameList).hasSize(1);

        return indexNameList.iterator().next();
    }

    protected void createCciSuccess(String sqlCreateCci) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, SKIP_WAIT_CCI_CREATION_HINT + sqlCreateCci);
    }

    protected void createCciFailed(String sqlCreateCci, String errorMessage1, String errorMessage2) {
        JdbcUtil.executeUpdateFailed(tddlConnection, SKIP_WAIT_CCI_CREATION_HINT + sqlCreateCci, errorMessage1,
            errorMessage2);
    }

    protected void createCciAsync(String sqlCreateCci) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, SKIP_WAIT_CCI_CREATION_HINT + HINT_PURE_MODE + sqlCreateCci);
    }

    protected void createCciAsync(Connection conn, String sqlCreateCci) {
        JdbcUtil.executeUpdateSuccess(conn, SKIP_WAIT_CCI_CREATION_HINT + HINT_PURE_MODE + sqlCreateCci);
    }

    protected void createCciWithErr(String sqlCreateCci, String error) {
        JdbcUtil.executeUpdateFailed(tddlConnection, SKIP_WAIT_CCI_CREATION_HINT + sqlCreateCci, error);
    }

    protected String failOnDdlTaskHint(@NotNull String taskName) {
        return String.format(HINT_DDL_FAIL_POINT_TMPL, FailPointKey.FP_FAIL_ON_DDL_TASK_NAME, taskName);
    }

    protected String getRealCciName(String tableName, String cciPrefix) {
        final String sql = String.format("show columnar index from %s;", tableName);

        try (final ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (rs.next()) {
                final String fullCciName = rs.getString(3);
                if (fullCciName.startsWith(cciPrefix)) {
                    return fullCciName;
                }
            }
        } catch (SQLException e) {
            logger.error(e);
        }
        return null;
    }

    @NotNull
    protected List<TablePartitionRecord> queryCciTablePartitionRecords(String realCciName) throws Exception {
        final String sqlQueryTablePartition =
            "select * from metadb.table_partitions where table_name = ? and tbl_type = 7";
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, realCciName);
        return MetaDbUtil.query(sqlQueryTablePartition, params, TablePartitionRecord.class, tddlConnection);
    }

    @NotNull
    protected List<TableGroupRecord> queryCciTgFromMetaDb(String realCciName) throws Exception {
        final String sqlQueryCciTg = "select b.* \n"
            + "from metadb.table_partitions a\n"
            + "join metadb.table_group b on a.group_id = b.id and a.table_schema = b.schema_name\n"
            + "where a.table_name = ? \n"
            + "and a.part_level = 0 \n";
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, realCciName);
        return MetaDbUtil.query(sqlQueryCciTg, params, TableGroupRecord.class, tddlConnection);
    }

    @NotNull
    protected List<Long> queryCciTgFromInformationSchema(String tgName) throws Exception {
        final String sqlQueryCciTg = "select * from information_schema.TABLE_GROUP where table_group_name = ?";
        final List<Object> params = new ArrayList<>();
        params.add(tgName);

        final List<Long> result = new ArrayList<>();
        try (final PreparedStatement ps = JdbcUtil.preparedStatementSet(sqlQueryCciTg, params, tddlConnection)) {
            final ResultSet rs = JdbcUtil.executeQuery(sqlQueryCciTg, ps);
            while (rs.next()) {
                result.add(rs.getLong(2));
            }
        }

        return result;
    }

    @NotNull
    public List<CdcDdlRecord> queryDdlRecordByJobId(Long ddlJobId) throws SQLException {
        final String sqlQueryDdlRecord =
            "select * from __cdc__." + CdcTableUtil.CDC_DDL_RECORD_TABLE + " where job_id = ?";

        try (PreparedStatement stmt = tddlConnection.prepareStatement(sqlQueryDdlRecord)) {
            stmt.setObject(1, ddlJobId);
            final ResultSet rs = stmt.executeQuery();
            final List<CdcDdlRecord> result = new ArrayList<>();
            while (rs.next()) {
                result.add(CdcDdlRecord.fill(rs));
            }
            return result;
        }
    }

    @NotNull
    public List<CdcDdlRecord> queryDdlRecordByCciName(String cciName) throws SQLException {
        final String sqlQueryDdlRecord =
            "select * from __cdc__." + CdcTableUtil.CDC_DDL_RECORD_TABLE + " where ddl_sql like ?";

        try (PreparedStatement stmt = tddlConnection.prepareStatement(sqlQueryDdlRecord)) {
            stmt.setObject(1, String.format("%%%s%%", cciName));
            final ResultSet rs = stmt.executeQuery();
            final List<CdcDdlRecord> result = new ArrayList<>();
            while (rs.next()) {
                result.add(CdcDdlRecord.fill(rs));
            }
            return result;
        }
    }

    @NotNull
    protected List<CdcDdlRecord> queryDdlRecordByDdlSql(String schemaName, String tableName, String ddlSql)
        throws SQLException {
        final List<CdcDdlRecord> result = new ArrayList<>();

        final String sql = String.format(
            "select * from __cdc__.%s where schema_name = ? and table_name = ? and ddl_sql like ?",
            CdcTableUtil.CDC_DDL_RECORD_TABLE);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            ps.setString(3, "%" + ddlSql + "%");

            final ResultSet resultSet = ps.executeQuery();

            while (resultSet.next()) {
                result.add(CdcDdlRecord.fill(resultSet));
            }
        }

        return result;
    }

    @NotNull
    protected List<CdcDdlRecord> queryDdlRecordByDdlSql(String schemaName, String ddlSql)
        throws SQLException {
        final List<CdcDdlRecord> result = new ArrayList<>();

        final String sql = String.format(
            "select * from __cdc__.%s where schema_name = ? and ddl_sql like ?",
            CdcTableUtil.CDC_DDL_RECORD_TABLE);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, "%" + ddlSql + "%");

            final ResultSet resultSet = ps.executeQuery();

            while (resultSet.next()) {
                result.add(CdcDdlRecord.fill(resultSet));
            }
        }

        return result;
    }

    protected List<ColumnarTableEvolutionRecord> queryLatestColumnarTableEvolutionRecordByDdlJobId(Long ddlJobId)
        throws SQLException {
        final List<ColumnarTableEvolutionRecord> result;
        try (final Connection metaConn = getMetaConnection()) {
            final TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaConn);
            result = tableInfoManager.queryColumnarTableEvolutionLatestByDdlJobId(ddlJobId);
        }

        return result;
    }

    protected List<ColumnarTableEvolutionRecord> queryColumnarTableEvolutionRecordByDdlJobId(Long ddlJobId)
        throws SQLException {
        final List<ColumnarTableEvolutionRecord> result;
        try (final Connection metaConn = getMetaConnection()) {
            final TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaConn);
            result = tableInfoManager.queryColumnarTableEvolutionByDdlJobId(ddlJobId);
        }

        return result;
    }

    protected List<ColumnarTableMappingRecord> queryColumnarTableMappingRecordByTableId(Long tableId)
        throws SQLException {
        final List<ColumnarTableMappingRecord> result;
        try (final Connection metaConn = getMetaConnection()) {
            final TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaConn);
            result = tableInfoManager.queryColumnarTableMapping(tableId);
        }

        return result;
    }

    protected List<ColumnarTableMappingRecord> queryDropColumnarTableMappingRecordByIndexName(String schemaName,
                                                                                               String tableName,
                                                                                               String indexName)
        throws SQLException {
        final List<ColumnarTableMappingRecord> result;
        try (final Connection metaConn = getMetaConnection()) {
            final ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            accessor.setConnection(metaConn);
            result = accessor.queryBySchemaTableIndexLike(schemaName, tableName, indexName + "%",
                ColumnarTableStatus.DROP.name());
        }

        return result;
    }

    protected void checkLatestColumnarSchemaEvolutionRecord(Long ddlJobId,
                                                            String schemaName,
                                                            String tableName,
                                                            String indexName,
                                                            DdlType ddlType,
                                                            ColumnarTableStatus cciTableStatus) throws SQLException {
        final List<ColumnarTableEvolutionRecord> columnarTableEvolutionRecords =
            queryLatestColumnarTableEvolutionRecordByDdlJobId(ddlJobId);
        Truth.assertThat(columnarTableEvolutionRecords).hasSize(1);
        Truth.assertThat(columnarTableEvolutionRecords.get(0).tableSchema).isEqualTo(schemaName);
        Truth.assertThat(columnarTableEvolutionRecords.get(0).tableName).isEqualTo(tableName);
        Truth.assertThat(columnarTableEvolutionRecords.get(0).indexName).startsWith(indexName);
        Truth.assertThat(columnarTableEvolutionRecords.get(0).ddlType).isEqualTo(ddlType.name());
        Truth.assertThat(columnarTableEvolutionRecords.get(0).columns).isNotEmpty();

        final List<ColumnarTableMappingRecord> columnarTableMappingRecords =
            queryColumnarTableMappingRecordByTableId(columnarTableEvolutionRecords.get(0).tableId);
        Truth.assertThat(columnarTableMappingRecords).hasSize(1);
        Truth.assertThat(columnarTableMappingRecords.get(0).tableSchema).isEqualTo(schemaName);
        Truth.assertThat(columnarTableMappingRecords.get(0).tableName).isEqualTo(tableName);
        Truth.assertThat(columnarTableMappingRecords.get(0).indexName).startsWith(indexName);
        Truth.assertThat(columnarTableMappingRecords.get(0).status).isEqualTo(cciTableStatus.name());
        Truth.assertThat(columnarTableMappingRecords.get(0).latestVersionId)
            .isEqualTo(columnarTableEvolutionRecords.get(0).versionId);
    }

    protected void checkLatestColumnarSchemaEvolutionRecordByDdlSql(String sqlDdl,
                                                                    String schemaName,
                                                                    String tableName,
                                                                    String indexName,
                                                                    DdlType ddlType,
                                                                    ColumnarTableStatus cciTableStatus)
        throws SQLException {
        final List<CdcDdlRecord> cdcDdlRecords = queryDdlRecordByDdlSql(schemaName, tableName, sqlDdl);
        Truth
            .assertWithMessage("No ddl record found for sql: %s ", sqlDdl)
            .that(cdcDdlRecords)
            .hasSize(1);

        final List<ColumnarTableEvolutionRecord> columnarTableEvolutionRecords =
            queryLatestColumnarTableEvolutionRecordByDdlJobId(cdcDdlRecords.get(0).getJobId());
        Truth.assertThat(columnarTableEvolutionRecords).hasSize(1);
        Truth.assertThat(columnarTableEvolutionRecords.get(0).tableSchema).isEqualTo(schemaName);
        Truth.assertThat(columnarTableEvolutionRecords.get(0).tableName).isEqualTo(tableName);
        Truth.assertThat(columnarTableEvolutionRecords.get(0).indexName).startsWith(indexName);
        Truth.assertThat(columnarTableEvolutionRecords.get(0).ddlType).isEqualTo(ddlType.name());
        Truth.assertThat(columnarTableEvolutionRecords.get(0).columns).isNotEmpty();

        final List<ColumnarTableMappingRecord> columnarTableMappingRecords =
            queryColumnarTableMappingRecordByTableId(columnarTableEvolutionRecords.get(0).tableId);
        Truth.assertThat(columnarTableMappingRecords).hasSize(1);
        Truth.assertThat(columnarTableMappingRecords.get(0).tableSchema).isEqualTo(schemaName);
        Truth.assertThat(columnarTableMappingRecords.get(0).tableName).isEqualTo(tableName);
        Truth.assertThat(columnarTableMappingRecords.get(0).indexName).startsWith(indexName);
        Truth.assertThat(columnarTableMappingRecords.get(0).status).isEqualTo(cciTableStatus.name());
        Truth.assertThat(columnarTableMappingRecords.get(0).latestVersionId)
            .isEqualTo(columnarTableEvolutionRecords.get(0).versionId);
    }

    protected void checkLatestColumnarMappingRecordByDropDbSql(String sqlDdl,
                                                               String schemaName,
                                                               String tableName,
                                                               String indexName,
                                                               DdlType ddlType,
                                                               ColumnarTableStatus cciTableStatus)
        throws SQLException {
        final List<CdcDdlRecord> cdcDdlRecords = queryDdlRecordByDdlSql(schemaName, sqlDdl);
        Truth
            .assertWithMessage("No ddl record found for sql: %s ", sqlDdl)
            .that(cdcDdlRecords)
            .hasSize(1);

        final List<ColumnarTableMappingRecord> columnarTableMappingRecords =
            queryDropColumnarTableMappingRecordByIndexName(schemaName, tableName, indexName);
        Truth.assertThat(columnarTableMappingRecords).hasSize(1);
        Truth.assertThat(columnarTableMappingRecords.get(0).tableSchema).isEqualTo(schemaName);
        Truth.assertThat(columnarTableMappingRecords.get(0).tableName).isEqualTo(tableName);
        Truth.assertThat(columnarTableMappingRecords.get(0).indexName).startsWith(indexName);
        Truth.assertThat(columnarTableMappingRecords.get(0).status).isEqualTo(cciTableStatus.name());
        Truth.assertThat(columnarTableMappingRecords.get(0).latestVersionId).isGreaterThan(-1);
    }

    protected void checkColumnarSchemaEvolutionRecordByDdlSql(String sqlDdl,
                                                              String schemaName,
                                                              String tableName,
                                                              List<String> indexName,
                                                              DdlType ddlType,
                                                              ColumnarTableStatus cciTableStatus)
        throws SQLException {
        final List<CdcDdlRecord> cdcDdlRecords = queryDdlRecordByDdlSql(schemaName, tableName, sqlDdl);
        Truth
            .assertWithMessage("No ddl record found for sql: %s ", sqlDdl)
            .that(cdcDdlRecords)
            .hasSize(1);

        final List<ColumnarTableEvolutionRecord> columnarTableEvolutionRecords =
            queryColumnarTableEvolutionRecordByDdlJobId(cdcDdlRecords.get(0).getJobId());
        for (int i = 0; i < columnarTableEvolutionRecords.size(); i++) {
            Truth.assertThat(columnarTableEvolutionRecords.get(i).tableSchema).isEqualTo(schemaName);
            Truth.assertThat(columnarTableEvolutionRecords.get(i).tableName).isEqualTo(tableName);
            Truth.assertThat(columnarTableEvolutionRecords.get(i).indexName).startsWith(indexName.get(i));
            Truth.assertThat(columnarTableEvolutionRecords.get(i).ddlType).isEqualTo(ddlType.name());
            Truth.assertThat(columnarTableEvolutionRecords.get(i).columns).isNotEmpty();

            final List<ColumnarTableMappingRecord> columnarTableMappingRecords =
                queryColumnarTableMappingRecordByTableId(columnarTableEvolutionRecords.get(i).tableId);
            Truth.assertThat(columnarTableMappingRecords.get(0).tableSchema).isEqualTo(schemaName);
            Truth.assertThat(columnarTableMappingRecords.get(0).tableName).isEqualTo(tableName);
            Truth.assertThat(columnarTableMappingRecords.get(0).indexName).startsWith(indexName.get(i));
            Truth.assertThat(columnarTableMappingRecords.get(0).status).isEqualTo(cciTableStatus.name());
            Truth.assertThat(columnarTableMappingRecords.get(0).latestVersionId)
                .isEqualTo(columnarTableEvolutionRecords.get(0).versionId);

        }
    }

    protected void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException ignored) {
        }
    }

    protected void rollbackDDL(JobInfo job) {
        String sql = String.format("rollback ddl %s", job.parentJob.jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        waitForSeconds(1);
    }

    protected static class JobInfo {
        public JobEntry parentJob;
        public List<JobEntry> subJobs;
    }

    protected static class JobEntry {
        public long jobId;
        public String state;
        public String traceId;
        public String responseNode;
    }

    protected JobInfo fetchCurrentJob(String expectedTableName) throws SQLException {
        JobInfo job = null;
        JobEntry parentJob = null;
        List<JobEntry> subJobs = new ArrayList<>();

        String sql = "show full ddl";
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                JobEntry currentJob = new JobEntry();

                String tableName = rs.getString("OBJECT_NAME");

                if (!TStringUtil.equalsIgnoreCase(tableName, expectedTableName)) {
                    continue;
                }

                currentJob.jobId = rs.getLong("JOB_ID");
                currentJob.state = rs.getString("STATE");
                currentJob.traceId = rs.getString("TRACE_ID");
                currentJob.responseNode = rs.getString("RESPONSE_NODE");

                if (currentJob.responseNode.contains("subjob")) {
                    subJobs.add(currentJob);
                } else if (parentJob == null) {
                    parentJob = currentJob;
                } else {
                    Assert.fail("Unexpected: found multiple parent jobs");
                }
            }
        }

        if (parentJob != null) {
            job = new JobInfo();
            job.parentJob = parentJob;
            job.subJobs = subJobs;
        }

        return job;
    }

    protected void checkJobState(String expected, Callable<JobInfo> jobInfoSupplier) throws Exception {
        JobInfo job = jobInfoSupplier.call();
        if (job == null) {
            Assert.fail("Not found any job");
        }
        if (!TStringUtil.equalsIgnoreCase(job.parentJob.state, expected)) {
            Assert.fail(String.format("Job %s has wrong state %s", job.parentJob.jobId, job.parentJob.state));
        }
    }

    protected void checkJobGone(String tableName) throws SQLException {
        JobInfo job = fetchCurrentJob(tableName);
        if (job != null) {
            Assert.fail(String.format("Job %s is still there in %s", job.parentJob.jobId, job.parentJob.state));
        }
    }

    protected void checkJobState(DdlState expected, String tableName) throws SQLException {
        try {
            checkJobState(expected.toString(), () -> fetchCurrentJob(tableName));
        } catch (SQLException e) {
            throw e;
        } catch (Exception ex) {
            throw new RuntimeException("", ex);
        }
    }

    protected JobInfo fetchCurrentJobUntil(DdlState expectedState, String tableName, int waitSeconds)
        throws SQLException, InterruptedException {
        JobInfo job;

        int count = 0;
        do {
            if (count > 0) {
                TimeUnit.SECONDS.sleep(1);
            }

            job = fetchCurrentJob(tableName);

            count++;
        } while ((job == null || !expectedState.name().equalsIgnoreCase(job.parentJob.state)) && count <= waitSeconds);

        return job;
    }

    @Nullable
    public String queryCciTgName() {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show tablegroup")) {
            while (rs.next()) {
                String name = rs.getString("TABLE_GROUP_NAME");
                if (TableGroupNameUtil.isColumnarTg(name)) {
                    return name;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    protected List<String> getStorageInstIds(String dbName) {
        String sql = String.format("show ds where db='%s'", dbName);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> storageInstIds = new ArrayList<>();
        try {
            while (rs.next()) {
                storageInstIds.add(rs.getString("STORAGE_INST_ID"));
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            Assert.fail(errorMs + " \n" + ex);
        }
        return storageInstIds;
    }

    protected void executeDdlAndCheckCdcRecord(String sqlDdl, String expectedDdlSql, String tableName,
                                               boolean withDdlId) throws SQLException {
        executeDdlAndCheckCdcRecord(sqlDdl,
            expectedDdlSql,
            tableName,
            cdcDdlRecord -> Truth.assertThat(cdcDdlRecord.ddlSql).ignoringCase().contains(expectedDdlSql),
            ddlExtInfo -> {
                if (withDdlId) {
                    Truth.assertThat(ddlExtInfo.getDdlId()).isGreaterThan(0);
                }
                Truth.assertThat(ddlExtInfo.getOriginalDdl()).ignoringCase().contains(sqlDdl);
            });
    }

    protected void executeDdlAndCheckCdcRecord(String sqlDdl, String expectedDdlSql, String tableName,
                                               Consumer<CdcDdlRecord> cdcDdlRecordConsumer,
                                               Consumer<DDLExtInfo> ddlExtInfoConsumer)
        throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlDdl);

        // Check cdc mark
        final List<CdcDdlRecord> ddlRecords = queryDdlRecordByDdlSql(getDdlSchema(), tableName, expectedDdlSql);
        Truth
            .assertWithMessage("No ddl record found for sql: %s \n expected: %s", sqlDdl, expectedDdlSql)
            .that(ddlRecords)
            .hasSize(1);
        cdcDdlRecordConsumer.accept(ddlRecords.get(0));
        final DDLExtInfo ddlExtInfo = JSONObject.parseObject(ddlRecords.get(0).ext, DDLExtInfo.class);
        ddlExtInfoConsumer.accept(ddlExtInfo);
    }

    protected void checkTraceRowCount(int rowCount) {
        checkTraceRowCount(rowCount, tddlConnection);
    }

    protected List<List<String>> checkTraceRowCountIs(int expectedTraceCount) {
        return checkTraceRowCount(Matchers.is(expectedTraceCount));
    }

    protected List<List<String>> checkTraceRowCount(Matcher<Integer> matcher) {
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat("Unexpected trace count: \n" + Optional
            .ofNullable(trace)
            .map(tu -> tu.stream().map(r -> String.join(", ", r)).collect(Collectors.joining("\n")))
            .orElse("show trace result is null"), trace.size(), matcher);

        return trace;
    }

    protected static @NotNull String buildSqlCheckData(List<String> columnNames, String tableName) {
        return "select " + String.join(",", columnNames) + " from " + tableName;
    }

    protected void executeOnceThenCheckDataAndTraceResultAndRouteCorrectness(String hint,
                                                                             String insert,
                                                                             List<String> columnNames,
                                                                             String tableName,
                                                                             Matcher<Integer> traceCountMatcher)
        throws SQLException {
        final List<List<Object>> mysqlResult = executeOnceThenCheckDataAndTraceResult(hint,
            insert,
            buildSqlCheckData(columnNames, tableName),
            traceCountMatcher);

        JdbcUtil.assertRouteCorrectness(hint,
            tableName,
            mysqlResult,
            columnNames,
            ImmutableList.of("c1"),
            tddlConnection);
    }

    protected @NotNull List<List<Object>> executeOnceThenCheckDataAndTraceResult(String hint,
                                                                                 String insert,
                                                                                 String sqlCheckData,
                                                                                 Matcher<Integer> traceCountMatcher) {

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null, true);
        checkTraceRowCount(traceCountMatcher);

        return selectContentSameAssert(sqlCheckData, null, mysqlConnection, tddlConnection);
    }

    protected void executeTwiceThenCheckDataAndTraceResult(String hint,
                                                           String insert,
                                                           String sqlCheckData,
                                                           Matcher<Integer> traceCountMatcher) {
        executeTwiceThenCheckDataAndTraceResult(hint, insert, sqlCheckData, false, traceCountMatcher);
    }

    protected void executeTwiceThenCheckDataAndTraceResult(String hint,
                                                           String insert,
                                                           String sqlCheckData,
                                                           boolean compareAffectedRows,
                                                           Matcher<Integer> traceCountMatcher) {
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert(sqlCheckData, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            compareAffectedRows);
        checkTraceRowCount(traceCountMatcher);

        selectContentSameAssert(sqlCheckData, null, mysqlConnection, tddlConnection);
    }

    protected void executeTwiceThenCheckGsiDataAndTraceResult(String hint,
                                                              String insert,
                                                              String tableName,
                                                              String gsiName,
                                                              int traceCount) throws SQLException {
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null, true);
        checkTraceRowCountIs(traceCount);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    protected void executeThriceThenCheckDataAndTraceResult(String hint,
                                                            String insert,
                                                            String sqlCheckData,
                                                            boolean compareAffectedRows,
                                                            Matcher<Integer> traceCountMatcher) {
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert(sqlCheckData, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            compareAffectedRows);
        checkTraceRowCount(traceCountMatcher);

        selectContentSameAssert(sqlCheckData, null, mysqlConnection, tddlConnection);
    }

    protected void executeThriceThenCheckGsiDataAndTraceResult(String hint,
                                                               String insert,
                                                               String tableName,
                                                               String gsiName,
                                                               boolean compareAffectedRows,
                                                               int traceCount) throws SQLException {
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + insert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + insert, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + hint + insert, null,
            compareAffectedRows);
        checkTraceRowCountIs(traceCount);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }
}

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
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
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
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlColumnDeclaration.SpecialIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlIndexDefinition.SqlIndexResiding;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;
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
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataValidator.resultSetContentSameAssert;
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

    private Connection createTddlDb(String db) {
        Connection connection = null;
        try {
            connection = super.getPolardbxConnection();
            if (!usingNewPartDb()) {
                JdbcUtil.createDatabase(connection, db, DB_HINT);
            } else {
                JdbcUtil.createPartDatabase(connection, db);
            }
        } catch (Throwable t) {
            //ignore
        }
        return connection;
    }

    private Connection createMysqlDb(String db) {
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
            String myDatabase1 = database1 + "_mysql";
            this.tddlConnection = createTddlDb(database1);
            this.mysqlConnection = createMysqlDb(myDatabase1);
            this.tddlDatabase1 = database1;
            this.mysqlDatabase1 = myDatabase1;
            this.infomationSchemaDbConnection = getMysqlConnection("information_schema");
        }
        return tddlConnection;
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
            JdbcUtil.dropDatabase(tddlConnection, tddlDatabase1);
            JdbcUtil.dropDatabase(mysqlConnection, mysqlDatabase1);
            this.tddlDatabase1 = null;
        }

        if (!StringUtils.isEmpty(tddlDatabase2)) {
            JdbcUtil.dropDatabase(tddlConnection2, tddlDatabase2);
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
                Set<String> storageAddress = getStorageAddressByGroupName(grpName);
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
        Set<String> storageAddress = getStorageAddressByGroupName(grpName);
        String phyDbName = groupInfos.groupAndPhyDbMaps.get(grpName);
        Connection shardDbConn = getMysqlConnectionByAddress(storageAddress, phyDbName);
        return shardDbConn;
    }

    private Set<String> getStorageAddressByGroupName(String grpName) {
        Set<String> address = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        try (Connection metaDbConn = ConnectionManager.getInstance().getDruidMetaConnection()) {
            JdbcUtil.useDb(metaDbConn, PropertiesUtil.getMetaDB);
            String instanceId = PropertiesUtil.configProp.getProperty("instanceId");
            try (Statement stmt = metaDbConn.createStatement()) {
                stmt.execute(String.format("select s.ip,s.port from group_detail_info d,storage_info s where "
                        + "d.storage_inst_id = s.storage_inst_id and  d.group_name = '%s' and d.inst_id = '%s'",
                    grpName, instanceId));
                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next()) {
                        String ip = rs.getString("ip");
                        String port = rs.getString("port");
                        address.add(ip + ":" + port);
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
        if (address.isEmpty()) {
            throw new RuntimeException("can`t find storage info for group " + grpName);
        }

        return address;
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
        int result = 0;

        Pair<String, Exception> failed = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            result = stmt.executeUpdate(insert);
        } catch (SQLSyntaxErrorException msee) {
            throw msee;
        } catch (SQLException e) {
            failed = Pair.of(insert, e);
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
                    assertWithMessage("DRDS/MySQL 错误不一致, Sql：\n " + failed.left).that(failed.right.getMessage())
                        .contains(msg);
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
                return rs.getString("Create Table").replace(" DEFAULT COLLATE = utf8mb4_0900_ai_ci", "");
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

    public int getNodeNum(Connection conn) {
        int count = 0;
        try (ResultSet rs = JdbcUtil.executeQuery("show node", conn)) {
            while (rs.next()) {
                count++;
            }
        } catch (Throwable e) {
            com.alibaba.polardbx.common.utils.Assert.fail();
        }
        return count;
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
                    0l, 0l));
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
            return expected.getKey().equalsDeep(actual.getKey(), Litmus.IGNORE)
                && expected.getValue().equalsDeep(actual.getValue(), Litmus.IGNORE);
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
            return expected.getKey().equalsDeep(actual.getKey(), Litmus.IGNORE)
                && expected.getValue().equalsDeep(actual.getValue(), Litmus.IGNORE);
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
        try (final ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "CHECK GLOBAL INDEX " + gsiName)) {
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

    public static void execDdlWithRetry(String schemaName, String tableName, String ddl, Connection conn) {
        try {
            try {
                Statement stmt = conn.createStatement();
                stmt.execute(ddl);
            } catch (SQLException e) {
                String msg = e.getMessage();
                System.out.println(msg);
                if (!(msg.contains("Deadlock found") || msg.contains("check status") || msg.contains(
                    "Query timeout"))) {
                    throw e;
                }
                // get ddl job id
                String jobId = findDdlByTable(schemaName, tableName, conn);
                if (jobId.isEmpty()) {
                    return;
                }
                // retry 5 times
                int retryCnt = 0;
                while (true) {
                    try {
                        JdbcUtil.executeUpdateSuccess(conn, "continue ddl " + jobId);
                        break;
                    } catch (Throwable e1) {
                        if (StringUtils.containsIgnoreCase(e1.getMessage(), "in RUNNING state")) {
                            // already running, just wait to finish
                            while (true) {
                                String state = findDdlStateByTable(schemaName, tableName, conn);
                                if (state.isEmpty()) {
                                    // finish
                                    return;
                                } else if (!state.equalsIgnoreCase("RUNNING")) {
                                    // pause for some reason, retry
                                    break;
                                }
                                System.out.println("wait ddl job " + jobId + " to finish");
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
}

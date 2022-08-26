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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.DefaultDBInfo;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.TableEntity;
import com.alibaba.polardbx.qatest.util.ConfigUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.BROADCAST_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_ONE_TB_SUFFIX;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

/**
 * 错误处理详细规则： 1. 源物理分表不为空，目标物理分表不为空：
 * 不管源表跟目标表的表结构是否一致，此时都需要抛出表已存在异常。（因为两个分表的数据可能不一致！） 2. 源物理分表不为空，目标物理分表为空：
 * 正常case，继续执行接下来的操作。 3. 源物理分表为空，目标物理分表不为空： 这种情形都当做是由于上一次执行RENAME
 * TABLE没有最终成功导致的（不考虑源表物理分表不完整，目标表物理分表已存在这种case）， 此时直接下推执行即可（忽略MySQL抛出的表已存在异常）。
 *
 * @author arnkore 2017-06-05 15:45
 */

public class RenameTableTest extends DDLBaseNewDBTestCase {

    private static final Logger logger = LoggerFactory.getLogger(RenameTableTest.class);

    private static final String ORIGIN_TABLE_PREFIX = "rename_origin_";

    private static final String TARGET_TABLE_PREFIX = "rename_target_";

    private static final String RECYCLE_BIN_TABLE_PREFIX = "rt_";

    private String originTableName;

    private String targetTableName;

    private Map<String, String> targetTableMap = new HashMap<String, String>();

    private static final String RECYCLE_BIN_HINT = "/!TDDL:ENABLE_RECYCLEBIN=true*/";
    private static final String ALLOW_ALTER_GSI_INDIRECTLY_HINT =
        "/*+TDDL:cmd_extra(ALLOW_ALTER_GSI_INDIRECTLY=true)*/";

    @Before
    public void initTarget() {
        targetTableMap.clear();

        String tbName = schemaPrefix + TARGET_TABLE_PREFIX + ONE_DB_ONE_TB_SUFFIX;
        targetTableMap.put(tbName, String.format("create table %s(id bigint(20))", tbName));

        tbName = schemaPrefix + TARGET_TABLE_PREFIX + ONE_DB_MUTIL_TB_SUFFIX;
        targetTableMap.put(tbName,
            String.format("create table %s(id bigint(20)) tbpartition by hash(id) tbpartitions 4", tbName));

        tbName = schemaPrefix + TARGET_TABLE_PREFIX + MULTI_DB_ONE_TB_SUFFIX;
        targetTableMap.put(tbName, String.format("create table %s(id bigint(20)) dbpartition by hash(id)", tbName));

        tbName = schemaPrefix + TARGET_TABLE_PREFIX + MUlTI_DB_MUTIL_TB_SUFFIX;
        targetTableMap.put(tbName,
            String
                .format("create table %s(id bigint(20)) dbpartition by hash(id) tbpartition by hash(id) tbpartitions 4",
                    tbName));

        tbName = schemaPrefix + TARGET_TABLE_PREFIX + BROADCAST_TB_SUFFIX;
        targetTableMap.put(tbName, String.format("create table %s(id bigint(20)) broadcast", tbName));
    }

    private Map<String, TableEntity> originTableEntityMap = Maps.newHashMap();
    private Map<String, TableEntity> recycleBinTableEntityMap = Maps.newHashMap();

    @Before
    public void initSource() {
        initSource(originTableEntityMap, ORIGIN_TABLE_PREFIX, new String[] {
            ONE_DB_ONE_TB_SUFFIX,
            ONE_DB_MUTIL_TB_SUFFIX, MULTI_DB_ONE_TB_SUFFIX, MUlTI_DB_MUTIL_TB_SUFFIX, BROADCAST_TB_SUFFIX});
        if (TStringUtil.isEmpty(schemaPrefix) && originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX)) {
            initSource(recycleBinTableEntityMap, RECYCLE_BIN_TABLE_PREFIX, new String[] {"a", "b", "c", "d", "e"});
        }
    }

    private void initSource(Map<String, TableEntity> tableEntityMap, String tableNamePrefix,
                            String[] tableNameSuffixes) {
        String tbPartitionKey = "pk", dbPartitionKey = "pk";
        int tbPartitionNum = 4;
        String tbName;

        tableEntityMap.clear();

        // 单库单表
        tbName = schemaPrefix + tableNamePrefix + tableNameSuffixes[0];
        TableEntity tableEntityOneDbOneTb = new TableEntity();
        tableEntityOneDbOneTb.setTbName(tbName);
        tableEntityOneDbOneTb.setColumnInfos(TableColumnGenerator.getAllTypeColum());
        tableEntityMap.put(tbName, tableEntityOneDbOneTb);

        // 单库多表
        tbName = schemaPrefix + tableNamePrefix + tableNameSuffixes[1];
        TableEntity tableEntityOneDbMultiTb = new TableEntity();
        tableEntityOneDbMultiTb.setTbName(tbName);
        tableEntityOneDbMultiTb.setColumnInfos(TableColumnGenerator.getAllTypeColum());
        tableEntityOneDbMultiTb.setTbpartitionKey(tbPartitionKey);
        tableEntityOneDbMultiTb.setTbpartitionNum(tbPartitionNum);
        tableEntityMap.put(tbName, tableEntityOneDbMultiTb);

        // 多库单表
        tbName = schemaPrefix + tableNamePrefix + tableNameSuffixes[2];
        TableEntity tableEntityMultiDbOneTb = new TableEntity();
        tableEntityMultiDbOneTb.setTbName(tbName);
        tableEntityMultiDbOneTb.setColumnInfos(TableColumnGenerator.getAllTypeColum());
        tableEntityMultiDbOneTb.setDbpartitionKey(dbPartitionKey);
        tableEntityMap.put(tbName, tableEntityMultiDbOneTb);

        // 多库多表
        tbName = schemaPrefix + tableNamePrefix + tableNameSuffixes[3];
        TableEntity tableEntityMultiDbMultiTb = new TableEntity();
        tableEntityMultiDbMultiTb.setTbName(tbName);
        tableEntityMultiDbMultiTb.setColumnInfos(TableColumnGenerator.getAllTypeColum());
        tableEntityMultiDbMultiTb.setDbpartitionKey(dbPartitionKey);
        tableEntityMultiDbMultiTb.setTbpartitionKey(tbPartitionKey);
        tableEntityMultiDbMultiTb.setTbpartitionNum(tbPartitionNum);
        tableEntityMap.put(tbName, tableEntityMultiDbMultiTb);

        // 广播表
        tbName = schemaPrefix + tableNamePrefix + tableNameSuffixes[4];
        TableEntity tableEntityBroadcast = new TableEntity();
        tableEntityBroadcast.setTbName(tbName);
        tableEntityBroadcast.setColumnInfos(TableColumnGenerator.getAllTypeColum());
        tableEntityBroadcast.setBroadcast(true);
        tableEntityMap.put(tbName, tableEntityBroadcast);
    }

    public RenameTableTest(String originTableName, String targetTableName, boolean schema) {
        this.crossSchema = schema;
        this.originTableName = originTableName;
        this.targetTableName = targetTableName;
    }

    @Parameterized.Parameters(name = "{index}:originTableName={0},targetTableName={1},schema={2}")
    public static List<Object[]> prepareData() {
        return Arrays.asList(ExecuteTableName.renameTableOfAllBaseType("rename"));
    }

    private void createOriginTable(String tableName) {
        TableEntity tableEntity = originTableEntityMap.get(tableName);
        createTDDLTable(tddlDatabase2, tableEntity, tddlConnection);
    }

    private void createTargetTable(String targetTableName) throws SQLException {
        String createSql = targetTableMap.get(targetTableName);
        Statement statement = JdbcUtil.createStatement(tddlConnection);
        statement.execute(createSql);
    }

    private void dropTargetTables() {
        String[] fiveBaseTypeNewTables = new String[] {
            TARGET_TABLE_PREFIX + ONE_DB_ONE_TB_SUFFIX,
            TARGET_TABLE_PREFIX + ONE_DB_MUTIL_TB_SUFFIX, TARGET_TABLE_PREFIX + MULTI_DB_ONE_TB_SUFFIX,
            TARGET_TABLE_PREFIX + MUlTI_DB_MUTIL_TB_SUFFIX, TARGET_TABLE_PREFIX + BROADCAST_TB_SUFFIX};

        if (crossSchema) {
            dropTables(fiveBaseTypeNewTables, tddlDatabase2 + ".");
        } else {
            dropTables(fiveBaseTypeNewTables, tddlDatabase1 + ".");
        }
    }

    private void dropOriginTables() {
        String[] fiveBaseTypeOldTables = new String[] {
            ORIGIN_TABLE_PREFIX + ONE_DB_ONE_TB_SUFFIX,
            ORIGIN_TABLE_PREFIX + ONE_DB_MUTIL_TB_SUFFIX, ORIGIN_TABLE_PREFIX + MULTI_DB_ONE_TB_SUFFIX,
            ORIGIN_TABLE_PREFIX + MUlTI_DB_MUTIL_TB_SUFFIX, ORIGIN_TABLE_PREFIX + BROADCAST_TB_SUFFIX};

        if (crossSchema) {
            dropTables(fiveBaseTypeOldTables, tddlDatabase2 + ".");
        } else {
            dropTables(fiveBaseTypeOldTables, tddlDatabase1 + ".");
        }
    }

    private void dropTables(String[] tables, String schemaPrefix) {
        if (tables != null) {
            try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
                for (String tableName : tables) {
                    String sql = "drop table if exists " + schemaPrefix + tableName;
                    Statement statement = JdbcUtil.createStatement(connection);
                    try {
                        statement.execute(sql);
                    } catch (SQLException e) {
                        String errorMs = "[Statement executeSuccess] failed";
                        logger.error(errorMs, e);
                    } finally {
                        JdbcUtil.close(statement);
                    }
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
    }

    @After
    public void staticDestroy() {
        dropOriginTables();
        dropTargetTables();
    }

    @Before
    public void init() {
        this.originTableName = schemaPrefix + originTableName;
        this.targetTableName = schemaPrefix + targetTableName;
        dropOriginTables();
        dropTargetTables();
        createOriginTable(originTableName);
        if (TStringUtil.isEmpty(schemaPrefix) && originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX)) {
            createRecycleBinTables();
        }
    }

    private void createRecycleBinTables() {
        for (String tableName : recycleBinTableEntityMap.keySet()) {
            TableEntity tableEntity = recycleBinTableEntityMap.get(tableName);
            createTDDLTable(tddlDatabase2, tableEntity, tddlConnection);
        }
    }

    private void dropRecycleBinTables() {
        String[] fiveRecyclebinTables = new String[] {
            RECYCLE_BIN_TABLE_PREFIX + "a", RECYCLE_BIN_TABLE_PREFIX + "b",
            RECYCLE_BIN_TABLE_PREFIX + "c", RECYCLE_BIN_TABLE_PREFIX + "d", RECYCLE_BIN_TABLE_PREFIX + "e"};

        if (crossSchema) {
            dropTables(fiveRecyclebinTables, tddlDatabase2 + ".");
        } else {
            dropTables(fiveRecyclebinTables, tddlDatabase1 + ".");
        }
    }

    @After
    public void destroy() throws SQLException {
        dropTableIfExists(originTableName);
        dropTableIfExists(targetTableName);
        if (TStringUtil.isEmpty(schemaPrefix) && originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX)) {
            dropRecycleBinTables();
        }
    }

    /**
     * case2 源物理分表不为空，目标物理分表为空: 正常case，继续执行接下来的操作。
     */
    @Test
    public void testNormalCase() throws SQLException {
        ShowCreateTableResult oldRes = execShowCreateTable(originTableName);

        String renameSql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "rename table %s to %s",
            originTableName,
            targetTableName);
        JdbcUtil.executeSuccess(tddlConnection, renameSql);
        assertTableNonexists(originTableName);

        ShowCreateTableResult newRes = execShowCreateTable(targetTableName);
        Assert.assertTrue(oldRes.tableMetaConsistent(newRes));

        createOriginTable(originTableName);

        ShowCreateTableResult origRes = execShowCreateTable(originTableName);
        Assert.assertTrue(oldRes.tableMetaConsistent(origRes));
    }

    /**
     * case 1 源物理分表不为空，目标物理分表不为空：
     * 不管源表跟目标表的表结构是否一致，此时都需要抛出表已存在异常。（因为两个分表的数据可能不一致！）
     */
    @Test
    public void testTableAlreadyExists() throws SQLException {
        createTargetTable(targetTableName);
        String renameSql = String.format("rename table %s to %s", originTableName, targetTableName);
        Statement statement = JdbcUtil.createStatement(tddlConnection);
        try {
            statement.execute(renameSql);
            fail();
        } catch (Exception e) {
            containsString(String.format("Table '%s' already exists.", targetTableName));
        } finally {
            dropTableIfExists(originTableName);
            // dropTableIfExists(targetTableName); // in destroy phase
        }
    }

    public void purgeDDLTable() {
        // 1. 清理回收站
        try (Connection connection = getPolardbxConnection()) {
            Statement preparedStatement = null;
            try {
                preparedStatement = connection.createStatement();
                preparedStatement.executeUpdate("purge recyclebin");
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    /**
     * 部分rename成功，重新执行后需要能继续成功。 源物理分表为空，目标物理分表不为空： 这种情形都当做是由于上一次执行RENAME
     * TABLE没有最终成功导致的（不考虑源表物理分表不完整，目标表物理分表已存在这种case），
     * 此时直接下推执行即可（忽略MySQL抛出的表已存在异常）。
     */
    @Test
    public void testRepetiveExecutionWhenErrorOccur() throws SQLException {
        if (originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX) || originTableName.endsWith(BROADCAST_TB_SUFFIX)) {
            purgeDDLTable();
            ShowTopologyResult topologyRes = fetchTopology(originTableName);
            String oldPhysicalTableName = topologyRes.randomTableName();
            Map<String, String> reverseTopologyMap = topologyRes.getReverseTopoloyMap();
            String groupName = reverseTopologyMap.get(oldPhysicalTableName);

            Connection conn = null;
            try {
                String schemaName =
                    TStringUtil.isEmpty(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
                conn = getGroupMysqlConnection(schemaName, groupName);
                String newPhysicalTableName = mappingNewPhysicalTableName(oldPhysicalTableName,
                    getSimpleTableName(originTableName),
                    getSimpleTableName(targetTableName));
                createPhysicalTable(conn, newPhysicalTableName);

                ShowCreateTableResult oldRes = execShowCreateTable(originTableName);
                String renameSql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "rename table %s to %s",
                    originTableName,
                    targetTableName);
                Statement statement = JdbcUtil.createStatement(tddlConnection);
                try {
                    statement.execute(renameSql);
                } catch (Exception e) {
                    containsString(String.format("Table '%s' already exists.", targetTableName));
                }

                dropPhysicalTable(conn, newPhysicalTableName);
                statement = JdbcUtil.createStatement(tddlConnection);

                try {
                    statement.execute(renameSql);
                } catch (Exception e) {
                    containsString(String.format("Table '%s' already exists.", targetTableName));
                }

                ShowCreateTableResult newRes = execShowCreateTable(targetTableName);
                assertTableNonexists(originTableName);
                Assert.assertTrue(oldRes.tableMetaConsistent(newRes));
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }

        }
    }

    @Test
    public void testCheckSequenceBeforeRenameTable() {
        String sql = "DROP TABLE IF EXISTS test_rename_table";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = "CREATE TABLE test_rename_table (pk int not null auto_increment primary key) dbpartition by hash(pk)";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = "drop sequence AUTO_SEQ_test_rename_table2";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = "create sequence AUTO_SEQ_test_rename_table2";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "rename table test_rename_table to test_rename_table2";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            sql,
            String.format("Sequence of table 'test_rename_table2' already exists"));
        sql = "DROP TABLE IF EXISTS test_rename_table";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = "drop sequence AUTO_SEQ_test_rename_table2";
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    @Test
    @Ignore("fix by ???")
    public void testReCreateTableAfterRenameOperation() {
        if (originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX)) {
            String sql = "drop table if exists reabc";
            JdbcUtil.executeUpdate(tddlConnection, sql);
            sql = "create table reabc(id bigint(20))";
            JdbcUtil.executeUpdate(tddlConnection, sql);
            sql = "rename table reabc to redef";
            JdbcUtil.executeUpdate(tddlConnection, sql);
            sql = "create table reabc(id bigint(20))";
            JdbcUtil.executeUpdate(tddlConnection, sql);
        }
    }

    @Test
    public void testLogicalRenameTableEnabled() throws SQLException {
        if (originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX) && TStringUtil.isEmpty(schemaPrefix)) {
            testLogicalRenameTable(true);
        }
    }

    @Test
    public void testLogicalRenameTableDisabled() throws SQLException {
        if (originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX) && TStringUtil.isEmpty(schemaPrefix)) {
            testLogicalRenameTable(false);
        }
    }

    private void testLogicalRenameTable(boolean enabled) throws SQLException {
        final String hint = "/*+TDDL:cmd_extra(ENABLE_RANDOM_PHY_TABLE_NAME=FALSE)*/";
        String createSql = "create table if not exists %s(c1 int, c2 int) dbpartition by hash(c1)";
        String dropSql = "drop table if exists %s";

        if (!enabled) {
            createSql = hint + createSql;
        }

        // Drop the table first.
        final String origTableName = "logical_rename_table_checking";
        JdbcUtil.executeUpdate(tddlConnection, String.format(dropSql, origTableName));

        // Create a new sharding table with random physical table name suffix enabled.
        JdbcUtil.executeUpdate(tddlConnection, String.format(createSql, origTableName));
        ShowTopologyResult showTopologyResult = fetchTopology(origTableName);
        String newPhyTableName = showTopologyResult.randomTableName();

        // Rename the table to a longer name with the same prefix.
        String tableName = origTableName;
        String newTableName = "logical_rename_table_checking_new";
        newPhyTableName = renameAndCheck(tableName, newTableName, newPhyTableName, enabled);

        // Rename the table to original name.
        tableName = newTableName;
        newTableName = origTableName;
        newPhyTableName = renameAndCheck(tableName, newTableName, newPhyTableName, enabled);

        // Rename the table to a shorter name with the same prefix.
        tableName = newTableName;
        newTableName = "logical_rename_table";
        newPhyTableName = renameAndCheck(tableName, newTableName, newPhyTableName, enabled);

        // Rename the table to a different name.
        try {
            tableName = newTableName;
            newTableName = "renamed_different_name";
            newPhyTableName = renameAndCheck(tableName, newTableName, newPhyTableName, enabled);
        } catch (Exception e) {
            if (TStringUtil.contains(e.getMessage(), "ERR_ASSERT_TRUE")) {
                // Avoid next test is affected by this.
                JdbcUtil.executeUpdate(tddlConnection, String.format(dropSql, newTableName));
            }
            throw e;
        }

        // Rename the table to original name.
        try {
            tableName = newTableName;
            newTableName = origTableName;
            renameAndCheck(tableName, newTableName, newPhyTableName, enabled);
        } catch (Exception e) {
            if (TStringUtil.equalsIgnoreCase(e.getMessage(), "ERR_ASSERT_TRUE")) {
                // Avoid next test is affected by this.
                JdbcUtil.executeUpdate(tddlConnection, String.format(dropSql, newTableName));
            }
            throw e;
        }

        // Drop the table.
        JdbcUtil.executeUpdate(tddlConnection, String.format(dropSql, origTableName));
    }

    private String renameAndCheck(String sourceTableName, String targetTableName, String previousPhyTableName,
                                  boolean enabled) throws SQLException {
        String sql = "rename table %s to %s";
        JdbcUtil.executeUpdate(tddlConnection, String.format(sql, sourceTableName, targetTableName));

        ShowTopologyResult showTopologyResult = fetchTopology(targetTableName);
        String currentPhyTableName = showTopologyResult.randomTableName();

        boolean phyTableNameNotChanged = TStringUtil.equalsIgnoreCase(previousPhyTableName, currentPhyTableName);
        if (enabled) {
            Assert.assertTrue(phyTableNameNotChanged);
        } else {
            Assert.assertTrue(!phyTableNameNotChanged);
        }

        return currentPhyTableName;
    }

    @Test
    public void testInvalidTargetSchema() {
        if (TStringUtil.isNotEmpty(tddlDatabase2)) {
            return;
        }
        String invalidSchema = "invalid";
        String renameSql = String.format("rename table %s to %s", originTableName, invalidSchema + "."
            + targetTableName);
        Statement statement = JdbcUtil.createStatement(tddlConnection);
        try {
            statement.execute(renameSql);
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unknown target database " + invalidSchema));
        }
    }

    @Test
    public void testTableNameContainingMinusWithRecycleBin() {
        if (originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX)) {
            // The table name contains a minus that have to be escaped.
            dropWithRecycleBin("`test-minus`");
        }
    }

    @Test
    public void testTableNameContainingBacktickWithRecycleBin() {
        if (originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX)) {
            // The table name contains two backticks that have to be escaped.
            dropWithRecycleBin("`test``backtick`");
        }
    }

    @Test
    public void testRecycleBinWithFlashback() throws SQLException {
        // Run once only
        if (TStringUtil.isEmpty(schemaPrefix) && originTableName.endsWith(ONE_DB_ONE_TB_SUFFIX)) {
            for (String tableName : recycleBinTableEntityMap.keySet()) {
                ShowCreateTableResult oldRes = execShowCreateTable(tableName);

                // Drop the table with recycle bin enabled
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    String.format(RECYCLE_BIN_HINT + "drop table if exists %s", tableName));

                assertTableNonexists(tableName);

                String recyclebinTableName = null;
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show recyclebin")) {
                    if (rs.next()) {
                        recyclebinTableName = rs.getString("NAME");
                    }
                }
                Assert.assertTrue(TStringUtil.isNotEmpty(recyclebinTableName));

                // Flashback
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    String.format("flashback table %s to before drop", recyclebinTableName));

                ShowCreateTableResult origRes = execShowCreateTable(tableName);
                Assert.assertTrue(oldRes.tableMetaConsistent(origRes));
            }
        }
    }

    private void dropWithRecycleBin(String tableName) {
        purgeDDLTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table if not exists " + tableName + " (c1 int)");

        // Try to drop the table with recycle bin enabled
        JdbcUtil.executeUpdateSuccess(tddlConnection, RECYCLE_BIN_HINT + "drop table if exists " + tableName);

        // purge again
        purgeDDLTable();
    }

    private void assertTableNonexists(String tableName) {
        try {
            execShowCreateTable(tableName);
            fail();
        } catch (Exception e) {
            containsString(String.format("Table '%s' metadata cannot be fetched", tableName));
        }
    }

    private ShowCreateTableResult execShowCreateTable(String tableName) throws SQLException {
        String sql = "show create table " + tableName;
        Statement statement = JdbcUtil.createStatement(tddlConnection);
        try {
            return convert(statement.executeQuery(sql));
        } finally {
            JdbcUtil.close(statement);
        }
    }

    private String mappingNewPhysicalTableName(String oldPhysicalTableName, String oldLogicalTableName,
                                               String newLogicalTableName) {
        return oldPhysicalTableName.replaceFirst(oldLogicalTableName, newLogicalTableName);
    }

    private void dropPhysicalTable(Connection conn, String tableName) throws SQLException {
        String dropTableSql = String.format("drop table %s", tableName);
        Statement stmt = conn.createStatement();
        stmt.execute(dropTableSql);
    }

    private Connection getGroupMysqlConnection(String schemaName, String groupName) {

        Connection conn = getMysqlConnection();
        if (conn == null) {
            throw new IllegalArgumentException("Cann't found this group!");
        }
        DefaultDBInfo.ShardGroupInfo groupInfos =
            DefaultDBInfo.getInstance().getShardGroupListByMetaDb(schemaName, false)
                .getValue();
        String phyDbName = groupInfos.groupAndPhyDbMaps.get(groupName);
        useDb(conn, phyDbName);
        return conn;

    }

    private void createPhysicalTable(Connection conn, String tableName) throws SQLException {
        String createTableSql = String.format("create table %s(id bigint(20))", tableName);
        Statement stmt = conn.createStatement();
        stmt.execute(createTableSql);
    }

    public ShowTopologyResult fetchTopology(String tableName) throws SQLException {
        String sql = String.format("show topology from %s", tableName);
        Statement statement = JdbcUtil.createStatement(tddlConnection);
        ResultSet rs = statement.executeQuery(sql);
        Map<String, List<String>> topologoyMap = Maps.newHashMap();
        while (rs.next()) {
            String groupName = rs.getString("GROUP_NAME");
            String _tableName = rs.getString("TABLE_NAME");
            List<String> tables = topologoyMap.get(groupName);
            if (tables == null) {
                tables = Lists.newArrayList();
                topologoyMap.put(groupName, tables);
            }
            tables.add(_tableName);
        }

        return new ShowTopologyResult(topologoyMap);
    }

    private static ShowCreateTableResult convert(ResultSet rs) throws SQLException {
        if (rs != null && rs.next()) {
            String tableName = rs.getString("Table");
            String createTableStatement = rs.getString("Create Table");
            return new ShowCreateTableResult(tableName, createTableStatement);
        } else {
            return null;
        }
    }

    private static class ShowTopologyResult {

        private Map<String/* groupName */, List<String/* tableName */>> topologyMap;

        private Map<String/* tableName */, String/* groupName */> reverseTopoloyMap;

        public ShowTopologyResult(Map<String, List<String>> topologyMap) {
            if (topologyMap == null) {
                throw new IllegalArgumentException();
            }

            this.topologyMap = topologyMap;
            reverseTopoloyMap = Maps.newHashMap();
            for (String groupName : topologyMap.keySet()) {
                List<String> tables = topologyMap.get(groupName);
                for (String tableName : tables) {
                    reverseTopoloyMap.put(tableName, groupName);
                }
            }
        }

        public String randomTableName() {
            List<String> allTables = Lists.newArrayList(reverseTopoloyMap.keySet());
            Random rd = new Random();
            return allTables.get(rd.nextInt(allTables.size()));
        }

        public Map<String, List<String>> getTopologyMap() {
            return topologyMap;
        }

        public Map<String, String> getReverseTopoloyMap() {
            return reverseTopoloyMap;
        }
    }

    private static class ShowCreateTableResult {

        private String tableName;

        private String createTableStatement;

        public ShowCreateTableResult(String tableName, String createTableStatement) {
            this.tableName = tableName;
            this.createTableStatement = createTableStatement;
        }

        public boolean tableMetaConsistent(ShowCreateTableResult other) {
            if (other == null) {
                return false;
            }

            if (StringUtils.isEmpty(createTableStatement) || StringUtils.isEmpty(other.createTableStatement)) {
                return false;
            }

            String replCreateTableStmt = other.getCreateTableStatement().replaceFirst("`" + other.tableName + "`",
                "`" + tableName + "`");
            return createTableStatement.equals(replCreateTableStmt);
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getCreateTableStatement() {
            return createTableStatement;
        }

        public void setCreateTableStatement(String createTableStatement) {
            this.createTableStatement = createTableStatement;
        }
    }

    /**
     * create TDDL提供的最基本的五种类型表
     */
    public static void createTDDLTable(String appName, TableEntity tableEntity, Connection connection) {

        tableDrop(tableEntity, connection);

        tableEntity.getCreateTableDdl();

        if (ConfigUtil.isSingleApp(appName)) {
            tableCreate(tableEntity, true, false, connection);
        } else {
            tableCreate(tableEntity, false, false, connection);
        }
    }

    /**
     * 建表
     */
    public static void tableCreate(TableEntity tableEntity, boolean isSingle, boolean ifNotExits, Connection conn) {
        JdbcUtil.executeUpdate(conn, tableEntity.getCreateTableDdl(isSingle, ifNotExits));
    }

    /**
     * 删表
     */
    public static void tableDrop(TableEntity tableEntity, Connection conn) {
        JdbcUtil.executeUpdate(conn, "drop table if exists " + tableEntity.getTbName());
    }
}

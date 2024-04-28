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

package com.alibaba.polardbx.qatest.ddl.auto.locality;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Value;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author moyi
 * @since 2021/08
 */
@NotThreadSafe
public class LocalityPartDbTest extends LocalityTestBase {

    protected static final Log log = LogFactory.getLog(LocalityPartDbTest.class);
    protected static final String currentDatabase = "test_locality_partdb_test";

    @BeforeClass
    public static void beforeClass() {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.createPartDatabase(connection, currentDatabase);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void afterClass() {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.dropDatabase(connection, currentDatabase);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Before
    public void initPolarDBXConnection() {
        useDb(tddlConnection, currentDatabase);
    }

    /**
     * Database with locality
     */
    @Test
    public void testDatabaseLocality() {

        final String dn = chooseDatanode(true);
        final String localitySql = " LOCALITY=" + TStringUtil.quoteString("dn=" + dn);
        final String dbName = "test_locality_partdb";
        final String dropDbSql = String.format("drop database if exists %s", dbName);
        final String createDbSql = String.format("create database %s mode = auto %s", dbName, localitySql);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropDbSql);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createDbSql);
        List<DSBean> dsList = getDsListOfSchema(dbName);
        Assert.assertTrue(
            "dsList is " + dsList,
            dsList.stream()
                .filter(x -> !GroupInfoUtil.isSingleGroup(x.getGroup()))
                .allMatch(x -> x.getStorageInst().equalsIgnoreCase(dn))
        );

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropDbSql);
    }

    @Test
    public void testDrdsDatabaseLocality() {

        final String dn = chooseDatanode(true);
        final String localitySql = " LOCALITY=" + TStringUtil.quoteString("dn=" + dn);
        final String dbName = "test_locality_drdsdb";
        final String dropDbSql = String.format("drop database if exists %s", dbName);
        final String createDbSql = String.format("create database %s mode = drds %s", dbName, localitySql);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropDbSql);

        String errMsg = "database of drds mode doesn't support locality specification!";
        JdbcUtil.executeUpdateFailed(tddlConnection, createDbSql, errMsg);
    }

    /**
     * Table-group with locality
     */
    @Test
    public void testTableGroupLocality() throws SQLException {

        final String tgName = "tg_locality";
        final String tableName = "test_tb";
        final String dn = chooseDatanode();
        final String localitySql = " LOCALITY=" + TStringUtil.quoteString("dn=" + dn);
        final String createTgSql = String.format("create tablegroup %s %s", tgName, localitySql);
        final String createTableSql = String.format("create table %s(id int) tablegroup='%s' "
            + "partition by hash(id) partitions 32", tableName, tgName);

        // create tablegroup
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);
        // check tablegroup
        List<TableGroupVO> tgList = showTableGroup();
        Assert.assertTrue(tgList.stream().anyMatch(x -> x.tableGroupName.equalsIgnoreCase(tgName)));

        // create table in tablegroup
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        // check table topology
        DSBean ds =
            getDsListOfSchema(currentDatabase).stream().filter(x -> x.storageInst.equalsIgnoreCase(dn)).findFirst()
                .get();
        List<TopologyVO> topologyVOList = showTableTopology(tableName);
        Assert.assertTrue(
            "group should at " + ds + ", but topology is " + topologyVOList,
            topologyVOList.stream().allMatch(x -> x.groupName.equalsIgnoreCase(ds.group)));

        // move table to another tablegroup
        // TODO(moyi)
    }

    /**
     * Partition table not support locality
     */
//    @Test
//    public void testLocalityNotSupported() {
//
//        final String tableName = "table_hash";
//        final String dn = chooseDatanode();
//        final String localitySql = " LOCALITY=" + TStringUtil.quoteString("dn=" + dn);
//
//        final String createBroadcastTableSql = String.format("create table %s (id int) %s %s",
//            tableName, "broadcast", localitySql);
//        JdbcUtil.executeUpdateFailed(tddlConnection, createBroadcastTableSql, "not support");
//    }
    @Test
    public void testSingleTableLocality() {

        final String tableName = "table_single";
        final String dn = chooseDatanode();
        final String localitySql = "dn=" + dn;
        final String createTableSql = String.format("create table %s(id int) single LOCALITY=%s", tableName,
            TStringUtil.quoteString(localitySql));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        // check show create
        String createResult = showCreateTable(tableName);
        Assert.assertTrue(" create table is " + createResult, createResult.contains(localitySql));
        // check locality_info
        Optional<LocalityTestBase.LocalityBean> localityInfo =
            getLocalityBeanInfos().stream().filter(x -> tableName.equals(x.objectName)).findFirst();
        Assert.assertEquals(localitySql, localityInfo.get().locality);

        // drop table and check locality
        final String dropTableSql = "drop table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTableSql);
        boolean exists = getLocalityBeanInfos().stream().anyMatch(x -> tableName.equals(x.objectName));
        Assert.assertFalse(exists);
    }

    /**
     * Partition-group with locality
     */
    @Test
    public void testPartitionGroupLocality() throws SQLException {

        List<String> dnList = getDatanodes();
        List<DSBean> dsList = getDsBeanList();
        Assume.assumeTrue(dnList.size() >= 2);

        final String dn1 = dnList.get(0);
        final Set<String> targetDs = dsList.stream()
            .filter(x -> x.getStorageInst().equalsIgnoreCase(dn1) && x.getDatabase().equals(currentDatabase))
            .map(DSBean::getGroup).collect(Collectors.toSet());
        LOG.info("With locality the target group should be " + targetDs);

        final String tableName = "test_pg";
        final String locality1 = String.format("locality='dn=%s'", dn1);
        final String createTableSql = String.format("create table %s(id int) partition by range(id) " +
                "(partition p0 values less than (1000) %s" +
                ",partition p1 values less than (2000) %s" +
                ")",
            tableName, locality1, locality1);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        String showCreate = showCreateTable(tableName);
        String tg1 = getTableGroupOfTable(tableName);
        // check show create
        Assert.assertTrue(" create table is " + showCreate, StringUtils.containsIgnoreCase(showCreate, locality1));
        // check topology
        List<TopologyVO> topologyVOList = showTableTopology(tableName);
        Assert.assertTrue(topologyVOList.stream().allMatch(x -> targetDs.contains(x.getGroupName())));

        // create another table, it should be previous table-group
        String tableName2 = "test_pg1";
        String createTableSql2 = String.format("create table %s(id int) partition by range(id) " +
                "(partition p0 values less than (1000) %s" +
                ",partition p1 values less than (2000) %s" +
                ")",
            tableName2, locality1, locality1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql2);
        String showCreate2 = showCreateTable(tableName2);
        Assert.assertTrue(showCreate2, StringUtils.containsIgnoreCase(showCreate2, locality1));
        String tg2 = getTableGroupOfTable(tableName2);
        Assert.assertEquals(tg1, tg2);
        Assert.assertTrue(showTableTopology(tableName2).stream().allMatch(x -> targetDs.contains(x.getGroupName())));

        // create a table with different locality
        String tableName3 = "test_pg3";
        String locality3 = String.format("locality='dn=%s'", dnList.get(1));
        Set<String> targetDs3 = dsList.stream()
            .filter(x -> x.getStorageInst().equalsIgnoreCase(dnList.get(1)) && x.getDatabase().equals(currentDatabase))
            .map(DSBean::getGroup).collect(Collectors.toSet());
        String createTableSql3 = String.format("create table %s(id int) partition by range(id) " +
                "(partition p0 values less than (1000) %s" +
                ",partition p1 values less than (2000) %s" +
                ")",
            tableName3, locality3, locality3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql3);
        String showCreate3 = showCreateTable(tableName3);
        Assert.assertTrue(showCreate3, StringUtils.containsIgnoreCase(showCreate3, locality3));
        Assert.assertTrue(showCreate3, StringUtils.containsIgnoreCase(showCreate3, locality3));
        String tg3 = getTableGroupOfTable(tableName3);
        Assert.assertNotEquals(tg2, tg3);
        Assert.assertTrue(showTableTopology(tableName3).stream().allMatch(x -> targetDs3.contains(x.getGroupName())));
    }

    private List<TopologyVO> showTableTopology(String table) throws SQLException {
        final String sql = "show topology from " + table;

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            List<TopologyVO> result = new ArrayList<>();
            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String partitionName = rs.getString("PARTITION_NAME");
                TopologyVO topology = new TopologyVO(groupName, tableName, partitionName);

                result.add(topology);
            }
            return result;
        }
    }

    private String getTableGroupOfTable(String tableName) {
        final String sql = "show full create table " + tableName;
        final String create = JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 2);
        LOG.info("show full create table " + tableName + ": " + create);
        final Pattern pattern = Pattern.compile("tablegroup\\s*=\\s*`(\\w+)`");
        Matcher matcher = pattern.matcher(create);
        if (!matcher.find()) {
            throw new RuntimeException("tablegroup not found: " + create);
        }
        return matcher.group(1);
    }

    private String showCreateTable(String tablename) {
        final String sql = "show create table " + tablename;
        return JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 2);
    }

    private List<TableGroupVO> showTableGroup() throws SQLException {
        final String sql = "show tablegroup";
        List<TableGroupVO> result = new ArrayList<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEMA");
                long tableGroupId = rs.getLong("TABLE_GROUP_ID");
                String tableGroupName = rs.getString("TABLE_GROUP_NAME");
                String locality = rs.getString("LOCALITY");
                String tables = rs.getString("TABLE_COUNT");
                TableGroupVO tg = new TableGroupVO(schema, tableGroupId, tableGroupName, locality, tables);
                result.add(tg);
            }

            return result;
        }
    }

    @Value
    static class TableGroupVO {
        String schemaName;
        long tableGroupId;
        String tableGroupName;
        String locality;
        String tables;
    }

    @Value
    static class TopologyVO {
        String groupName;
        String tableName;
        String partitionName;
    }

}

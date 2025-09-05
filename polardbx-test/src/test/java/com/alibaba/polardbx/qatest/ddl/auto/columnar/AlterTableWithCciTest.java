/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarColumnEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesAccessor;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.ddl.AlterTableTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class AlterTableWithCciTest extends DDLBaseNewDBTestCase {
    final static Log log = LogFactory.getLog(AlterTableTest.class);

    private static final String PRIMARY_TABLE_PREFIX = "alter_table_with_cci_prim";
    private static final String INDEX_PREFIX = "alter_table_with_cci_cci";
    private static final String PRIMARY_TABLE_NAME1 = PRIMARY_TABLE_PREFIX + "_1";
    private static final String INDEX_NAME1 = INDEX_PREFIX + "_1";
    private static final String PRIMARY_TABLE_NAME2 = PRIMARY_TABLE_PREFIX + "_2";
    private static final String INDEX_NAME2 = INDEX_PREFIX + "_2";
    private static final String PRIMARY_TABLE_NAME3 = PRIMARY_TABLE_PREFIX + "_3";
    private static final String INDEX_NAME3 = INDEX_PREFIX + "_2";

    private static final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `seller_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext, \n"
        + "    PRIMARY KEY (`id`), \n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
        + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";

    private static final String creatTableTmpl2 = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `seller_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext,\n"
        + "    PRIMARY KEY (`id`), \n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
        + ") PARTITION BY RANGE(id) \n"
        + "(partition p0 values less than (1000)\n"
        + ",partition p1 values less than (2000)\n"
        + ");\n";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @After
    public void after() {
//        dropTableIfExists(PRIMARY_TABLE_NAME1);
//        dropTableIfExists(PRIMARY_TABLE_NAME2);
//        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @Test
    public void testAddColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c2"));
        checkCciMeta(indexName);

        sql = "alter table %s add c3 int first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c3"));
        checkCciMeta(indexName);

        sql = "alter table %s add c4 int after c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c4"));
        checkCciMeta(indexName);
    }

    @Test
    public void testDropColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s drop seller_id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("seller_id"));
        checkCciMeta(indexName);

        // drop last column
        sql = "alter table %s drop order_snapshot";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("order_snapshot"));
        checkCciMeta(indexName);
    }

    @Test
    public void testModifyColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s modify column seller_id longtext";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);

        sql = "alter table %s modify column seller_id varchar(64) after id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);

        sql = "alter table %s modify column seller_id varchar(64) first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);
    }

    @Test
    public void testChangeColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s change column seller_id seller_id_1 varchar(64)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("seller_id_1", "seller_id")));
        checkCciMeta(indexName);
    }

    @Test
    public void testMultiAlters() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s add column c2 int, add column c3 int first, add column c4 int after c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c2", "c3", "c4"));

//        sql = "alter table %s add c2 int";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
//        sql = "alter table %s add c3 int first";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
//        sql = "alter table %s add c4 int after c3";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column c2 bigint, modify column c3 int first, change column c4 c40 int after id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("c40", "c4")));
        checkCciMeta(indexName);

        sql = "alter table %s change column c40 c4 int, drop column c2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);

        sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s drop column c2, drop column c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("c2, c3"));
        checkCciMeta(indexName);
    }

    @Test
    public void testRepartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);
        String originCciName = fetchCciSysTable(schemaName, tableName, indexName);

        String sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        String afterCciName = fetchCciSysTable(schemaName, tableName, indexName);

        Assert.assertTrue(originCciName.equals(afterCciName));

        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        afterCciName = fetchCciSysTable(schemaName, tableName, indexName);
        Assert.assertTrue(originCciName.equals(afterCciName));
        checkCciMeta(indexName);
    }

    @Test
    public void testOMC() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // SINGLE STATEMENT
        String sql = "alter table %s modify column seller_id longtext, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);

        sql = "alter table %s modify column seller_id varchar(64) after id, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);

        sql = "alter table %s modify column seller_id longtext first, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);

        sql = "alter table %s change column seller_id seller_id_1 varchar(64), algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("seller_id_1", "seller_id")));
        checkCciMeta(indexName);

        // MULTI STATEMENTS
        sql =
            "alter table %s modify column seller_id_1 longtext, add column c2 int, add column c3 int first, add column c4 int, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);

        sql =
            "alter table %s modify column c2 bigint, modify column c3 int first, change column c4 c40 int after id, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("c40", "c4")));
        checkCciMeta(indexName);

        sql = "alter table %s change column c40 c4 int, drop column c2, add column tmp int algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);
    }

    @Test
    public void testRenameCci() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;
        String newIndexName = INDEX_NAME2;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s rename index %s to %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, indexName, newIndexName));

        // show primary table
        String showCreateTableSql = String.format("show create table %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        String createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(newIndexName));

        // show cci table
        showCreateTableSql = String.format("show create table %s", newIndexName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(newIndexName));
    }

    @Test
    public void testRebuildTable() throws SQLException {
        // 特殊的情况，修改分区键（即使不更改分区键的长度）会重建主表，而不是走alter table的路径
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s modify column order_id varchar(20) comment 'test'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkCciMeta(indexName);
    }

    @Test
    public void testAddColumnAfterRepartition() throws SQLException {
        /**
         * 测试在经过repartition(move partition, split partition, etc.)后indexes系统表的version列变更
         * 在老代码中，再创建新的列，其version列值为0，导致排序的时候被列为第一列，失去了CCI的标识和CLUSTER属性
         * 新代码会将新列的version列值设置为和原表一致，避免这个问题。
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl2,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // check last version & seq
        Pair<Long, Long> versionAndSeqBefore = checkVersionAndSeq(schemaName, tableName);
        Assert.assertNotNull(versionAndSeqBefore);

        // split partition
        String sql =
            "ALTER TABLE %s SPLIT PARTITION p1 INTO  (PARTITION p10 VALUES LESS THAN (1500), PARTITION p11 VALUES LESS THAN(2000))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        Pair<Long, Long> versionAndSeqAfter = checkVersionAndSeq(schemaName, tableName);
        Assert.assertNotNull(versionAndSeqAfter);
        assertEquals(versionAndSeqBefore.getKey(), versionAndSeqAfter.getKey());

        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c2"));
        checkCciMeta(indexName);

        sql = "alter table %s add c3 int first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c3"));
        checkCciMeta(indexName);

        // test omc
        versionAndSeqBefore = checkVersionAndSeq(schemaName, tableName);
        Assert.assertNotNull(versionAndSeqBefore);

        // split partition
        sql =
            "ALTER TABLE %s SPLIT PARTITION p11 INTO  (PARTITION p110 VALUES LESS THAN (1700), PARTITION p111 VALUES LESS THAN(2000))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column c3 bigint, add c4 int after c3, algorithm = omc";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        versionAndSeqAfter = checkVersionAndSeq(schemaName, tableName);
        Assert.assertNotNull(versionAndSeqAfter);
        assertEquals(versionAndSeqBefore.getKey(), versionAndSeqAfter.getKey());

        compareColumnPositions(schemaName, tableName);
        compareColumnRecords(schemaName, tableName);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c4"));
        checkCciMeta(indexName);
    }

    public Pair<Long, Long> checkVersionAndSeq(String schemaName, String tableName) throws SQLException {
        String sql = "select version, seq_in_index from indexes "
            + "where table_schema='%s' and table_name='%s' and index_location = 1 order by version,seq_in_index desc limit 1";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName))) {
            if (rs.next()) {
                return new Pair<>(rs.getLong(1), rs.getLong(2));
            }
        }
        return null;
    }

    private void compareColumnPositions(String schemaName, String tableName)
        throws SQLException {
        Map<Integer, String> sysTableColumnPositions = fetchColumnPositionsFromSysTable(schemaName, tableName);
        Map<Integer, String>
            evolutionTableColumnPositions = fetchColumnPositionsFromEvolutionTable(schemaName, tableName);
        compareColumnPositions(sysTableColumnPositions, evolutionTableColumnPositions);
    }

    private Map<Integer, String> fetchColumnPositionsFromSysTable(String schemaName, String tableName)
        throws SQLException {
        Map<Integer, String> columnPositions = new HashMap<>();
        String sql = "select ordinal_position, column_name from columns "
            + "where table_schema='%s' and table_name='%s' order by ordinal_position";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName))) {
            while (rs.next()) {
                columnPositions.put(rs.getInt(1), rs.getString(2));
            }
        }
        return columnPositions;
    }

    private Map<Integer, String> fetchColumnPositionsFromEvolutionTable(String schemaName, String tableName)
        throws SQLException {
        Map<Integer, String> columnPositions = new HashMap<>();
        String sql1 = "select columns from columnar_table_evolution "
            + "where table_schema='%s' and table_name='%s' order by `version_id` desc limit 1";
        String sql2 = "select column_name from columnar_column_evolution where id=%s";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql1, schemaName, tableName))) {
            Assert.assertTrue(rs.next());
            List<Long> columnIds = ColumnarTableEvolutionRecord.deserializeListFromJson(rs.getString(1));
            for (int i = 0; i < columnIds.size(); i++) {
                ResultSet rs1 = stmt.executeQuery(String.format(sql2, columnIds.get(i)));
                Assert.assertTrue(rs1.next());
                columnPositions.put(i + 1, rs1.getString(1));
            }

        }
        return columnPositions;
    }

    private void compareColumnPositions(Map<Integer, String> sysTableColumnPositions,
                                        Map<Integer, String> evolutionTableColumnPositions) {
        printColumnPositions(sysTableColumnPositions, "Columns System Table");
        printColumnPositions(evolutionTableColumnPositions, "Columnar Table Evolution");

        if (sysTableColumnPositions == null || sysTableColumnPositions.isEmpty() ||
            evolutionTableColumnPositions == null || evolutionTableColumnPositions.isEmpty()) {
            Assert.fail("Invalid column names and positions");
        }

        if (sysTableColumnPositions.size() != evolutionTableColumnPositions.size()) {
            Assert.fail("Different column sizes");
        }

        for (Integer columnPos : sysTableColumnPositions.keySet()) {
            if (!TStringUtil.equalsIgnoreCase(sysTableColumnPositions.get(columnPos),
                evolutionTableColumnPositions.get(columnPos))) {
                Assert.fail("Different column names '" + sysTableColumnPositions.get(columnPos) + "' and '"
                    + evolutionTableColumnPositions.get(columnPos) + "' at " + columnPos);
            }
        }
    }

    private void printColumnPositions(Map<Integer, String> columnPositions, String tableInfo) {
        if (MapUtils.isNotEmpty(columnPositions)) {
            StringBuilder buf = new StringBuilder();
            buf.append("\n").append("Column Positions from ").append(tableInfo).append(":\n");
            for (Map.Entry<Integer, String> entry : columnPositions.entrySet()) {
                buf.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
            }
            log.info(buf);
        } else {
            log.info("No Column Positions from " + tableInfo);
        }
    }

    private void compareColumnRecords(String schemaName, String tableName)
        throws SQLException {
        List<ColumnsRecord> sysTableColumnRecords = fetchColumnRecordsFromSysTable(schemaName, tableName);
        List<ColumnsRecord> evolutionTableColumnRecords = fetchColumnRecordsFromEvolutionTable(schemaName, tableName);
        compareColumnRecords(sysTableColumnRecords, evolutionTableColumnRecords);
    }

    private List<ColumnsRecord> fetchColumnRecordsFromSysTable(String schemaName, String tableName)
        throws SQLException {
        List<ColumnsRecord> columnsRecords;
        try (Connection metaDbConn = getMetaConnection();) {
            ColumnsAccessor columnsAccessor = new ColumnsAccessor();
            columnsAccessor.setConnection(metaDbConn);
            columnsRecords = columnsAccessor.query(schemaName, tableName);
        }
        return columnsRecords;
    }

    private List<ColumnsRecord> fetchColumnRecordsFromEvolutionTable(String schemaName, String tableName)
        throws SQLException {
        List<ColumnsRecord> columnsRecords = new ArrayList<>();
        String sql1 = "select columns from columnar_table_evolution "
            + "where table_schema='%s' and table_name='%s' order by `version_id` desc limit 1";
        String sql2 = "select columns_record from columnar_column_evolution where id=%s";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql1, schemaName, tableName))) {
            Assert.assertTrue(rs.next());
            List<Long> columnIds = ColumnarTableEvolutionRecord.deserializeListFromJson(rs.getString(1));
            for (Long columnId : columnIds) {
                ResultSet rs1 = stmt.executeQuery(String.format(sql2, columnId));
                Assert.assertTrue(rs1.next());
                columnsRecords.add(ColumnarColumnEvolutionRecord.deserializeFromJson(rs1.getString(1)));
            }

        }
        return columnsRecords;
    }

    private void compareColumnRecords(List<ColumnsRecord> sysTableColumnRecords,
                                      List<ColumnsRecord> evolutionTableColumnRecords) {

        if (sysTableColumnRecords == null || sysTableColumnRecords.isEmpty() ||
            evolutionTableColumnRecords == null || evolutionTableColumnRecords.isEmpty()) {
            Assert.fail("Invalid column records");
        }

        if (sysTableColumnRecords.size() != evolutionTableColumnRecords.size()) {
            Assert.fail("Different column sizes");
        }

        for (int i = 0; i < sysTableColumnRecords.size(); i++) {
            ColumnsRecord sysRecord = sysTableColumnRecords.get(i);
            ColumnsRecord evolutionRecord = evolutionTableColumnRecords.get(i);
            if (!ColumnsRecord.equalsColumnRecord(sysRecord, evolutionRecord)) {
                Assert.fail("Different column records in '" + sysTableColumnRecords.get(i).columnName);
            }
        }
    }

    private void checkAddIndexesRecords(String schemaName, String tableName, List<String> addColumns)
        throws SQLException {
        List<IndexesRecord> indexesRecords;
        try (Connection metaDbConn = getMetaConnection()) {
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            IndexesAccessor indexesAccessor = new IndexesAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);
            indexesAccessor.setConnection(metaDbConn);

            List<String> indexes =
                columnarTableMappingAccessor.querySchemaTable(schemaName, tableName).stream().map(c -> c.indexName)
                    .collect(
                        Collectors.toList());
            for (String index : indexes) {
                indexesRecords = indexesAccessor.query(schemaName, tableName, index);
                if (GeneralUtil.isEmpty(indexesRecords)) {
                    continue;
                }
                Assert.assertTrue(indexesRecords.stream().anyMatch(record -> addColumns.contains(record.columnName)));
            }
        }
    }

    private void checkDropIndexesRecords(String schemaName, String tableName, List<String> dropColumns)
        throws SQLException {
        List<IndexesRecord> indexesRecords;
        try (Connection metaDbConn = getMetaConnection()) {
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            IndexesAccessor indexesAccessor = new IndexesAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);
            indexesAccessor.setConnection(metaDbConn);

            List<String> indexes =
                columnarTableMappingAccessor.querySchemaTable(schemaName, tableName).stream().map(c -> c.indexName)
                    .collect(
                        Collectors.toList());
            for (String index : indexes) {
                indexesRecords = indexesAccessor.query(schemaName, tableName, index);
                if (GeneralUtil.isEmpty(indexesRecords)) {
                    continue;
                }
                Assert.assertTrue(indexesRecords.stream().noneMatch(record -> dropColumns.contains(record.columnName)));
            }
        }
    }

    private void checkChangeIndexesRecords(String schemaName, String tableName, List<Pair<String, String>> columNames)
        throws SQLException {
        List<IndexesRecord> indexesRecords;
        try (Connection metaDbConn = getMetaConnection()) {
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            IndexesAccessor indexesAccessor = new IndexesAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);
            indexesAccessor.setConnection(metaDbConn);

            List<String> newColumnsNames = columNames.stream().map(Pair::getKey).collect(Collectors.toList());
            List<String> oldColumnsNames = columNames.stream().map(Pair::getValue).collect(Collectors.toList());

            List<String> indexes =
                columnarTableMappingAccessor.querySchemaTable(schemaName, tableName).stream().map(c -> c.indexName)
                    .collect(
                        Collectors.toList());
            for (String index : indexes) {
                indexesRecords = indexesAccessor.query(schemaName, tableName, index);
                if (GeneralUtil.isEmpty(indexesRecords)) {
                    continue;
                }
                Assert.assertTrue(
                    indexesRecords.stream().anyMatch(record -> newColumnsNames.contains(record.columnName)));
                Assert.assertTrue(
                    indexesRecords.stream().noneMatch(record -> oldColumnsNames.contains(record.columnName)));
            }
        }
    }

    private String fetchCciSysTable(String schemaName, String tableName, String columnarName)
        throws SQLException {
        String sql = "select index_name from columnar_table_mapping "
            + "where table_schema='%s' and table_name='%s' and index_name like '%%%s%%' order by latest_version_id desc limit 1";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName, columnarName))) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return "";
    }

    private void checkCciMeta(String indexName) throws SQLException {
        String sql = String.format("check columnar index %s meta", indexName);
        ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        org.junit.Assert.assertTrue(resultSet.next());
        String detail = resultSet.getString("details");
        Assert.assertTrue(detail.startsWith("OK"));
    }
}


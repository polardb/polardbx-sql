package com.alibaba.polardbx.qatest.storagepool.importDatabase;

/**
 * Created by zhuqiwei.
 */

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/**
 * We need to test whether the imported table could be altered partition
 */
public class AlterPartitionTest extends ImportDatabaseBase {
    final static String TB_NAME_PATTERN = "tb";
    final static List<String> schemaNames = ImmutableList.of(
        //测试库名后缀带s会不会报错
        "alter_partition_db0s",
        "alter_partition_db1",
        "alter_partition_db2ssS"
    );

    void testMoveSingleTableAndGroup() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schemaNames.get(0);
        List<String> instNameList =
            storageInstList.stream().map(x -> x.storageInstId).distinct().collect(Collectors.toList());
        Assert.assertTrue(instNameList.size() >= 3);
        //prepare phy database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
            Connection polardbxConn = getPolardbxConnection()
        ) {
            //pre clean
            cleanDatabse(polardbxConn, phyDatabaseName);
            cleanDatabse(storageInstConn, phyDatabaseName);
            //create database
            preparePhyDatabase(phyDatabaseName, storageInstConn);
            //prepare tables
            preparePhyTables(storageInstConn, phyDatabaseName, 4, 50);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //import
        final String importSql = "import database `%s` locality=\"dn=%s\"";
        try (Connection polardbxConn = getPolardbxConnection();
            Statement stmt = polardbxConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(importSql, phyDatabaseName, instName))
        ) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
                Assert.assertTrue("ALL SUCCESS".equalsIgnoreCase(result));
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //test reimport database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName)) {
            JdbcUtil.executeUpdateSuccess(storageInstConn, "use " + phyDatabaseName);
            JdbcUtil.executeUpdateSuccess(storageInstConn, "alter table tb0 add index idx1(name)");
            JdbcUtil.executeUpdateSuccess(storageInstConn, "alter table tb1 modify column name varchar(255)");
            JdbcUtil.executeUpdateSuccess(storageInstConn,
                "alter table tb2 add column addr varchar(128) default \"hangzhou\"");
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        final String reimportSql = "import database if exists `%s` locality=\"dn=%s\"";
        try (Connection polardbxConn = getPolardbxConnection();
            Statement stmt = polardbxConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(reimportSql, phyDatabaseName, instName))
        ) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
                Assert.assertTrue("ALL SUCCESS".equalsIgnoreCase(result));
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //expand nodes to rebalance
        final String alterLocalitySql = "alter database \"%s\" set locality=\"dn=%s\"";

        try (Connection polardbxConn = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format(alterLocalitySql, phyDatabaseName,
                String.join(",",
                    instNameList.subList(0, instNameList.size())))
            );
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        checkRebalanceResult(phyDatabaseName);

        //move single table
        final String moveTableSql = "alter table %s move partitions p1 to '%s'";
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName);
        ) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(moveTableSql, "tb0", instNameList.get(1)));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                RepartitionTest.checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                RepartitionTest.checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //move tablegroup
        final String moveTgSql = "alter tablegroup single_tg move partitions p1 to '%s'";
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName);
        ) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(moveTgSql, instNameList.get(2)));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                RepartitionTest.checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                RepartitionTest.checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    void testAlterTablePartition() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schemaNames.get(1);
        List<String> instNameList =
            storageInstList.stream().map(x -> x.storageInstId).distinct().collect(Collectors.toList());
        Assert.assertTrue(instNameList.size() >= 3);
        //prepare phy database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
            Connection polardbxConn = getPolardbxConnection()
        ) {
            //pre clean
            cleanDatabse(polardbxConn, phyDatabaseName);
            cleanDatabse(storageInstConn, phyDatabaseName);
            //create database
            preparePhyDatabase(phyDatabaseName, storageInstConn);
            //prepare tables
            preparePhyTables(storageInstConn, phyDatabaseName, 4, 50);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //import
        final String importSql = "import database `%s` locality=\"dn=%s\"";
        try (Connection polardbxConn = getPolardbxConnection();
            Statement stmt = polardbxConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(importSql, phyDatabaseName, instName))
        ) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
                Assert.assertTrue("ALL SUCCESS".equalsIgnoreCase(result));
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //test reimport database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName)) {
            JdbcUtil.executeUpdateSuccess(storageInstConn, "use " + phyDatabaseName);
            JdbcUtil.executeUpdateSuccess(storageInstConn, "alter table tb0 add index idx1(name)");
            JdbcUtil.executeUpdateSuccess(storageInstConn, "alter table tb1 modify column name varchar(255)");
            JdbcUtil.executeUpdateSuccess(storageInstConn,
                "alter table tb2 add column addr varchar(128) default \"hangzhou\"");
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        final String reimportSql = "import database if exists `%s` locality=\"dn=%s\"";
        try (Connection polardbxConn = getPolardbxConnection();
            Statement stmt = polardbxConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(reimportSql, phyDatabaseName, instName))
        ) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
                Assert.assertTrue("ALL SUCCESS".equalsIgnoreCase(result));
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //expand nodes to rebalance
        final String alterLocalitySql = "alter database \"%s\" set locality=\"dn=%s\"";

        try (Connection polardbxConn = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format(alterLocalitySql, phyDatabaseName,
                String.join(",",
                    instNameList.subList(0, instNameList.size())))
            );
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        checkRebalanceResult(phyDatabaseName);

        //do repartition
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb0 partition by key(id, name) partitions 36"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(
                    "alter table tb1 partition by List(id) (partition p1 values in(0,1,2), partition p2 values in(3,4,5), partition p3 values in(1000,1001), partition pd values in(default))"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(
                    "alter table tb2 partition by range(id) (partition p1 values less than(10), partition p2 values less than (20), partition pd values less than(maxvalue))"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //test alter
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("ALTER TABLE tb0 SPLIT INTO hp PARTITIONS 3 BY HOT VALUE(1);"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb1 drop partition p3"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb2 merge partitions p1,p2 to p1_2"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                RepartitionTest.checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                RepartitionTest.checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //try to clean
        try (Connection polardbxConn = getPolardbxConnection()) {
            cleanDatabse(polardbxConn, phyDatabaseName);
        } catch (Exception ignore) {
        }
    }

    void testAlterTableGrpupPartition() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schemaNames.get(2);
        List<String> instNameList =
            storageInstList.stream().map(x -> x.storageInstId).distinct().collect(Collectors.toList());
        Assert.assertTrue(instNameList.size() >= 3);
        //prepare phy database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
            Connection polardbxConn = getPolardbxConnection()
        ) {
            //pre clean
            cleanDatabse(polardbxConn, phyDatabaseName);
            cleanDatabse(storageInstConn, phyDatabaseName);
            //create database
            preparePhyDatabase(phyDatabaseName, storageInstConn);
            //prepare tables
            preparePhyTables(storageInstConn, phyDatabaseName, 4, 50);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //import
        final String importSql = "import database `%s` locality=\"dn=%s\"";
        try (Connection polardbxConn = getPolardbxConnection();
            Statement stmt = polardbxConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(importSql, phyDatabaseName, instName))
        ) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
                Assert.assertTrue("ALL SUCCESS".equalsIgnoreCase(result));
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //test reimport database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName)) {
            JdbcUtil.executeUpdateSuccess(storageInstConn, "use " + phyDatabaseName);
            JdbcUtil.executeUpdateSuccess(storageInstConn, "alter table tb0 add index idx1(name)");
            JdbcUtil.executeUpdateSuccess(storageInstConn, "alter table tb1 modify column name varchar(255)");
            JdbcUtil.executeUpdateSuccess(storageInstConn,
                "alter table tb2 add column addr varchar(128) default \"hangzhou\"");
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        final String reimportSql = "import database if exists `%s` locality=\"dn=%s\"";
        try (Connection polardbxConn = getPolardbxConnection();
            Statement stmt = polardbxConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(reimportSql, phyDatabaseName, instName))
        ) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
                Assert.assertTrue("ALL SUCCESS".equalsIgnoreCase(result));
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //expand nodes to rebalance
        final String alterLocalitySql = "alter database \"%s\" set locality=\"dn=%s\"";

        try (Connection polardbxConn = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format(alterLocalitySql, phyDatabaseName,
                String.join(",",
                    instNameList.subList(0, instNameList.size())))
            );
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        checkRebalanceResult(phyDatabaseName);

        //do repartition
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb0 partition by key(id, name) partitions 36"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(
                    "alter table tb1 partition by List(id) (partition p1 values in(0,1,2), partition p2 values in(3,4,5), partition p3 values in(1000,1001), partition pd values in(default))"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(
                    "alter table tb2 partition by range(id) (partition p1 values less than(10), partition p2 values less than (20), partition pd values less than(maxvalue))"));

            JdbcUtil.executeUpdateSuccess(polardbxConn, "create tablegroup tg_test0");
            JdbcUtil.executeUpdateSuccess(polardbxConn, "create tablegroup tg_test1");
            JdbcUtil.executeUpdateSuccess(polardbxConn, "create tablegroup tg_test2");
            JdbcUtil.executeUpdateSuccess(polardbxConn, "alter table tb0 set tablegroup='tg_test0'");
            JdbcUtil.executeUpdateSuccess(polardbxConn, "alter table tb1 set tablegroup='tg_test1'");
            JdbcUtil.executeUpdateSuccess(polardbxConn, "alter table tb2 set tablegroup='tg_test2'");

        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //test alter
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("ALTER TABLEGROUP tg_test0 SPLIT INTO hp PARTITIONS 3 BY HOT VALUE(1);"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter tablegroup tg_test1 drop partition p3"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter tablegroup tg_test2 merge partitions p1,p2 to p1_2"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                RepartitionTest.checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                RepartitionTest.checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //try to clean
        try (Connection polardbxConn = getPolardbxConnection()) {
            cleanDatabse(polardbxConn, phyDatabaseName);
        } catch (Exception ignore) {
        }
    }

    public void runTestCases() {
        try {
            testMoveSingleTableAndGroup();
            testAlterTablePartition();
            testAlterTableGrpupPartition();
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            for (String schema : schemaNames) {
                //clean
                try (Connection polardbxConn = getPolardbxConnection()) {
                    cleanDatabse(polardbxConn, schema);
                } catch (Exception ignore) {
                }
            }
        }
    }

    private void preparePhyTables(Connection storageInstConn, String phyDbName, int tableNum, int dataRows) {
        final String paddingTables = "create table %s("
            + "id int primary key,"
            + "name varchar(50)"
            + ");";
        final String paddingData = "insert into %s ("
            + "id,"
            + "name"
            + ")"
            + "values (?, ?)";

        final String useSql = "use `" + phyDbName + "`";

        JdbcUtil.executeQuerySuccess(storageInstConn, useSql);

        for (int i = 0; i < tableNum; i++) {
            final String tbName = TB_NAME_PATTERN + i;
            final String createSql = String.format(paddingTables, tbName);
            JdbcUtil.executeUpdateSuccess(storageInstConn, createSql);
            final String insertDataSql = String.format(paddingData, tbName);

            try (PreparedStatement statement = storageInstConn.prepareStatement(insertDataSql)) {
                for (int j = 0; j < dataRows; j++) {
                    statement.setInt(1, j);
                    statement.setString(2, String.valueOf(j));
                    statement.addBatch();
                }
                statement.executeBatch();
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }
        }
    }

}

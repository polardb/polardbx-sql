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
 * We need to test whether the imported table could be repartition
 */
public class RepartitionTest extends ImportDatabaseBase {
    final static String TB_NAME_PATTERN = "tb";
    final static List<String> schemaNames = ImmutableList.of(
        "repartition_and_import_db0s",  //测试库名后缀带s会不会报错
        "td1" //short db name test
    );

    void testRepartition() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schemaNames.get(0);

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

        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
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

        //do repartition
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb0 partition by hash(id) partitions 1"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb1 partition by key(id) partitions 3"));
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format("alter table tb2 broadcast"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //do repartition again
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb0 partition by hash(name) partitions 3"));
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format("alter table tb1 single"));
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format("alter table tb2 partition by key(id)"));
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format("alter table tb3 broadcast"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
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

    void testRebalanceThenRepartition() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        List<String> instNameList =
            storageInstList.stream().map(x -> x.storageInstId).distinct().collect(Collectors.toList());
        String instName = instNameList.get(0);
        final String phyDatabaseName = schemaNames.get(1);

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

        //repartition
        //do repartition
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb0 partition by hash(id) partitions 1"));
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb1 partition by key(id) partitions 3"));
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format("alter table tb2 broadcast"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //shrink nodes to rebalance
        final String alterLocalitySql2 = "alter database \"%s\" set locality=\"dn=%s\"";

        try (Connection polardbxConn = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format(alterLocalitySql2, phyDatabaseName,
                String.join(",",
                    instNameList.subList(0, 2)))
            );
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        checkRebalanceResult(phyDatabaseName);

        //do repartition again
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format("alter table tb0 partition by hash(name) partitions 3"));
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format("alter table tb1 single"));
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format("alter table tb2 partition by key(id)"));
            JdbcUtil.executeUpdateSuccess(polardbxConn, String.format("alter table tb3 broadcast"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        //check data and readwrite
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            for (int i = 0; i < 4; i++) {
                checkTableData(polardbxConn, phyDatabaseName, "tb" + i, (0L + 49) * 50 / 2, 50L);
                checkTableReadWrite(polardbxConn, phyDatabaseName, "tb" + i, 9999L);
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

    public static void checkTableData(Connection conn, String schema, String table, Long dataSum, Long dataRows) {
        final String useSql = "use `" + schema + "`";
        final String sumSql = "select sum(id) from %s";
        final String countSql = "select count(1) from %s";

        JdbcUtil.executeQuerySuccess(conn, useSql);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, String.format(sumSql, table));
        ) {
            Long result = null;
            if (rs.next()) {
                result = rs.getLong(1);
            }
            Assert.assertTrue(dataSum.equals(result));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, String.format(countSql, table));
        ) {
            Long result = null;
            if (rs.next()) {
                result = rs.getLong(1);
            }
            Assert.assertTrue(dataRows.equals(result));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static void checkTableReadWrite(Connection conn, String schema, String table, Long pkId) {
        final String useSql = "use `" + schema + "`";
        final String insertSql = "insert into %s(id, name)values (%s, %s)";
        final String querySql = "select id from %s where id = %s";
        final String updateSql = "update %s set id = %s where id = %s";
        final String deleteSql = "delete from %s where id = %s";

        JdbcUtil.executeQuerySuccess(conn, useSql);

        //insert a record
        JdbcUtil.executeUpdateSuccess(conn, String.format(insertSql, table, pkId, pkId));

        //check insert succeed
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, String.format(querySql, table, pkId))) {
            Long result = null;
            if (rs.next()) {
                result = rs.getLong(1);
            }
            Assert.assertTrue(pkId.equals(result));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //update a record
        JdbcUtil.executeUpdateSuccess(conn, String.format(updateSql, table, pkId + 1, pkId));
        //check update succeed
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, String.format(querySql, table, pkId + 1))) {
            Long result = null;
            if (rs.next()) {
                result = rs.getLong(1);
            }
            Assert.assertTrue(result != null && result.equals(pkId + 1));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //delete a rocord
        JdbcUtil.executeUpdateSuccess(conn, String.format(deleteSql, table, pkId + 1));

        //check delete succeed
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, String.format(querySql, table, pkId + 1))) {
            Long result = null;
            if (rs.next()) {
                result = rs.getLong(1);
            }
            Assert.assertTrue(result == null);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public void runTestCases() {
        try {
            testRepartition();
            testRebalanceThenRepartition();
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
}

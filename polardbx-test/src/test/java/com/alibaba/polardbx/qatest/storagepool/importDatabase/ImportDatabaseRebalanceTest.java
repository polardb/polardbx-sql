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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * We need to test whether the imported table & database can be rebalanced
 */
public class ImportDatabaseRebalanceTest extends ImportDatabaseBase {
    final static String TB_NAME_PATTERN = "tb";

    final static List<String> schameNames = ImmutableList.of(
        "single_db_rebalance_db",
        "multi_db_rebalance_db1s",  //测试库名后缀带s会不会报错
        "multi_db_rebalance_db2",
        "multi_db_rebalance_db3S",
        "td4"  //short db name test

    );

    private void preparePhyTables(Connection storageInstConn, String phyDbName, int tableNum, int dataRows) {
        final String paddingTables = "CREATE TABLE %s (\n"
            + "    col_int INT primary key auto_increment,\n"
            + "    col_float FLOAT,\n"
            + "    col_double DOUBLE,\n"
            + "    col_decimal DECIMAL(10,2),\n"
            + "    col_varchar VARCHAR(255),\n"
            + "    col_char CHAR(128),\n"
            + "    col_text TEXT,\n"
            + "    col_date DATE,\n"
            + "    col_datetime DATETIME,\n"
            + "    col_time TIME,\n"
            + "    col_boolean BOOLEAN\n"
            + ");";

        final String paddingData = "INSERT INTO %s ("
            + "col_int,"
            + "col_float, "
            + "col_double, "
            + "col_decimal, "
            + "col_varchar, "
            + "col_char, "
            + "col_text, "
            + "col_date, "
            + "col_datetime, "
            + "col_time, "
            + "col_boolean"
            + ") "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        final String useSql = "use `" + phyDbName + "`";

        JdbcUtil.executeQuerySuccess(storageInstConn, useSql);
        for (int i = 0; i < tableNum; i++) {
            final String tbName = TB_NAME_PATTERN + i;
            final String createSql = String.format(paddingTables, tbName);
            JdbcUtil.executeUpdateSuccess(storageInstConn, createSql);
            final String insertDataSql = String.format(paddingData, tbName);

            try (PreparedStatement statement = storageInstConn.prepareStatement(insertDataSql)) {
                for (int j = 0; j < dataRows; j++) {
                    statement.setNull(1, java.sql.Types.INTEGER);
                    statement.setFloat(2, (float) Math.random() * 100);
                    statement.setDouble(3, Math.random() * 100);
                    statement.setDouble(4, Math.random() * 100);
                    statement.setString(5, "varchar_" + Math.random());
                    statement.setString(6, "char_" + Math.random());
                    statement.setString(7, "text_" + Math.random());
                    statement.setDate(8, new java.sql.Date(System.currentTimeMillis()));
                    statement.setTimestamp(9, new java.sql.Timestamp(System.currentTimeMillis()));
                    statement.setTime(10, new java.sql.Time(System.currentTimeMillis()));
                    statement.setBoolean(11, Math.random() < 0.5);
                    statement.addBatch();
                }
                statement.executeBatch();
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }
        }
    }

    void testSingle2MultiDnRebalance() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schameNames.get(0);

        List<String> instIdList = storageInstList.stream()
            .map(x -> x.storageInstId)
            .distinct()
            .collect(Collectors.toList());

        //prepare phy database and tables
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
            Connection polardbxConnection = getPolardbxConnection()) {
            //pre clean
            cleanDatabse(polardbxConnection, phyDatabaseName);
            cleanDatabse(storageInstConn, phyDatabaseName);

            //create database
            preparePhyDatabase(phyDatabaseName, storageInstConn);

            //prepare tables
            preparePhyTables(storageInstConn, phyDatabaseName, 3, 50);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //import database
        final String importSql = "import database `" + phyDatabaseName + "` locality=\'dn=" + instName + "\'";
        try (Connection polardbxConnection = getPolardbxConnection();
            Statement stmt = polardbxConnection.createStatement();
            ResultSet rs = stmt.executeQuery(importSql)
        ) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
            }

            Assert.assertTrue(result != null && result.equalsIgnoreCase("ALL SUCCESS"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //test reimport database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName)) {
            JdbcUtil.executeUpdateSuccess(storageInstConn, "use " + phyDatabaseName);
            JdbcUtil.executeUpdateSuccess(storageInstConn, "alter table tb0 add index idx1(col_char)");
            JdbcUtil.executeUpdateSuccess(storageInstConn, "alter table tb1 modify column col_char varchar(255)");
            JdbcUtil.executeUpdateSuccess(storageInstConn,
                "alter table tb2 add column addr varchar(100) default \"hangzhou\"");
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

        //alter locality
        final String alterLocalitySql = "alter database `" + phyDatabaseName + "` set locality=\" dn="
            + String.join(",", instIdList) + "\"";
        try (Connection polardbxConnection = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(polardbxConnection, alterLocalitySql);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        checkRebalanceResult(phyDatabaseName);

        //shrink nodes
        final String alterLocalityShrinkSql = "alter database `" + phyDatabaseName + "` set locality=\" dn="
            + instName + "\"";
        try (Connection polardbxConn = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(polardbxConn, alterLocalityShrinkSql);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        checkRebalanceResult(phyDatabaseName);

        //try to clean
        try (Connection polardbxConn = getPolardbxConnection()) {
            cleanDatabse(polardbxConn, phyDatabaseName);
        } catch (Exception ignore) {
        }
    }

    void testMulti2MultiDnRebalance() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        List<String> distinctInstName =
            storageInstList.stream().map(x -> x.storageInstId).distinct().collect(Collectors.toList());
        final int instNum = distinctInstName.size();
        Assert.assertTrue(instNum > 6);
        Map<String, String> phyDbAndInstName = new TreeMap<>(String::compareToIgnoreCase);
        for (int i = 1; i < 5; i++) {
            phyDbAndInstName.put(schameNames.get(i), distinctInstName.get(i - 1));
        }

        for (Map.Entry<String, String> entry : phyDbAndInstName.entrySet()) {
            String phyDb = entry.getKey();
            String instName = entry.getValue();
            try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
                Connection polardbXConn = getPolardbxConnection()) {
                //pre clean
                cleanDatabse(polardbXConn, phyDb);
                cleanDatabse(storageInstConn, phyDb);

                //prepare
                preparePhyDatabase(phyDb, storageInstConn);
                //prepare tables
                preparePhyTables(storageInstConn, phyDb, 2, 30);
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }
        }

        //import all databases
        final String importSql = "import database `%s` locality=\"dn=%s\"";
        for (Map.Entry<String, String> entry : phyDbAndInstName.entrySet()) {
            String phyDb = entry.getKey();
            String instName = entry.getValue();
            String importDb = String.format(importSql, phyDb, instName);
            try (Connection polardbxConn = getPolardbxConnection();
                Statement stmt = polardbxConn.createStatement();
                ResultSet rs = stmt.executeQuery(importDb);
            ) {
                String result = null;
                while (rs.next()) {
                    result = rs.getString("STATE");
                    Assert.assertTrue("ALL SUCCESS".equalsIgnoreCase(result));
                }
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }
        }

        //add new storage inst, then alter locality to rebalance
        Set<String> newInstToRebalance = new TreeSet<>(String::compareToIgnoreCase);
        Set<String> tobeAvoid = new TreeSet<>(String::compareToIgnoreCase);
        tobeAvoid.addAll(phyDbAndInstName.values());
        for (int i = 0; i < 4; i++) {
            String newInst = randomChooseOne(
                distinctInstName,
                tobeAvoid
            );
            if (newInst != null) {
                newInstToRebalance.add(newInst);
                tobeAvoid.add(newInst);
            }
        }

        //expand nodes
        final String alterLocalitySql = "alter database \"%s\" set locality=\"dn=%s\"";
        for (Map.Entry<String, String> entry : phyDbAndInstName.entrySet()) {
            String phyDb = entry.getKey();
            String instName = entry.getValue();
            String sql = String.format(alterLocalitySql, phyDb, phyDbAndInstName.values().stream().collect(
                Collectors.joining(",")) + "," + String.join(",", newInstToRebalance));
            try (Connection polardbxConn = getPolardbxConnection()) {
                JdbcUtil.executeUpdateSuccess(polardbxConn, sql);
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }

            checkRebalanceResult(phyDb);
        }

        //shrink nodes
        for (Map.Entry<String, String> entry : phyDbAndInstName.entrySet()) {
            String phyDb = entry.getKey();
            String instName = entry.getValue();
            String sql = String.format(alterLocalitySql, phyDb, phyDbAndInstName.values().stream().collect(
                Collectors.joining(",")));
            try (Connection polardbxConn = getPolardbxConnection()) {
                JdbcUtil.executeUpdateSuccess(polardbxConn, sql);
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }

            checkRebalanceResult(phyDb);
        }

        for (Map.Entry<String, String> entry : phyDbAndInstName.entrySet()) {
            String phyDb = entry.getKey();
            //try to clean
            try (Connection polardbxConn = getPolardbxConnection()) {
                cleanDatabse(polardbxConn, phyDb);
            } catch (Exception e) {
            }
        }
    }

    public void runTestCases() {
        try {
            testSingle2MultiDnRebalance();
            testMulti2MultiDnRebalance();
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            for (String schema : schameNames) {
                //clean
                try (Connection polardbxConn = getPolardbxConnection()) {
                    cleanDatabse(polardbxConn, schema);
                } catch (Exception ignore) {
                }
            }

        }
    }
}

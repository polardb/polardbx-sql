package com.alibaba.polardbx.qatest.storagepool.importDatabase;

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

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class HintTest extends ImportDatabaseBase {
    final static String TB_NAME_PATTERN = "tb";
    final static List<String> schemaNames = ImmutableList.of(
        "test_hint_db0",
        "test_hint_db1s",  //测试库名后缀带s会不会报错
        "test_hint_db2",
        "test_hint_db3"
    );

    void testDropLogicalTable() {
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
            preparePhyTables(storageInstConn, phyDatabaseName, 3, 1);
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

        //drop logical layer table
        final String dropTableSql = "/*+TDDL: IMPORT_TABLE=true */ drop table %s";
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(dropTableSql, "tb0"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //check logical not exists and phy exists
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName);
            Connection storageConn = buildJdbcConnectionByStorageInstId(instName)) {
            //logical table not exist
            Assert.assertTrue(!checkTableExist(polardbxConn, phyDatabaseName, "tb0"));
            //physical table exist
            Assert.assertTrue(checkTableExist(storageConn, phyDatabaseName, "tb0"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //drop both logical and phy layer
        final String dropTableSql2 = "drop table %s";
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(dropTableSql2, "tb1"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        //check logical not exists and phy table not exists
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName);
            Connection storageConn = buildJdbcConnectionByStorageInstId(instName)) {
            //logical table not exist
            Assert.assertTrue(!checkTableExist(polardbxConn, phyDatabaseName, "tb1"));
            //physical table not exist
            Assert.assertTrue(!checkTableExist(storageConn, phyDatabaseName, "tb1"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //drop both logical and phy layer
        final String dropTableSql3 = "/*+TDDL: IMPORT_TABLE=false */ drop table %s";
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(dropTableSql3, "tb2"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        //check logical not exists and phy table not exists
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName);
            Connection storageConn = buildJdbcConnectionByStorageInstId(instName)) {
            //logical table not exist
            Assert.assertTrue(!checkTableExist(polardbxConn, phyDatabaseName, "tb2"));
            //physical table not exist
            Assert.assertTrue(!checkTableExist(storageConn, phyDatabaseName, "tb2"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //try to clean
        try (Connection polardbxConn = getPolardbxConnection()) {
            cleanDatabse(polardbxConn, phyDatabaseName);
        } catch (Exception ignore) {
        }
    }

    void testDropLogicalDatabase() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName1 = schemaNames.get(1);
        final String phyDatabaseName2 = schemaNames.get(2);
        final String phyDatabaseName3 = schemaNames.get(3);

        //prepare phy database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
            Connection polardbxConn = getPolardbxConnection()
        ) {
            //pre clean
            cleanDatabse(polardbxConn, phyDatabaseName1);
            cleanDatabse(storageInstConn, phyDatabaseName1);
            //create database
            preparePhyDatabase(phyDatabaseName1, storageInstConn);
            //prepare tables
            preparePhyTables(storageInstConn, phyDatabaseName1, 1, 1);

            //pre clean
            cleanDatabse(polardbxConn, phyDatabaseName2);
            cleanDatabse(storageInstConn, phyDatabaseName2);
            //create database
            preparePhyDatabase(phyDatabaseName2, storageInstConn);
            //prepare tables
            preparePhyTables(storageInstConn, phyDatabaseName2, 1, 1);

            //pre clean
            cleanDatabse(polardbxConn, phyDatabaseName3);
            cleanDatabse(storageInstConn, phyDatabaseName3);
            //create database
            preparePhyDatabase(phyDatabaseName3, storageInstConn);
            //prepare tables
            preparePhyTables(storageInstConn, phyDatabaseName3, 1, 1);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //import
        final String importSql = "import database `%s` locality=\"dn=%s\"";
        for (int i = 1; i <= 3; i++) {
            try (Connection polardbxConn = getPolardbxConnection();
                Statement stmt = polardbxConn.createStatement();
                ResultSet rs = stmt.executeQuery(String.format(importSql, schemaNames.get(i), instName))
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

        //drop logical layer database
        final String dropTableSql = "/*+TDDL: IMPORT_DATABASE=true */ drop database %s";
        try (Connection polardbxConn = getPolardbxConnection("polardbx")) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(dropTableSql, phyDatabaseName1));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //check logical not exists but phy exists
        try (Connection polardbxConn = getPolardbxConnection("polardbx");
            Connection storageConn = buildJdbcConnectionByStorageInstId(instName)) {
            //logical db not exist
            Assert.assertTrue(!checkDatabaseExist(polardbxConn, phyDatabaseName1));
            //physical db exist
            Assert.assertTrue(checkDatabaseExist(storageConn, phyDatabaseName1));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //drop both logical and phy layer database
        final String dropTableSql2 = "drop database %s";
        try (Connection polardbxConn = getPolardbxConnection("polardbx")) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(dropTableSql2, phyDatabaseName2));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //check logical not exists and phy not exists
        try (Connection polardbxConn = getPolardbxConnection("polardbx");
            Connection storageConn = buildJdbcConnectionByStorageInstId(instName)) {
            //logical db not exist
            Assert.assertTrue(!checkDatabaseExist(polardbxConn, phyDatabaseName2));
            //physical db exist
            Assert.assertTrue(!checkDatabaseExist(storageConn, phyDatabaseName2));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //drop both logical and phy layer database
        final String dropTableSql3 = "/*+TDDL: IMPORT_DATABASE=false */ drop database %s";
        try (Connection polardbxConn = getPolardbxConnection("polardbx")) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(dropTableSql3, phyDatabaseName3));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //check logical not exists and phy not exists
        try (Connection polardbxConn = getPolardbxConnection("polardbx");
            Connection storageConn = buildJdbcConnectionByStorageInstId(instName)) {
            //logical db not exist
            Assert.assertTrue(!checkDatabaseExist(polardbxConn, phyDatabaseName3));
            //physical db exist
            Assert.assertTrue(!checkDatabaseExist(storageConn, phyDatabaseName3));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //try to clean
        try (Connection polardbxConn = getPolardbxConnection();
            Connection storageConn = buildJdbcConnectionByStorageInstId(instName)
        ) {
            cleanDatabse(storageConn, phyDatabaseName1);
            cleanDatabse(polardbxConn, phyDatabaseName1);
            cleanDatabse(storageConn, phyDatabaseName2);
            cleanDatabse(polardbxConn, phyDatabaseName2);
            cleanDatabse(storageConn, phyDatabaseName3);
            cleanDatabse(polardbxConn, phyDatabaseName3);
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

    public void runTestCases() {
        try {
            testDropLogicalTable();
            testDropLogicalDatabase();
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

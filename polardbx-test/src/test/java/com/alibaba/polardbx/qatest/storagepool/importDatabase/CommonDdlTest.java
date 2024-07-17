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
import java.util.stream.Collectors;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class CommonDdlTest extends ImportDatabaseBase {
    final static String TB_NAME_PATTERN = "tb";
    final static List<String> schemaNames = ImmutableList.of(
        "common_ddl_db0"
    );

    private void testExecuteDdl() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schemaNames.get(0);
        List<String> instNameList =
            storageInstList.stream().map(x -> x.storageInstId).distinct().collect(Collectors.toList());
        Assert.assertTrue(instNameList.size() > 2);

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
            JdbcUtil.executeUpdateSuccess(storageInstConn,
                "alter table tb3 add column addr varchar(128) default \"hangzhou\"");
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

        //move tablegroup
        final String moveTgSql = "alter tablegroup single_tg move partitions p1 to '%s'";
        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName);
        ) {
            JdbcUtil.executeUpdateSuccess(polardbxConn,
                String.format(moveTgSql, instNameList.get(2)));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        try (Connection polardbxConn = getPolardbxConnection(phyDatabaseName)) {
            JdbcUtil.executeUpdateSuccess(polardbxConn, "alter table tb0 add local index idx1(name)");
            JdbcUtil.executeUpdateSuccess(polardbxConn, "alter table tb1 modify column name varchar(256)");
            JdbcUtil.executeUpdateSuccess(polardbxConn, "alter table tb2 add column addr varchar(128) default null");
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
            testExecuteDdl();
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

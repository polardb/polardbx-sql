package com.alibaba.polardbx.qatest.storagepool.importDatabase;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ImportTableTest extends ImportDatabaseBase {

    final static List<String> schameNames = ImmutableList.of(
        "import_tb_test_succeed",
        "import_tb_test_fk_succeed",
        "error_case_database"
    );
    private static final List<String> shouldSucceedCases = ImmutableList.of(
        //normal test
        "create table t1 ("
            + "b char(0) not null,"
            + " pk int primary key"
            + ")",

        //engine = innodb/myisam
        "create table t2 ("
            + "a int not null auto_increment,"
            + "primary key (a)"
            + ") engine=innodb",

        //engine = innodb/myisam
        "create table t3 ("
            + "a int not null auto_increment,"
            + "primary key (a)"
            + ") engine=myisam",

        //dummy table names
        "create table `a/a` ("
            + "a int,"
            + " pk int primary key"
            + ");",

        "create table 1ea10 ("
            + "1a20 int,"
            + "1e int,"
            + "pk int primary key"
            + ");",

        "create table import_tb_test_succeed.$test1 ("
            + "a$1 int,"
            + " $b int,"
            + " c$ int,"
            + " pk int primary key"
            + ");",

        "create table import_tb_test_succeed.test2$ ("
            + "a int,"
            + "pk int primary key"
            + ");",

        //32 indexes
        "create table t4 ("
            + "a int not null, "
            + "b int, "
            + "primary key(a), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b),"
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b), "
            + "key (b)"
            + ");",

        //type test
        "create table t5("
            + "a int,"
            + "b int,"
            + "c int unsigned,"
            + "d date,"
            + "e char,"
            + "f datetime,"
            + "g time,"
            + "h blob,"
            + "pk int primary key"
            + ");",

        "create table t6 ("
            + "pk int primary key,"
            + "a tinyint, "
            + "b smallint, "
            + "c mediumint, "
            + "d int, "
            + "e bigint, "
            + "f float(3,2),"
            + "g double(4,3), "
            + "h decimal(5,4), "
            + "i year, "
            + "j date, "
            + "k timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
            + "l datetime, "
            + "m enum('a','b'), "
            + "n set('a','b'), "
            + "o char(10)"
            + ");",

        //default
        "create table t7("
            + "pk int primary key,"
            + "str varchar(10) default 'def',"
            + "strnull varchar(10),"
            + "intg int default '10',"
            + "rel double default '3.14'"
            + ");",

        "create table t8("
            + "pk int primary key, "
            + "name varchar(10), "
            + "age smallint default -1"
            + ");",

        "create table t9 ("
            + "pk int primary key, "
            + "b bool not null default false"
            + ");",

        //using
        "CREATE TABLE t10("
            + "pk int primary key, "
            + "c1 VARCHAR(33), "
            + "KEY USING BTREE (c1)"
            + ");",

        "CREATE TABLE t11("
            + "pk int primary key,"
            + "c1 VARCHAR(33), "
            + "KEY (c1) USING BTREE"
            + ");",

        "CREATE TABLE t12("
            + "pk int primary key,"
            + "c1 VARCHAR(33), "
            + "KEY USING BTREE (c1) USING HASH"
            + ");"
    );

    private static final List<String> foreignKeyTest = ImmutableList.of(
        // foreign keys
        "create table tfk1 ("
            + "a int,"
            + "key(a),"
            + "pk int primary key"
            + ");",

        "create table tfk2 ("
            + "b int,"
            + "foreign key(b) references tfk1(a), "
            + "key(b), "
            + "pk int primary key"
            + ");",

        "create table tfk3 ("
            + "c int,"
            + "foreign key(c) references tfk2(b), "
            + "key(c), "
            + "pk int primary key"
            + ");",

        "create table tfk4 ("
            + "d int,"
            + "foreign key(d) references tfk3(c), "
            + "key(d), "
            + "pk int primary key"
            + ");",
        "create table tfk5 ("
            + "e int,"
            + "foreign key(e) references tfk4(d), "
            + "key(e), "
            + "pk int primary key"
            + ");",
        "create table tfk6 ("
            + "f int,"
            + "foreign key(f) references tfk5(e), "
            + "key(f), "
            + "pk int primary key"
            + ");"
    );

    private static final List<String> errorTableTest = ImmutableList.of(
        //unsupported column charset
        "CREATE TABLE `tb0` (\n"
            + "`name` varchar(20) CHARACTER SET armscii8 DEFAULT NULL,\n"
            + "`id` int(11) NOT NULL,\n"
            + "PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8",

        //unsupport table charset
        "CREATE TABLE `tb1` (\n"
            + "  `name` varchar(20) DEFAULT NULL,\n"
            + "  `id` int(11) NOT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=cp1256",

        //not contained primary key
        "CREATE TABLE `tb2` (\n"
            + "  `name` varchar(20) DEFAULT NULL,\n"
            + "  `addr` text\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8",

        //unsupported engine
        "CREATE TABLE `tb3` (\n"
            + "  `id` int(11) NOT NULL,\n"
            + "  `name` varchar(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=MEMORY DEFAULT CHARSET=utf8"
    );

    private void testImportVariousTableSucceed() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schameNames.get(0);

        //prepare phy database and tables
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
            Connection polardbxConnection = getPolardbxConnection();) {
            //pre clean
            cleanDatabse(polardbxConnection, phyDatabaseName);
            cleanDatabse(storageInstConn, phyDatabaseName);

            //create database
            preparePhyDatabase(phyDatabaseName, storageInstConn);

            //prepare tables
            preparePhyTables(shouldSucceedCases, storageInstConn, phyDatabaseName);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //test import database
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
    }

    private void testImportTablesContainedForeignKey() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schameNames.get(1);

        //prepare phy database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
            Connection polardbxConnection = getPolardbxConnection()) {
            //pre clean
            cleanDatabse(polardbxConnection, phyDatabaseName);
            cleanDatabse(storageInstConn, phyDatabaseName);
            //create database
            preparePhyDatabase(phyDatabaseName, storageInstConn);
            //prepare tables
            preparePhyTables(ImmutableList.of(foreignKeyTest.get(0)), storageInstConn, phyDatabaseName);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        final String importSql = "import database `" + phyDatabaseName + "` locality=\'dn=" + instName + "\'";
        //pre import
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

        //create all phy table
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName)) {
            preparePhyTables(new ArrayList<>(foreignKeyTest.subList(1, foreignKeyTest.size())), storageInstConn,
                phyDatabaseName);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        final String reimportSql =
            "import database if exists`" + phyDatabaseName + "` locality=\'dn=" + instName + "\'";
        //re import
        try (Connection polardbxConnection = getPolardbxConnection();
            Statement stmt = polardbxConnection.createStatement();
            ResultSet rs = stmt.executeQuery(reimportSql)
        ) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
            }

            Assert.assertTrue(result != null && result.equalsIgnoreCase("ALL SUCCESS"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

    }

    private void testErrorCase() {
        List<StorageInfoRecord> storageInstList = getAvailableStorageInfo();
        String instName = storageInstList.get(0).storageInstId;
        final String phyDatabaseName = schameNames.get(2);

        //prepare phy database
        try (Connection storageInstConn = buildJdbcConnectionByStorageInstId(instName);
            Connection polardbxConnection = getPolardbxConnection()) {
            //pre clean
            cleanDatabse(polardbxConnection, phyDatabaseName);
            cleanDatabse(storageInstConn, phyDatabaseName);
            //create database
            preparePhyDatabase(phyDatabaseName, storageInstConn);
            //prepare tables
            preparePhyTables(errorTableTest, storageInstConn, phyDatabaseName);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }

        //import
        final String importSql = "import database `" + phyDatabaseName + "` locality=\'dn=" + instName + "\'";
        try (Connection polardbxConnection = getPolardbxConnection();
            Statement stmt = polardbxConnection.createStatement();
            ResultSet rs = stmt.executeQuery(importSql)) {
            String result = null;
            while (rs.next()) {
                result = rs.getString("STATE");
                Assert.assertTrue("fail".equalsIgnoreCase(result));
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public void runTestCases() {
        try {
            enableForeignKey(true);
            testImportVariousTableSucceed();
            testImportTablesContainedForeignKey();
            enableForeignKey(false);
            testErrorCase();
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

    protected void enableForeignKey(boolean enable) {
        final String sql = (enable ? "set global enable_foreign_key = true" : "set global enable_foreign_key = false");
        try (Connection polardbxConn = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(polardbxConn, sql);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

}

package com.alibaba.polardbx.qatest.dml;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Rds80DirectTest extends AutoCrudBasedLockTestCase {
    private final Connection conn = getMysqlDirectConnection();
    private final Connection metaDBConn = getMetaConnection();

    private final String dbName = "Rds80DirectTestDb";

    @Before
    public void before() {
        JdbcUtil.executeSuccess(conn, "DROP DATABASE IF EXISTS " + dbName);
        JdbcUtil.executeSuccess(conn, "CREATE DATABASE " + dbName);
        JdbcUtil.executeSuccess(conn, "USE " + dbName);
    }

    @After
    public void after() {
        JdbcUtil.executeSuccess(conn, "DROP DATABASE IF EXISTS " + dbName);
    }

    @Test
    public void testGetTso() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.executeSuccess(conn, "create sequence " + dbName + ".gts_base cache 2 TIMESTAMP");
        long before = 0, after = 0;
        final String sql = "call dbms_tso.get_timestamp('" + dbName + "', 'gts_base',1)";
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql)) {
            Assert.assertTrue(sql + " return 0 row, expected 1.", rs.next());
            before = rs.getLong(1);
            // Only one row should be returned.
            Assert.assertFalse(sql + " return more than 1 row, expected 1.", rs.next());
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql)) {
            Assert.assertTrue(sql + " return 0 row, expected 1.", rs.next());
            after = rs.getLong(1);
            // Only one row should be returned.
            Assert.assertFalse(sql + " return more than 1 row, expected 1.", rs.next());
        }

        Assert.assertTrue("after.tso <= before.tso!", after > before);
    }

    @Test
    public void testGcnRelated() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        long tso = 0;
        String sql = "call dbms_tso.get_timestamp('mysql', 'gts_base',1)";
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(metaDBConn, sql)) {
            Assert.assertTrue(sql + "return 0 row, expected 1.", rs.next());
            tso = rs.getLong(1);
            // Only one row should be returned.
            Assert.assertFalse(sql + "return more than 1 row, expected 1.", rs.next());
        }

        JdbcUtil.executeSuccess(conn, "set innodb_snapshot_seq = " + tso);

        long snapshot = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "show variables like 'innodb_snapshot_seq'")) {
            Assert.assertTrue(sql + "return 0 row, expected 1.", rs.next());
            snapshot = rs.getLong(2);
            // Only one row should be returned.
            Assert.assertFalse(sql + "return more than 1 row, expected 1.", rs.next());
            Assert.assertEquals("set snapshot_tso != get snapshot_tso", tso, snapshot);
        }

        JdbcUtil.executeSuccess(conn, "set innodb_commit_seq = " + tso);

        long commit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "show variables like 'innodb_commit_seq'")) {
            Assert.assertTrue(sql + "return 0 row, expected 1.", rs.next());
            commit = rs.getLong(2);
            // Only one row should be returned.
            Assert.assertFalse(sql + "return more than 1 row, expected 1.", rs.next());
            Assert.assertEquals("set snapshot_tso != get snapshot_tso", tso, commit);
        }

        JdbcUtil.executeSuccess(conn, "set innodb_current_snapshot_seq = ON");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "show variables like 'innodb_current_snapshot_seq'")) {
            Assert.assertTrue(sql + "return 0 row, expected 1.", rs.next());
            String result = rs.getString(2);
            // Only one row should be returned.
            Assert.assertFalse(sql + "return more than 1 row, expected 1.", rs.next());
            Assert.assertTrue("set snapshot_tso != get snapshot_tso", "ON".equalsIgnoreCase(result));
        }

        JdbcUtil.executeSuccess(conn, "set innodb_current_snapshot_seq = OFF");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "show variables like 'innodb_current_snapshot_seq'")) {
            Assert.assertTrue(sql + "return 0 row, expected 1.", rs.next());
            String result = rs.getString(2);
            // Only one row should be returned.
            Assert.assertFalse(sql + "return more than 1 row, expected 1.", rs.next());
            Assert.assertTrue("set snapshot_tso != get snapshot_tso", "OFF".equalsIgnoreCase(result));
        }
    }

    @Test
    public void testSupportXRpc() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "show global variables like 'new_rpc'")) {
            Assert.assertTrue("Not found new_rpc variable", rs.next());
        }
    }

    @Test
    public void testReturning() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "call dbms_admin.show_native_procedure()")) {
            boolean found = false;
            while (rs.next()) {
                String schema, proc;
                schema = rs.getString("SCHEMA_NAME");
                proc = rs.getString("PROC_NAME");
                if ("dbms_trans".equalsIgnoreCase(schema) && "returning".equalsIgnoreCase(proc)) {
                    found = true;
                }
            }
            Assert.assertTrue("Not found dbms_trans.returning procedure.", found);
        }

        final String createTable = "CREATE TABLE `returning` (\n"
            + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\t`k` int(11) NOT NULL DEFAULT '0',\n"
            + "\t`c` char(120) NOT NULL DEFAULT '',\n"
            + "\t`pad` char(60) NOT NULL DEFAULT '',\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tKEY `k_1` (`k`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 2 DEFAULT CHARSET = utf8mb4";

        JdbcUtil.executeSuccess(conn, createTable);

        JdbcUtil.executeSuccess(conn, "insert into returning values(1,1,'a','a')");

        String sql =
            "call dbms_trans.returning(\"*\", 'insert ignore into returning values(1,1,\"a\",\"a\"),(2,2,\"b\",\"b\")')";
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql)) {
            Assert.assertTrue(sql + "return 0 row, expected 1.", rs.next());
            long id = rs.getLong("id");
            int k = rs.getInt("k");
            String c = rs.getString("c");
            String pad = rs.getString("pad");
            // Only one row should be returned.
            Assert.assertFalse(sql + "return more than 1 row, expected 1.", rs.next());

            // Only (2, 2, 'b', 'b') should be returned.
            Assert.assertEquals("expected id = 2, but id = " + id, 2, id);
            Assert.assertEquals("expected k = 2, but k = " + k, 2, k);
            Assert.assertTrue("expected c = b, but c = " + c, "b".equalsIgnoreCase(c));
            Assert.assertTrue("expected pad = b, but pad = " + c, "b".equalsIgnoreCase(pad));
        }
    }

    @Test
    public void testHashCheck() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        final String createTable = "create table t1(id int, name varchar(20))";
        JdbcUtil.executeSuccess(conn, createTable);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select hashcheck(id, name) from t1")) {
            Assert.assertTrue("select hashcheck return empty result set", rs.next());
            String result = rs.getString(1);
            Assert.assertTrue("select hashcheck empty table return not null, result: " + result,
                null == result || "NULL".equalsIgnoreCase(result));
            Assert.assertFalse("select hashcheck return more than 1 rows", rs.next());
        }

        JdbcUtil.executeSuccess(conn, "insert into t1 values(1, '1'), (2, null)");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select hashcheck(id, name) from t1")) {
            Assert.assertTrue("select hashcheck return empty result set", rs.next());
            String result = rs.getString(1);
            Assert.assertTrue("select hashcheck return wrong result, expect 885305594283465037 but is " + result,
                "885305594283465037".equalsIgnoreCase(result));
            Assert.assertFalse("select hashcheck return more than 1 rows", rs.next());
        }
    }

    @Test
    public void testBloomFilter() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        final String createTable = "CREATE TABLE `bloom_test` (\n"
            + "  `id` int(11) NOT NULL,\n"
            + "  `val` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ")";
        JdbcUtil.executeSuccess(conn, createTable);
        JdbcUtil.executeSuccess(conn, "insert into bloom_test values (1,2),(2,4)");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            "select * from bloom_test where bloomfilter('000', 1, 1, val)")) {
            Assert.assertFalse("select bloomfilter return not empty result set", rs.next());
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            "select * from bloom_test where bloomfilter('111', 1, 1, val)")) {
            Map<String, String> results = new HashMap<>();
            while (rs.next()) {
                results.put(rs.getString("id"), rs.getString("val"));
            }
            Assert.assertEquals("select bloomfilter return rows != 2 actual: " + results.size(), 2, results.size());
            Assert.assertEquals("Not found record (1, 2) for bloomfilter ", "2", results.get("1"));
            Assert.assertEquals("Not found record (2, 4) for bloomfilter ", "4", results.get("2"));
        }
    }

    @Test
    public void testXXHash() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "show variables like 'udf_bloomfilter_xxhash'")) {
            Assert.assertTrue("Not found udf_bloomfilter_xxhash variable", rs.next());
            String value = rs.getString(2);
            Assert.assertTrue("udf_bloomfilter_xxhash is not ON, but is " + value, "ON".equalsIgnoreCase(value));
            Assert.assertFalse("Return more than 1 row for udf_bloomfilter_xxhash variable", rs.next());
        }

        final String createTable = "CREATE TABLE `bloom_test` (\n"
            + "  `id` int(11) NOT NULL,\n"
            + "  `val` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ")";
        JdbcUtil.executeSuccess(conn, createTable);
        JdbcUtil.executeSuccess(conn, "insert into bloom_test values (1,2),(2,4)");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            "select * from bloom_test where bloomfilter('000', 1, 1, val, 1)")) {
            Assert.assertFalse("select bloomfilter return not empty result set", rs.next());
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            "select * from bloom_test where bloomfilter('111', 1, 1, val, 1)")) {
            Map<String, String> results = new HashMap<>();
            while (rs.next()) {
                results.put(rs.getString("id"), rs.getString("val"));
            }
            Assert.assertEquals("select bloomfilter return rows != 2 actual: " + results.size(), 2, results.size());
            Assert.assertEquals("Not found record (1, 2) for bloomfilter ", "2", results.get("1"));
            Assert.assertEquals("Not found record (2, 4) for bloomfilter ", "4", results.get("2"));
        }
    }

    @Test
    public void testHLL() {
        if (!isMySQL80()) {
            return;
        }

        final String createTable = "CREATE TABLE `bloom_test2` (\n"
            + "  `id` int(11) NOT NULL,\n"
            + "  `val` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ")";
        JdbcUtil.executeSuccess(conn, createTable);
        JdbcUtil.executeSuccess(conn, "insert into bloom_test2 values (1,2),(2,4)");

        JdbcUtil.executeQuerySuccess(conn, "select HYPERLOGLOG(val) from bloom_test2");
        JdbcUtil.executeQuerySuccess(conn, "select HYPERLOGLOG(concat(id, val)) from bloom_test2");
    }

    @Test
    public void testSample() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        final String createTable = "CREATE TABLE `test` (\n"
            + "  `c1` int(11) NOT NULL,\n"
            + "  `c2` int(11) NOT NULL,\n"
            + "  `c3` int(11) NOT NULL,\n"
            + "  PRIMARY KEY (`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        JdbcUtil.executeSuccess(conn, createTable);
        JdbcUtil.executeSuccess(conn,
            "insert into test "
                + "WITH RECURSIVE t(c1, c2, c3) "
                + "AS (SELECT 1, 1, 1 UNION ALL SELECT c1+1, c1 % 50, c1 %200 FROM t WHERE c1 < 1000) "
                + "SELECT c1, c2, c3 FROM t");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select/*+sample_percentage(1)*/ * from test")) {
            int rows = 0;
            while (rs.next()) {
                rows++;
            }
            Assert.assertTrue("Sampling return rows more than 100, rows: " + rows, rows < 100);
        }
    }

    // 8032 not supported
    @Ignore
    @Test
    public void testInventorHint() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        final String createTable = "CREATE TABLE `test2` (\n"
            + "  `c1` int(11) NOT NULL,\n"
            + "  `c2` int(11) NOT NULL,\n"
            + "  `c3` int(11) NOT NULL,\n"
            + "  PRIMARY KEY (`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        JdbcUtil.executeSuccess(conn, createTable);
        JdbcUtil.executeSuccess(conn, "insert into test2 values (1, 1, 1)");
        JdbcUtil.executeSuccess(conn, "insert into test2 values (2, 2, 2)");

        JdbcUtil.executeUpdateSuccess(conn, "BEGIN");
        try {
            JdbcUtil.executeUpdateSuccess(conn, "UPDATE /*+ commit_on_success*/ test2 SET c2 = 1000 WHERE c1 = 1");
        } finally {
            JdbcUtil.executeUpdateSuccess(conn, "ROLLBACK");
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select c2 from test2 where c1=1")) {
            Assert.assertTrue("select empty result.", rs.next());
            int result = rs.getInt(1);
            Assert.assertEquals("Expected 1000, but got " + result, 1000, result);
            Assert.assertFalse("select more than 1 row", rs.next());
        }

        JdbcUtil.executeUpdateSuccess(conn, "BEGIN");
        try {
            JdbcUtil.executeUpdateFailed(conn,
                "UPDATE /*+ commit_on_success rollback_on_fail*/ test2 SET c1 = c1 + 1 WHERE c1 = 1",
                "Duplicate entry '2' for key 'PRIMARY'");
            // Make sure trx is rolled back
            JdbcUtil.executeUpdateFailed(conn,
                "UPDATE /*+ commit_on_success rollback_on_fail*/ test2 SET c1 = c1 + 1 WHERE c1 = 1",
                "Inventory transactinal hints didn't allowed in autocommit mode");
        } finally {
            JdbcUtil.executeUpdateSuccess(conn, "ROLLBACK");
        }

        JdbcUtil.executeUpdateSuccess(conn, "BEGIN");
        try {
            JdbcUtil.executeUpdateFailed(conn,
                "UPDATE /*+ commit_on_success rollback_on_fail target_affect_row(2)*/test2 SET c1 = c1 + 1 WHERE c1 = 2",
                "Inventory conditional hints didn't match with result");
            // Make sure trx is rolled back
            JdbcUtil.executeUpdateFailed(conn,
                "UPDATE /*+ commit_on_success rollback_on_fail target_affect_row(2)*/test2 SET c1 = c1 + 1 WHERE c1 = 2;",
                "Inventory transactinal hints didn't allowed in autocommit mode");
            // Test success
            JdbcUtil.executeUpdateSuccess(conn, "BEGIN");
            JdbcUtil.executeUpdateSuccess(conn,
                "UPDATE /*+ commit_on_success rollback_on_fail target_affect_row(1)*/test2 SET c1 = c1 + 1 WHERE c1 = 2");
        } finally {
            JdbcUtil.executeUpdateSuccess(conn, "ROLLBACK");
        }

        // c1 is 3 now.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select * from test2 where c1=3")) {
            Assert.assertTrue("select empty result.", rs.next());
            Assert.assertFalse("select more than 1 row", rs.next());
        }

    }

    @Test
    public void testFollowerRead() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            "SELECT LAST_APPLY_INDEX FROM information_schema.ALISQL_CLUSTER_LOCAL")) {
            Assert.assertTrue("select lsn return empty result", rs.next());
            int lsn = rs.getInt(1);
            Assert.assertFalse("select lsn return more than 1 row ", rs.next());
            JdbcUtil.executeUpdateSuccess(conn, "set read_lsn = " + lsn);
            try (ResultSet rs2 = JdbcUtil.executeQuerySuccess(conn, "show variables like 'read_lsn'")) {
                Assert.assertTrue("no variables read_lsn", rs2.next());
                int read_lsn = rs2.getInt(2);
                Assert.assertEquals("expected read_lsn: " + lsn + ", but is " + read_lsn, lsn, read_lsn);
            }
        }
    }

    @Test
    public void testTransactionGroup() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            "show variables like 'innodb_transaction_group'")) {
            Assert.assertTrue("no variables innodb_transaction_group", rs.next());
        }

        String createTable = "CREATE TABLE `test3` (\n"
            + "  `id` int(11) NOT NULL,\n"
            + "  `val` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ")";

        JdbcUtil.executeSuccess(conn, createTable);
        JdbcUtil.executeSuccess(conn, "insert into test3 values (1, 2), (2, 4)");

        Connection conn2 = getMysqlDirectConnection(dbName);
        JdbcUtil.executeUpdateSuccess(conn, "set innodb_transaction_group =ON");
        JdbcUtil.executeUpdateSuccess(conn2, "set innodb_transaction_group =ON");
        JdbcUtil.executeUpdateSuccess(conn, "xa start 'drdstest', 'group@0001', 3");
        JdbcUtil.executeUpdateSuccess(conn2, "xa start 'drdstest', 'group@0002', 3");

        try {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select * from test3 where id = 1")) {
                Assert.assertTrue("empty result", rs.next());
                int val = rs.getInt(2);
                Assert.assertFalse("More than 1 row", rs.next());
                Assert.assertEquals("Expected 2 but is " + val, 2, val);
            }

            JdbcUtil.executeUpdateSuccess(conn2, "update test3 set val=222 where id =1");

            try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select * from test3 where id = 1")) {
                Assert.assertTrue("empty result", rs.next());
                int val = rs.getInt(2);
                Assert.assertFalse("More than 1 row", rs.next());
                Assert.assertEquals("Can not see changes from same group, expected 222 but is " + val, 222, val);
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(conn, "xa end 'drdstest', 'group@0001', 3");
            JdbcUtil.executeUpdateSuccess(conn2, "xa end 'drdstest', 'group@0002', 3");
            JdbcUtil.executeUpdateSuccess(conn, "xa rollback 'drdstest', 'group@0001', 3");
            JdbcUtil.executeUpdateSuccess(conn2, "xa rollback 'drdstest', 'group@0002', 3");
        }
    }

    @Test
    public void testAlterType() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.executeQueryFaied(conn, "select alter_type(1)", "Incorrect arguments to alter_type");
    }

    @Test
    public void testSupportHA() {
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.executeQuerySuccess(conn,
            "select role, current_leader, instance_type from information_schema.alisql_cluster_local limit 1");
        JdbcUtil.executeQuerySuccess(conn, "select role, ip_port from information_schema.alisql_cluster_global");
        JdbcUtil.executeQuerySuccess(conn, "select * from information_schema.alisql_cluster_health");
    }

    @Test
    public void testStorageInfo() throws SQLException {
        if (!isMySQL80()) {
            return;
        }

        Connection cnConn = getPolardbxConnection();
        Map<String, String> results = new HashMap<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(cnConn,
            "SELECT * FROM INFORMATION_SCHEMA.STORAGE_PROPERTIES")) {
            while (rs.next()) {
                results.put(rs.getString(1), rs.getString(2));
            }
        }

        Assert.assertEquals("NOT support XA", "TRUE", results.get("supportXA"));
        Assert.assertEquals("NOT support TSO", "TRUE", results.get("supportTso"));
        Assert.assertEquals("NOT support LIZARD 1PC", "TRUE", results.get("supportLizard1PCTransaction"));
        Assert.assertEquals("NOT support MDL LOCK INFO", "TRUE", results.get("supportMdlDeadlockDetection"));
        Assert.assertEquals("NOT support BLOOM FILTER", "TRUE", results.get("supportsBloomFilter"));
        Assert.assertEquals("NOT support OPEN SSL", "TRUE", results.get("supportOpenSSL"));
        Assert.assertEquals("NOT support SHARE READ VIEW", "TRUE", results.get("supportSharedReadView"));
        Assert.assertEquals("NOT support RETURNING", "TRUE", results.get("supportsReturning"));
        Assert.assertEquals("NOT support ALTER TYPE", "TRUE", results.get("supportsAlterType"));
        Assert.assertEquals("NOT support HYPER LOG LOG", "TRUE", results.get("supportHyperLogLog"));
        Assert.assertEquals("NOT support XX HASH", "TRUE", results.get("supportXxHash"));
    }

}

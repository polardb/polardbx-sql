package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ArchiveTransactionTest extends CrudBasedLockTestCase {
    @Test
    public void testSimpleCase() throws SQLException {
        String tableName = "test_archive_transaction";
        String dropSql = "drop table if exists " + tableName;
        String createSql =
            String.format(
                "create table if not exists %s (id int, name varchar(20), primary key(id)) dbpartition by hash(id)",
                tableName);
        try {
            JdbcUtil.executeUpdateSuccess(mysqlConnection, dropSql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
            JdbcUtil.executeSuccess(tddlConnection, "set transaction_policy = archive");
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show variables like 'transaction_policy'");
            boolean found = false;
            while (rs.next()) {
                if (rs.getString("Variable_name").equalsIgnoreCase("transaction_policy")
                    && rs.getString("Value").equalsIgnoreCase("archive")) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
            JdbcUtil.executeSuccess(tddlConnection, "begin");
            JdbcUtil.executeSuccess(tddlConnection, "insert into " + tableName + " values(1, 'a')");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show trans");
            found = false;
            while (rs.next()) {
                if (rs.getString("TYPE").equalsIgnoreCase("ARCHIVE")) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
            JdbcUtil.executeSuccess(tddlConnection, "commit");
        } finally {
            JdbcUtil.executeIgnoreErrors(tddlConnection, "rollback");
            JdbcUtil.executeIgnoreErrors(tddlConnection, "set transaction_policy = tso");
            JdbcUtil.executeIgnoreErrors(tddlConnection, dropSql);
        }

    }
}

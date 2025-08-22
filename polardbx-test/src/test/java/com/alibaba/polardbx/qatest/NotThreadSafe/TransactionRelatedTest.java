package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;

@NotThreadSafe
public class TransactionRelatedTest extends CrudBasedLockTestCase {
    @Test
    public void testSavepointNullColumn() throws Exception {
        String tableName = "testNullColumn_tb";
        try {
            String createTable = "create table if not exists " + tableName +
                "( id int not null auto_increment primary key, name varchar(30) not null)";
            JdbcUtil.executeSuccess(tddlConnection, createTable);
            JdbcUtil.executeSuccess(tddlConnection, "begin");
            JdbcUtil.executeSuccess(tddlConnection, "savepoint sa_savepoint_1");
            JdbcUtil.executeFailed(tddlConnection, "insert into " + tableName + " (name) values (null)",
                "Column 'name' cannot be null");
            JdbcUtil.executeSuccess(tddlConnection, "rollback to savepoint sa_savepoint_1");
            JdbcUtil.executeSuccess(tddlConnection, "savepoint sa_savepoint_2");
            JdbcUtil.executeSuccess(tddlConnection, "insert into " + tableName + " (name) values ('test_name_0')");
            JdbcUtil.executeSuccess(tddlConnection, "rollback to savepoint sa_savepoint_2");
            JdbcUtil.executeSuccess(tddlConnection, "insert into " + tableName + " (name) values ('test_name_1')");
            JdbcUtil.executeSuccess(tddlConnection, "commit");

            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName);
            if (rs.next()) {
                Assert.assertEquals("test_name_1", rs.getString("name"));
            } else {
                Assert.fail("no data");
            }
        } finally {
            String dropTable = "drop table if exists " + tableName;
            JdbcUtil.executeSuccess(tddlConnection, dropTable);
        }
    }
}

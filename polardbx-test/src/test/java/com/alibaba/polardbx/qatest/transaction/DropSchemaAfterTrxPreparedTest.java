package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;

public class DropSchemaAfterTrxPreparedTest extends CrudBasedLockTestCase {
    @Test
    public void testDropSchemaAfterTransPrepared() throws Exception {
        final String db1Name = "XAFailureTestDB1";
        final String db2Name = "XAFailureTestDB2";
        final String tbName = "XAFailureTestTB";
        final String createDB = "create database %s mode=drds";
        final String createTB = "create table if not exists " + tbName + "(id int)dbpartition by hash(id)";
        final String dropDB = "drop database if exists %s";
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_AFTER_PRIMARY_COMMIT') */";

        try {
            // Create new db and tb.
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropDB, db1Name));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropDB, db2Name));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createDB, db1Name));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createDB, db2Name));
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + db1Name);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTB);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + db2Name);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTB);

            // Starts a distributed trx.
            JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
            // Make db1 primary.
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                hint + "insert into " + db1Name + "." + tbName + " values (0), (1), (2), (3)");
            // Cross schema trx.
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                hint + "insert into " + db2Name + "." + tbName + " values (0), (1), (2), (3)");
            try {
                JdbcUtil.executeUpdate(tddlConnection, "commit");
            } catch (Throwable t) {
                t.printStackTrace();
            }
            // Drop db1.
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropDB, db1Name));
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + db2Name);

            // Let recover task rollback the prepared trx in db2.
            Thread.sleep(10 * 1000);
            final ResultSet rs =
                JdbcUtil.executeQuery("select data from information_schema.prepared_trx_branch", tddlConnection);
            while (rs.next()) {
                final String data = rs.getString("DATA");
                System.out.println(data);
                Assert.assertFalse(data.contains(db2Name.toUpperCase()));
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropDB, db2Name));
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + PropertiesUtil.polardbXDBName1(usingNewPartDb()));
        }

    }
}

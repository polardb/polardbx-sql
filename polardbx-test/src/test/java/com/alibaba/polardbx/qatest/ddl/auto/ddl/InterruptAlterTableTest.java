package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.SQLException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InterruptAlterTableTest extends InterruptDDLByLockTest {

    protected static String ALTER_TABLE = "ALTER TABLE " + TEST_TABLE + " %s %s";

    protected static final String MODIFY_COLUMN = "MODIFY COLUMN";
    protected static final String ADD_COLUMN = "ADD COLUMN";

    protected static final String ALTER_PREFIX = "Alter";

    @Before
    public void enableInject() {
        FailPoint.enable("FP_PHYSICAL_DDL_TIMEOUT", "true");
    }

    @After
    public void disableInject() {
        FailPoint.disable("FP_PHYSICAL_DDL_TIMEOUT");
    }

    @Test
    public void t01_normal_alter() throws SQLException {
        // Expected: run until success
        String columnInfo = "c2 varchar(200)";
        String sql = String.format(ALTER_TABLE, MODIFY_COLUMN, columnInfo);
        JobInfo job = executeDDL(sql);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, true);
    }

    @Test
    public void t11_cancel_add_then_rollback() throws SQLException {
        // Expected: ADD COLUMN can be rolled back regardless of whether the shards are done.
        String columnInfo = "c5 int(11)";
        String sql = String.format(ALTER_TABLE, ADD_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, false);
        cancelDDL(job);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, false);
    }

    @Test
    public void t12_cancel_modify_then_rollback() throws SQLException {
        // Expected: MODIFY COLUMN can be rolled back only if there isn't any shard done yet.
        String columnInfo = "c2 varchar(300)";
        String sql = String.format(ALTER_TABLE, MODIFY_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, true);
        cancelDDL(job);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, false);
    }

    @Test
    public void t13_cancel_modify_then_recover() throws SQLException {
        // Expected: MODIFY COLUMN cannot be rolled back if any shard has been done,
        // so the state MUST be PAUSED instead of ROLLBACK_PAUSED in such invalid state
        // the DDL job won't be able to be rolled back or recovered.
        String columnInfo = "c2 varchar(300)";
        String sql = String.format(ALTER_TABLE, MODIFY_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, false);
        waitForSeconds(2);
        cancelDDLFailed(job, "the DDL job has been paused instead");
        checkPhyProcess(job);
        checkJobPaused();
        cancelDDLFailed(job, "the DDL operations cannot be rolled back. Please try: continue ddl");
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, true);
    }

    /* bad case */
    @Test
    @Ignore
    public void t21_pause_modify_then_recover() throws SQLException {
        // Expected: MODIFY COLUMN can be paused, then recovered.
        String columnInfo = "c2 varchar(400)";
        String sql = String.format(ALTER_TABLE, MODIFY_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, false);
        pauseDDL(job);
        checkPhyProcess(job);
        checkJobPaused();
        cancelDDLFailed(job, "the DDL operations cannot be rolled back. Please try: continue ddl");
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, true);
    }

    /* bad case */
    @Test
    @Ignore
    public void t22_pause_add_then_rollback() throws SQLException {
        // Expected: ADD COLUMN can be paused, then rolled back.
        String columnInfo = "c5 int(11)";
        String sql = String.format(ALTER_TABLE, ADD_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, false);
        pauseDDL(job);
        checkPhyProcess(job);
        checkJobPaused();
        cancelUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, false);
    }

    @Test
    public void t31_kill_logical_modify_then_recover() throws SQLException {
        // Expected: MODIFY COLUMN can be killed, then recovered (equivalent to PAUSE DDL).
        String columnInfo = "c2 varchar(500)";
        String sql = String.format(ALTER_TABLE, MODIFY_COLUMN, columnInfo);
        JobInfo job = executeSeparateDDL(sql, ALTER_PREFIX, false);
        waitForSeconds(2);
        killLogicalProcess(ALTER_PREFIX);
        waitForSeconds(2);
        checkPhyProcess(job);
        checkJobPaused();
        cancelDDLFailed(job, "the DDL operations cannot be rolled back. Please try: continue ddl");
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, true);
    }

    @Test
    public void t32_kill_logical_add_then_rollback() throws SQLException {
        // Expected: ADD COLUMN can be killed, then rolled back (equivalent to PAUSE DDL).
        String columnInfo = "c5 int(11)";
        String sql = String.format(ALTER_TABLE, ADD_COLUMN, columnInfo);
        JobInfo job = executeSeparateDDL(sql, ALTER_PREFIX, false);
        killLogicalProcess(ALTER_PREFIX);
        waitForSeconds(2);
        checkPhyProcess(job);
        checkJobPaused();
        cancelUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, false);
    }

    @Test
    public void t41_kill_physical_modify_with_auto_rollback() throws SQLException {
        // Expected: All physical DDLs are killed (have to kill them one by one).
        String columnInfo = "c2 varchar(600)";
        String rollbackHint = "/*+TDDL:cmd_extra(PHYSICAL_DDL_TASK_RETRY=false)*/";
        String sql = rollbackHint + String.format(ALTER_TABLE, MODIFY_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, true);
        waitForSeconds(2);
        killPhysicalProcess();
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, false);
    }

    @Test
    public void t42_kill_physical_modify_then_recover() throws SQLException {
        // Expected: Partial physical DDLs are killed (have to kill them one by one).
        String columnInfo = "c2 varchar(600)";
        String rollbackHint = "/*+TDDL:cmd_extra(PHYSICAL_DDL_TASK_RETRY=false)*/";
        String sql = rollbackHint + String.format(ALTER_TABLE, MODIFY_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, false);
        waitForSeconds(2);
        killPhysicalProcess();
        checkPhyProcess(job);
        checkJobPaused();
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, true);
    }

    @Test
    public void t43_kill_physical_add_with_auto_rollback() throws SQLException {
        // Expected: All physical DDLs are killed (have to kill them one by one).
        String columnInfo = "c5 int(11)";
        String rollbackHint = "/*+TDDL:cmd_extra(PHYSICAL_DDL_TASK_RETRY=false)*/";
        String sql = rollbackHint + String.format(ALTER_TABLE, ADD_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, true);
        waitForSeconds(2);
        killPhysicalProcess();
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, false);
    }

    @Test
    @Ignore
    public void t44_kill_physical_add_with_auto_rollback() throws SQLException {
        // Expected: Partial physical DDLs are killed (have to kill them one by one).
        String columnInfo = "c5 int(11)";
        String rollbackHint = "/*+TDDL:cmd_extra(PHYSICAL_DDL_TASK_RETRY=false)*/";
        String sql = rollbackHint + String.format(ALTER_TABLE, ADD_COLUMN, columnInfo);
        JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, false);
        waitForSeconds(2);
        killPhysicalProcess();
        checkPhyProcess(job);
        checkJobGone();
        checkColumnConsistency(columnInfo, false);
    }

    @Test
    public void t51_all_physical_modify_with_timeout() throws SQLException {
        // Expected: All shards hit timeout, but tolerated.
        String columnInfo = "c2 varchar(700)";
        String sql = String.format(ALTER_TABLE, MODIFY_COLUMN, columnInfo);
        try {
            injectDDLTimeout(1000);
            JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, true);
            waitUntilJobCompletedOrPaused();
            checkPhyProcess(job);
            checkJobGone();
            checkColumnConsistency(columnInfo, true);
        } finally {
            injectDDLTimeout(0);
        }
    }

    @Test
    public void t52_partial_physical_add_with_timeout() throws SQLException {
        // Expected: partial shards hit timeout, but tolerated.
        String columnInfo = "c5 int(11)";
        if (isMySQL80()) {
            columnInfo = "c5 int";
        }
        String sql = String.format(ALTER_TABLE, ADD_COLUMN, columnInfo);
        try {
            injectDDLTimeout(1000);
            JobInfo job = executeAsyncDDL(sql, ALTER_PREFIX, false);
            waitUntilJobCompletedOrPaused();
            checkPhyProcess(job);
            checkJobGone();
            checkColumnConsistency(columnInfo, true);
        } finally {
            injectDDLTimeout(0);
        }
    }

    @Test
    public void t61_all_failed_before_physical_alter() throws SQLException {
        // Expected: ADD COLUMN will be rolled back since all shards fails and aren't done.
        all_failed_before_physical_alter(ADD_COLUMN, "c6 int(11)");
    }

    @Test
    public void t62_all_failed_before_physical_alter() throws SQLException {
        // Expected: MODIFY COLUMN will be rolled back since all shards fails and aren't done.
        all_failed_before_physical_alter(MODIFY_COLUMN, "c2 varchar(800)");
    }

    @Test
    public void t63_all_failed_after_physical_alter() throws SQLException {
        // Expected: ADD COLUMN will complete successfully since all shards are actually done.
        all_failed_after_physical_alter(ADD_COLUMN, "c6 int(11)");
    }

    @Test
    public void t64_all_failed_after_physical_alter() throws SQLException {
        // Expected: MODIFY COLUMN will complete successfully since all shards are actually done.
        all_failed_after_physical_alter(MODIFY_COLUMN, "c2 varchar(800)");
    }

    private void all_failed_before_physical_alter(String operation, String columnInfo) throws SQLException {
        all_failed_physical_alter("FP_BEFORE_PHYSICAL_DDL_EXCEPTION", operation, columnInfo, false);
    }

    private void all_failed_after_physical_alter(String operation, String columnInfo) throws SQLException {
        all_failed_physical_alter("FP_AFTER_PHYSICAL_DDL_EXCEPTION", operation, columnInfo, true);
    }

    @Test
    public void t65_partial_failed_before_physical_alter() throws SQLException {
        // Expected: partial shards will fail before physical DDLs,
        // then ADD COLUMN will be in ROLLBACK_PAUSED, then can only be continued to rollback.
        partial_failed_before_physical_alter(ADD_COLUMN, "c7 int(11)", false);
    }

    @Test
    public void t66_partial_failed_before_physical_alter() throws SQLException {
        // Expected: partial shards will fail before physical DDLs,
        // then MODIFY COLUMN will be in PAUSED, then can only be recovered.
        partial_failed_before_physical_alter(MODIFY_COLUMN, "c2 varchar(900)", true);
    }

    @Test
    public void t67_partial_failed_after_physical_alter() throws SQLException {
        // Expected: partial shards will fail after physical DDLs,
        // but ADD COLUMN will be automatically recovered eventually.
        partial_failed_after_physical_alter(ADD_COLUMN, "c7 int(11)");
    }

    @Test
    public void t68_partial_failed_after_physical_alter() throws SQLException {
        // Expected: partial shards will fail after physical DDLs,
        // but MODIFY COLUMN will be automatically recovered eventually.
        partial_failed_after_physical_alter(MODIFY_COLUMN, "c2 varchar(1000)");
    }

    public void partial_failed_before_physical_alter(String operation, String columnInfo, boolean expectedToExist)
        throws SQLException {
        partial_failed_physical_alter("FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION", operation, columnInfo,
            expectedToExist);
    }

    public void partial_failed_after_physical_alter(String operation, String columnInfo) throws SQLException {
        all_failed_physical_alter("FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION", operation, columnInfo, true);
    }

    private void all_failed_physical_alter(String failPointKey, String operation, String columnInfo,
                                           boolean expectedToExist) throws SQLException {
        if (!isCurrentConnOnLeader()) {
            // Fail points must be set on leader node for DDL. Otherwise, they will not take effect.
            return;
        }
        String sql = String.format(ALTER_TABLE, operation, columnInfo);
        try {
            injectDDLException(failPointKey, true);
            JobInfo job = executeDDL(sql);
            checkPhyProcess(job);
            checkJobGone();
            checkColumnConsistency(columnInfo, expectedToExist);
        } finally {
            injectDDLException(failPointKey, false);
        }
    }

    public void partial_failed_physical_alter(String failPointKey, String operation, String columnInfo,
                                              boolean expectedToExist) throws SQLException {
        if (!isCurrentConnOnLeader()) {
            // Fail points must be set on leader node for DDL. Otherwise, they will not take effect.
            return;
        }
        String sql = String.format(ALTER_TABLE, operation, columnInfo);
        try {
            injectDDLException(failPointKey, true);
            JobInfo job = executeDDL(sql);
            if (expectedToExist) {
                checkJobPaused();
            } else {
                checkJobRollbackPaused();
            }
            injectDDLException(failPointKey, false);
            checkPhyProcess(job);
            if (expectedToExist) {
                continueUntilComplete(job);
            } else {
                continueRollbackUntilComplete(job);
            }
            checkPhyProcess(job);
            checkJobGone();
            checkColumnConsistency(columnInfo, expectedToExist);
        } finally {
            injectDDLException(failPointKey, false);
        }
    }

    @Test
    public void t99_cleanup() {
        timeToDie = true;
    }

}


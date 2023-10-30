package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.SQLException;

@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InterruptCreateDropTableTest extends InterruptDDLByShardTest {

    protected static final int NUM_SHARDS_FULL = 512;
    protected static final int NUM_SHARDS_LESS = 32;

    protected static final String DROP_TABLE = "drop table %s";

    protected static final String CREATE_PREFIX = "Create";
    protected static final String DROP_PREFIX = "Drop";

    static {
        stillAlive = true;
    }

    @Test
    public void t01_normal_create() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        JobInfo job = executeDDL(sql);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(true);
    }

    @Test
    public void t02_normal_drop() throws SQLException {
        String sql = String.format(DROP_TABLE, TEST_TABLE);
        JobInfo job = executeDDL(sql);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    public void t11_cancel_create_then_rollback() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        JobInfo job = executeAsyncDDL(sql, CREATE_PREFIX, NUM_SHARDS_LESS, true);
        cancelDDL(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    public void t12_cancel_drop_then_recover() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        executeDDL(sql);
        sql = String.format(DROP_TABLE, TEST_TABLE);
        JobInfo job = executeAsyncDDL(sql, DROP_PREFIX, NUM_SHARDS_LESS);
        cancelDDLFailed(job, "The DDL job has been paused");
        cancelDDLFailed(job, "original DDL itself cannot be rolled back");
        checkJobPaused();
        checkPhyProcess(job);
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    public void t21_pause_create_then_rollback() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        JobInfo job = executeAsyncDDL(sql, CREATE_PREFIX, NUM_SHARDS_LESS, true);
        pauseDDL(job);
        checkJobPaused();
        checkPhyProcess(job);
        cancelUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    public void t22_pause_create_then_recover() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        JobInfo job = executeAsyncDDL(sql, CREATE_PREFIX, NUM_SHARDS_LESS, true);
        pauseDDL(job);
        checkJobPaused();
        checkPhyProcess(job);
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(true);
    }

    @Test
    public void t23_pause_drop_then_recover() throws SQLException {
        String sql = String.format(DROP_TABLE, TEST_TABLE);
        JobInfo job = executeAsyncDDL(sql, DROP_PREFIX, NUM_SHARDS_LESS);
        pauseDDL(job);
        checkJobPaused();
        checkPhyProcess(job);
        cancelDDLFailed(job, "original DDL itself cannot be rolled back");
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    public void t31_kill_logical_create_rollback() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        JobInfo job = executeSeparateDDL(sql, CREATE_PREFIX, NUM_SHARDS_LESS, true);
        killLogicalProcess(CREATE_PREFIX);
        checkJobPaused();
        checkPhyProcess(job);
        cancelUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    public void t32_kill_logical_create_recover() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        JobInfo job = executeSeparateDDL(sql, CREATE_PREFIX, NUM_SHARDS_LESS, true);
        killLogicalProcess(CREATE_PREFIX);
        checkJobPaused();
        checkPhyProcess(job);
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(true);
    }

    @Test
    public void t33_kill_logical_drop_then_recover() throws SQLException {
        String sql = String.format(DROP_TABLE, TEST_TABLE);
        JobInfo job = executeSeparateDDL(sql, DROP_PREFIX, NUM_SHARDS_LESS);
        killLogicalProcess(DROP_PREFIX);
        checkJobPaused();
        checkPhyProcess(job);
        cancelDDLFailed(job, "original DDL itself cannot be rolled back");
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    @Ignore
    public void t41_kill_physical_create_with_auto_rollback() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        JobInfo job = executeAsyncDDL(sql, CREATE_PREFIX, NUM_SHARDS_LESS);
        killPhysicalProcess();
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    @Ignore
    public void t42_kill_physical_drop_recover() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_FULL);
        executeDDL(sql);
        sql = String.format(DROP_TABLE, TEST_TABLE);
        JobInfo job = executeAsyncDDL(sql, DROP_PREFIX, NUM_SHARDS_LESS);
        killPhysicalProcess();
        checkJobPaused();
        checkPhyProcess(job);
        continueUntilComplete(job);
        checkPhyProcess(job);
        checkJobGone();
        checkTableConsistency(false);
    }

    @Test
    public void t51_all_physical_create_with_timeout() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_LESS);
        try {
            injectDDLTimeout(1);
            JobInfo job = executeDDL(sql);
            checkPhyProcess(job);
            checkJobGone();
            checkTableConsistency(true);
        } finally {
            injectDDLTimeout(0);
        }
    }

    @Test
    public void t52_all_physical_drop_with_timeout() throws SQLException {
        String sql = String.format(DROP_TABLE, TEST_TABLE);
        try {
            injectDDLTimeout(1);
            JobInfo job = executeDDL(sql);
            checkPhyProcess(job);
            checkJobGone();
            checkTableConsistency(false);
        } finally {
            injectDDLTimeout(0);
        }
    }

    @Test
    public void t61_all_failed_before_physical_create() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_LESS);
        all_failed_before_physical_ddl(sql, false);
    }

    @Test
    public void t62_all_failed_before_physical_drop() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_LESS);
        executeDDL(sql);
        sql = String.format(DROP_TABLE, TEST_TABLE);
        all_failed_before_physical_ddl(sql, true);
    }

    @Test
    public void t63_all_failed_after_physical_drop() throws SQLException {
        String sql = String.format(DROP_TABLE, TEST_TABLE);
        all_failed_after_physical_ddl(sql, false);
    }

    @Test
    public void t64_all_failed_after_physical_create() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_LESS);
        all_failed_after_physical_ddl(sql, true);
    }

    private void all_failed_before_physical_ddl(String sql, boolean expectedToExist) throws SQLException {
        all_failed_physical_ddl("FP_BEFORE_PHYSICAL_DDL_EXCEPTION", sql, expectedToExist);
    }

    private void all_failed_after_physical_ddl(String sql, boolean expectedToExist) throws SQLException {
        all_failed_physical_ddl("FP_AFTER_PHYSICAL_DDL_EXCEPTION", sql, expectedToExist);
    }

    @Test
    public void t65_partial_failed_before_physical_drop() throws SQLException {
        String sql = String.format(DROP_TABLE, TEST_TABLE);
        partial_failed_before_physical_ddl(sql, false);
    }

    @Test
    public void t66_partial_failed_before_physical_create() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_LESS);
        partial_failed_before_physical_ddl(sql, true);
    }

    @Test
    public void t67_partial_failed_after_physical_create() throws SQLException {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, NUM_SHARDS_LESS);
        partial_failed_after_physical_ddl(sql, true);
    }

    @Test
    public void t68_partial_failed_after_physical_ddl() throws SQLException {
        String sql = String.format(DROP_TABLE, TEST_TABLE);
        partial_failed_after_physical_ddl(sql, false);
    }

    public void partial_failed_before_physical_ddl(String sql, boolean rollbackable) throws SQLException {
        partial_failed_physical_ddl("FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION", sql, rollbackable);
    }

    public void partial_failed_after_physical_ddl(String sql, boolean expectedToExist) throws SQLException {
        all_failed_physical_ddl("FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION", sql, expectedToExist);
    }

    private void all_failed_physical_ddl(String failPointKey, String sql, boolean expectedToExist) throws SQLException {
        if (!isCurrentConnOnLeader()) {
            // Fail points must be set on leader node for DDL. Otherwise, they will not take effect.
            return;
        }
        try {
            injectDDLException(failPointKey, true);
            JobInfo job = executeDDL(sql);
            checkPhyProcess(job);
            checkJobGone();
            checkTableConsistency(expectedToExist);
        } finally {
            injectDDLException(failPointKey, false);
        }
    }

    public void partial_failed_physical_ddl(String failPointKey, String sql, boolean rollbackable)
        throws SQLException {
        if (!isCurrentConnOnLeader()) {
            // Fail points must be set on leader node for DDL. Otherwise, they will not take effect.
            return;
        }
        try {
            injectDDLException(failPointKey, true);
            JobInfo job = executeDDL(sql);
            if (rollbackable) {
                checkJobGone();
            } else {
                checkJobPaused();
            }
            injectDDLException(failPointKey, false);
            checkPhyProcess(job);
            if (!rollbackable) {
                continueUntilComplete(job);
                checkPhyProcess(job);
                checkJobGone();
            }
            checkTableConsistency(false);
        } finally {
            injectDDLException(failPointKey, false);
        }
    }

    @Test
    public void t99_cleanup() {
        timeToDie = true;
    }

}


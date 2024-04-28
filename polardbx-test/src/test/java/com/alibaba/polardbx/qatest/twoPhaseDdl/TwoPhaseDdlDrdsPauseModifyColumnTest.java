package com.alibaba.polardbx.qatest.twoPhaseDdl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareDataForDrds;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.alterTableViaTwoPhaseDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkIfCompleteFully;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkIfExecuteTwoPhaseDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkIfRollbackFully;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkPhyDdlStatus;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkTableStatus;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.continueDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.getDdlJobIdFromPattern;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.killPhysicalDdlRandomly;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.pauseDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.tryRollbackDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.waitTillCommit;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.waitTillDdlDone;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.waitTillDdlRunning;

@NotThreadSafe
@RunWith(Parameterized.class)
public class TwoPhaseDdlDrdsPauseModifyColumnTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(TwoPhaseDdlCheckApplicabilityTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public TwoPhaseDdlDrdsPauseModifyColumnTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    public int smallDelay = 1;

    public int largeDelay = 5;

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("two_phase", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    // two_phase_ddl_drds_test1.online_ddl_pause, 128 * 50W
    @Test
    public void testAlterTableModifyDdlPauseBeforePrepare() throws SQLException, InterruptedException {
        String schemaName = "two_phase_ddl_drds_test1";
        String mytable = schemaPrefix + "online_ddl_pause";
        // prepare data
        prepareDataForDrds(tddlConnection, schemaName, mytable);
        //
        String enableTwoPhaseDdlHint =
            String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true,PURE_ASYNC_DDL_MODE=true)*/");
        String recoverDdl = String.format("alter table %s modify column c varchar(32)", mytable);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, recoverDdl);
        String ddl = String.format("alter table %s modify column c varchar(16)", mytable);
        String msg = String.format("table: %s, ddl: %s", mytable, ddl);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, enableTwoPhaseDdlHint + ddl);
        Long jobId = getDdlJobIdFromPattern(tddlConnection, ddl);
        int sleepTime = 1;
        Thread.sleep(sleepTime * 500);
        int i = 0;
        while (i < 1) {
            pauseDdl(tddlConnection, jobId);
            if (!checkPhyDdlStatus(schemaName, tddlConnection, jobId, sleepTime * 2, mytable)) {
                continueDdl(tddlConnection, jobId);
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("pause ddl failed! there are still physical ddl emitting for %s", msg));
            }
            Thread thread = new Thread(() -> {
                try (Connection connection = getPolardbxConnection(schemaName)) {
                    continueDdl(connection, jobId);
                } catch (SQLException ignored) {
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            Thread.sleep(sleepTime * 500);
            i++;
        }
        if (!checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
        if (!waitTillDdlDone(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl done timeout for %s", msg));
        }
        if (!checkIfExecuteTwoPhaseDdl(tddlConnection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job is not two phase ddl for %s", msg));
        }
        if (!checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
    }

    // two_phase_ddl_drds_test1.online_ddl_pause, 128 * 50W
    @Test
    public void testAlterTableModifyDdlPauseThenRandomKillBeforePrepareToRollback()
        throws SQLException, InterruptedException {
        String schemaName = "two_phase_ddl_drds_test1";
        String mytable = schemaPrefix + "online_ddl_pause";
        // prepare data
        prepareDataForDrds(tddlConnection, schemaName, mytable);
        //
        String enableTwoPhaseDdlHint =
            String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true,PURE_ASYNC_DDL_MODE=true)*/");
        String recoverDdl = String.format("alter table %s modify column c varchar(32)", mytable);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, recoverDdl);
        String ddl = String.format("alter table %s modify column c varchar(16)", mytable);
        String msg = String.format("table: %s, ddl: %s", mytable, ddl);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, enableTwoPhaseDdlHint + ddl);
        Long jobId = getDdlJobIdFromPattern(tddlConnection, ddl);
        int sleepTime = 1;
        Thread.sleep(sleepTime * 500);
        int i = 0;
        while (i < 1) {
            pauseDdl(tddlConnection, jobId);
            if (!checkPhyDdlStatus(schemaName, tddlConnection, jobId, sleepTime * 1, mytable)) {
                continueDdl(tddlConnection, jobId);
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("pause ddl failed! there are still physical ddl emitting for %s", msg));
            }
            Thread thread = new Thread(() -> {
                try (Connection connection = getPolardbxConnection(schemaName)) {
                    continueDdl(connection, jobId);
                } catch (SQLException ignored) {
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            Thread.sleep(sleepTime * 500);
            i++;
        }
        if (!waitTillDdlRunning(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl running failed for %s, PLEASE RERUN THIS CASE", msg));
        }
        if (!killPhysicalDdlRandomly(tddlConnection, jobId, mytable, "RUNNING")) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("kill physical ddl failed for %s, PLEASE RERUN THIS CASE", msg));
        }
        if (!checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
        if (!waitTillDdlDone(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl done timeout for %s", msg));
        }
        if (!checkIfRollbackFully(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("rollback not fully for %s", msg));

        }
        if (!checkIfExecuteTwoPhaseDdl(tddlConnection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job is not two phase ddl for %s", msg));
        }
        if (!checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
    }

    // two_phase_ddl_drds_test1.online_ddl_pause, 128 * 50W
    @Test
    public void testAlterTableModifyDdlPauseThenRollbackBeforePrepare() throws SQLException, InterruptedException {
        String schemaName = "two_phase_ddl_drds_test1";
        String mytable = schemaPrefix + "online_ddl_pause";
        // prepare data
        prepareDataForDrds(tddlConnection, schemaName, mytable);
        //
        String enableTwoPhaseDdlHint =
            String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true,PURE_ASYNC_DDL_MODE=true)*/");
        String recoverDdl = String.format("alter table %s modify column c varchar(32)", mytable);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, recoverDdl);
        String ddl = String.format("alter table %s modify column c varchar(16)", mytable);
        String msg = String.format("table: %s, ddl: %s", mytable, ddl);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, enableTwoPhaseDdlHint + ddl);
        Long jobId = getDdlJobIdFromPattern(tddlConnection, ddl);
        int sleepTime = 1;
        Thread.sleep(sleepTime * 500);
        int i = 0;
        while (i < 1) {
            pauseDdl(tddlConnection, jobId);
            if (!checkPhyDdlStatus(schemaName, tddlConnection, jobId, sleepTime * 2, mytable)) {
                continueDdl(tddlConnection, jobId);
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("pause ddl failed! there are still physical ddl emitting for %s", msg));
            }
            Thread thread = new Thread(() -> {
                try (Connection connection = getPolardbxConnection(schemaName)) {
                    continueDdl(connection, jobId);
                } catch (SQLException ignored) {
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            Thread.sleep(sleepTime * 500);
            i++;
        }
        tryRollbackDdl(tddlConnection, jobId);
        if (!checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
        if (!waitTillDdlDone(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl done timeout for %s", msg));
        }
        if (!checkIfRollbackFully(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("rollback not fully for %s", msg));

        }
        if (!checkIfExecuteTwoPhaseDdl(tddlConnection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job is not two phase ddl for %s", msg));
        }
        if (!checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
    }

    // two_phase_ddl_drds_test1.online_ddl_pause, 128 * 50W
    @Test
    public void testAlterTableModifyDdlPauseThenRandomKillBeforeCommitToContinueAndCompensation()
        throws SQLException, InterruptedException {
        String schemaName = "two_phase_ddl_drds_test1";
        String mytable = schemaPrefix + "online_ddl_pause";
        // prepare data
        prepareDataForDrds(tddlConnection, schemaName, mytable);
        //
        String enableTwoPhaseDdlHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true,PURE_ASYNC_DDL_MODE=true,MULTI_PHASE_COMMIT_DELAY=20)*/");
        String recoverDdl = String.format("alter table %s modify column c varchar(32)", mytable);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, recoverDdl);
        String ddl = String.format("alter table %s modify column c varchar(16)", mytable);
        String msg = String.format("table: %s, ddl: %s", mytable, ddl);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, enableTwoPhaseDdlHint + ddl);
        Long jobId = getDdlJobIdFromPattern(tddlConnection, ddl);
        int sleepTime = 1;
        Thread.sleep(sleepTime * 500);
        int i = 0;
        while (i < 1) {
            pauseDdl(tddlConnection, jobId);
            if (!checkPhyDdlStatus(schemaName, tddlConnection, jobId, sleepTime * 2, mytable)) {
                continueDdl(tddlConnection, jobId);
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("pause ddl failed! there are still physical ddl emitting for %s", msg));
            }
            Thread thread = new Thread(() -> {
                try (Connection connection = getPolardbxConnection(schemaName)) {
                    continueDdl(connection, jobId);
                } catch (SQLException ignored) {
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            Thread.sleep(sleepTime * 500);
            i++;
        }

        String logInfo = "finish pause and continue test, wait for commit...";
        logger.info(logInfo);
        if (!waitTillCommit(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl done timeout for %s", msg));
        }
        logInfo = "wait for committed finish, kill physical ddl randomly...";
        logger.info(logInfo);
        if (!killPhysicalDdlRandomly(tddlConnection, jobId, mytable, "REACHED_BARRIER")) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("kill physical ddl failed for %s, PLEASE RERUN THIS CASE", msg));
        }
        logInfo = "kill physical ddl randomly finish, check table status...";
        logger.info(logInfo);
//        if (checkTableStatus(tddlConnection, jobId, mytable)) {
//            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                String.format("check table success for %s, after kill connection finished", msg));
//        }
//        logInfo = "check table status finish, wait ddl done...";
        logger.info(logInfo);
        if (!waitTillDdlDone(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl done timeout for %s", msg));
        }
        logInfo = "wait ddl done finish, check if complete...";
        logger.info(logInfo);
        if (!checkIfCompleteFully(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("rollback not fully for %s", msg));
        }
        if (!checkIfExecuteTwoPhaseDdl(tddlConnection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job is not two phase ddl for %s", msg));
        }
        if (!checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
    }

}

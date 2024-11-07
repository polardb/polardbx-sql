package com.alibaba.polardbx.qatest.twoPhaseDdl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataCheckUtil.checkData;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.checkOnlineLogApply;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.checkTableDataForNonRebuildTable;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.checkTableDataForRebuildTable;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.recordOnlineLogApply;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.sampleRows;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.waitTillFlagSet;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkIfExecuteTwoPhaseDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.updateTable;

@NotThreadSafe
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TwoPhaseDdlOnlineLogApplyTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(TwoPhaseDdlCheckApplicabilityTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public TwoPhaseDdlOnlineLogApplyTest(boolean crossSchema) {
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

    // two_phase_ddl_test1.online_log_apply, 1 * 640W
    @Test
    public void test01AlterTableAddColumn() throws Exception {
        String schemaName = "two_phase_ddl_test1";
        String mytable = schemaPrefix + "online_log_apply";
        /* prepare data */
        prepareData(tddlConnection, schemaName, mytable, 64_000_00, DataManipulateUtil.TABLE_TYPE.SINGLE_TABLE);
        List<Integer> beforeCheckSum = checkData(tddlConnection, schemaName, mytable);
        //
        String enableTwoPhaseDdlHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true,PURE_ASYNC_DDL_MODE=true,CHECK_PHY_CONN_NUM=false,MULTI_PHASE_PREPARE_DELAY=40)*/");
        String columName = randomTableName("column", 2);
        String ddl = String.format("alter table %s add column %s int, ALGORITHM=INPLACE", mytable, columName);
        String msg = String.format("table: %s, ddl: %s", mytable, ddl);
        DdlStateCheckUtil.alterTableViaJdbc(tddlConnection, schemaName, mytable, enableTwoPhaseDdlHint + ddl);
        log.info("alter table stmt emmitted: " + enableTwoPhaseDdlHint + ddl);
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, ddl);
        int sleepTime = 4;
        Thread.sleep(sleepTime * 1000);
        int i = 0;
        AtomicInteger finishDdlFlag = new AtomicInteger(0);
        AtomicReference<List<Integer>> checkSum = new AtomicReference<>(beforeCheckSum);
        log.info("start to sample rows from table");
        Map<Integer, Integer> beforeRows = sampleRows(tddlConnection, schemaName, mytable);
        log.info("finish sample rows from table");
        Thread updateTableThread = new Thread(
            () -> {
                try (Connection connection = getPolardbxConnection(schemaName)) {
                    updateTable(connection, schemaName, mytable, beforeRows, finishDdlFlag, checkSum);
                } catch (SQLException | InterruptedException ignored) {
                }
            });
        AtomicReference<List<Date>> applyRowLogFlag = new AtomicReference<>(new ArrayList<Date>());
        Thread recordOnlineLogApply = new Thread(
            () -> {
                try (Connection connection = getPolardbxConnection(schemaName)) {
                    recordOnlineLogApply(connection, schemaName, mytable, finishDdlFlag, applyRowLogFlag);
                } catch (SQLException ignored) {
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        updateTableThread.start();
        recordOnlineLogApply.start();
        if (!DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl done timeout for %s", msg));
        }
        finishDdlFlag.set(1);
        if (!waitTillFlagSet(finishDdlFlag, 2)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait flag set timeout for %s", msg));
        }
        // TODO check online log apply
        if (!checkTableDataForRebuildTable(tddlConnection, jobId, schemaName, mytable, checkSum.get())) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check sum failed for %s", msg));
        }
        if (!checkOnlineLogApply(tddlConnection, jobId, mytable, applyRowLogFlag)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check online log apply failed for %s", msg));
        }
        if (!DdlStateCheckUtil.checkIfExecuteTwoPhaseDdl(tddlConnection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job is not two phase ddl for %s", msg));
        }
        if (!DdlStateCheckUtil.checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
    }

    // two_phase_ddl_test1.online_log_apply, 1 * 640W
    @Test
    public void test11AlterTableAddIndex() throws Exception {
        String schemaName = "two_phase_ddl_test1";
        String mytable = schemaPrefix + "online_log_apply";
        // prepare data
        prepareData(tddlConnection, schemaName, mytable, 64_000_00, DataManipulateUtil.TABLE_TYPE.SINGLE_TABLE);
        List<Integer> beforeCheckSum = checkData(tddlConnection, schemaName, mytable);
        //
        String enableTwoPhaseDdlHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true,PURE_ASYNC_DDL_MODE=true,CHECK_PHY_CONN_NUM=false,MULTI_PHASE_PREPARE_DELAY=40)*/");
        String indexName = randomTableName("index", 4);
        String ddl = String.format("alter table %s add local index %s(a, b)", mytable, indexName);
        String msg = String.format("table: %s, ddl: %s", mytable, ddl);
        DdlStateCheckUtil.alterTableViaJdbc(tddlConnection, schemaName, mytable, enableTwoPhaseDdlHint + ddl);
        log.info("alter table stmt emmitted: " + enableTwoPhaseDdlHint + ddl);
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, ddl);
        int sleepTime = 4;
        Thread.sleep(sleepTime * 1000);
        int i = 0;
        AtomicInteger finishDdlFlag = new AtomicInteger(0);
        AtomicReference<List<Integer>> checkSum = new AtomicReference<>(beforeCheckSum);
        log.info("start to sample rows from table");
        Map<Integer, Integer> beforeRows = sampleRows(tddlConnection, schemaName, mytable);
        log.info("finish sample rows from table");
        Thread updateTableThread = new Thread(
            () -> {
                try (Connection connection = getPolardbxConnection(schemaName)) {
                    updateTable(connection, schemaName, mytable, beforeRows, finishDdlFlag, checkSum);
                } catch (SQLException | InterruptedException ignored) {
                }
            });
        AtomicReference<List<Date>> applyRowLogFlag = new AtomicReference<>(new ArrayList<Date>());
        updateTableThread.start();

        if (!DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl done timeout for %s", msg));
        }
        finishDdlFlag.set(1);
        if (!waitTillFlagSet(finishDdlFlag, 2)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait flag set timeout for %s", msg));
        }
        // TODO check online log apply
        if (!checkTableDataForNonRebuildTable(tddlConnection, jobId, schemaName, mytable, indexName, checkSum.get())) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check sum failed for %s", msg));
        }
        if (!DdlStateCheckUtil.checkIfExecuteTwoPhaseDdl(tddlConnection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job is not two phase ddl for %s", msg));
        }
        if (!DdlStateCheckUtil.checkTableStatus(tddlConnection, jobId, mytable)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
    }

//    @Test
//    public void test21AlterTableAddColumnWithLocalIndex() throws SQLException, InterruptedException {
//        String schemaName = "two_phase_ddl_test1";
//        String mytable = schemaPrefix + "online_log_apply";
//        // prepare data
//        prepareData(tddlConnection, schemaName, mytable, 64_000_00, DataManipulateUtil.TABLE_TYPE.SINGLE_TABLE);
//        List<Integer> beforeCheckSum = checkData(tddlConnection, schemaName, mytable);
//        //
//        String enableTwoPhaseDdlHint =
//            String.format(
//                "/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true,PURE_ASYNC_DDL_MODE=true,CHECK_PHY_CONN_NUM=false,MULTI_PHASE_PREPARE_DELAY=40)*/");
//        String columName = randomTableName("column", 2);
//        String ddl = String.format("alter table %s add column %s int, ALGORITHM=INPLACE", mytable, columName);
//        String msg = String.format("table: %s, ddl: %s", mytable, ddl);
//        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, mytable, enableTwoPhaseDdlHint + ddl);
//        log.info("alter table stmt emmitted: " + enableTwoPhaseDdlHint + ddl);
//        Long jobId = getDdlJobIdFromPattern(tddlConnection, ddl);
//        int sleepTime = 4;
//        Thread.sleep(sleepTime * 1000);
//        int i = 0;
//        AtomicInteger finishDdlFlag = new AtomicInteger(0);
//        AtomicReference<List<Integer>> checkSum = new AtomicReference<>(beforeCheckSum);
//        log.info("start to sample rows from table");
//        Map<Integer, Integer> beforeRows = sampleRows(tddlConnection, schemaName, mytable);
//        log.info("finish sample rows from table");
//        Thread updateTableThread = new Thread(
//            () -> {
//                try (Connection connection = getPolardbxConnection(schemaName)) {
//                    updateTable(connection, schemaName, mytable, beforeRows, finishDdlFlag, checkSum);
//                } catch (SQLException | InterruptedException ignored) {
//                }
//            });
//        AtomicReference<List<Date>> applyRowLogFlag = new AtomicReference<>(new ArrayList<Date>());
//        Thread recordOnlineLogApply = new Thread(
//            () -> {
//                try (Connection connection = getPolardbxConnection(schemaName)) {
//                    recordOnlineLogApply(connection, schemaName, mytable, finishDdlFlag, applyRowLogFlag);
//                } catch (SQLException ignored) {
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            });
//        updateTableThread.start();
//        recordOnlineLogApply.start();
//        if (!waitTillDdlDone(tddlConnection, jobId, mytable)) {
//            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                String.format("wait ddl done timeout for %s", msg));
//        }
//        finishDdlFlag.set(1);
//        if (!waitTillFlagSet(finishDdlFlag, 2)) {
//            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                String.format("wait flag set timeout for %s", msg));
//        }
//        // TODO check online log apply
//        if (!checkTableDataForRebuildTable(tddlConnection, jobId, schemaName, mytable, checkSum.get())) {
//            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                String.format("check sum failed for %s", msg));
//        }
//        if (!checkOnlineLogApply(tddlConnection, jobId, mytable, applyRowLogFlag)) {
//            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                String.format("check online log apply failed for %s", msg));
//        }
//        if (!checkIfExecuteTwoPhaseDdl(tddlConnection, jobId)) {
//            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                String.format("this job is not two phase ddl for %s", msg));
//        }
//        if (!checkTableStatus(tddlConnection, jobId, mytable)) {
//            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                String.format("check table failed for %s, after continue ddl finished", msg));
//        }
//    }
}

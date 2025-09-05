package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;
import com.alibaba.polardbx.optimizer.utils.InventoryMode;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import com.alibaba.polardbx.stats.TransactionStatistics;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class OSSTableScanTest extends BasePlannerTest {

    private static final String schemaName = "test_oss_columnar_flashback";
    private static final long MOCK_TSO = 7147436271206400000L;

    public OSSTableScanTest() {
        super(schemaName);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Before
    public void prepareSchemaAndOssTable() throws SQLSyntaxErrorException {
        prepareSchemaByDdl();
        String createOssTableSql = "create table t1(id int, name varchar(10)) partition by hash(id) engine=oss";
        buildTable(appName, createOssTableSql);
    }

    @Test
    public void getFlashbackQueryTso() {
        runFlashbackQueryAndCompareTso("select * from t1 as of tso 100 + tso_timestamp()", 100 + MOCK_TSO);
    }

    @Test
    public void getFlashbackQueryTimestamp() {
        runFlashbackQueryAndCompareTso("select * from t1 as of timestamp '2024-01-01 12:00:00'", MOCK_TSO);
    }

    @Test
    public void getFlashbackQueryTsoForAbnormalValue() {
        runFlashbackQueryAndCompareTso("select * from t1 as of tso 0", 0);
        boolean exceptionCaught = false;
        try {
            runFlashbackQueryAndCompareTso("select * from t1 as of tso 'invalid_expression'", 0);
        } catch (Throwable t) {
            exceptionCaught = true;
            assertTrue(t.getMessage().equalsIgnoreCase("Illegal tso expression: ?0"));
        }
        assertTrue(exceptionCaught);
    }

    private void runFlashbackQueryAndCompareTso(String sql, long expectedTso) {
        ITransaction trx = new MockFlashbackTransaction();
        ExecutionContext executionContext = new ExecutionContext(appName);
        executionContext.setTransaction(trx);

        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(
            ByteString.from(sql), currentParameter, executionContext, false);
        Map<Integer, ParameterContext> param = OptimizerUtils.buildParam(sqlParameterized.getParameters());
        executionContext.setParams(new Parameters(param, false));

        SqlNode ast = new FastsqlParser().parse(
            sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext).get(0);
        ExecutionPlan plan = Planner.getInstance().getPlan(ast, PlannerContext.fromExecutionContext(executionContext));
        Assert.assertTrue(plan.getPlan() instanceof Gather);
        Gather gather = (Gather) plan.getPlan();
        Assert.assertTrue(gather.getInput(0) instanceof OSSTableScan);
        OSSTableScan ossTableScan = (OSSTableScan) gather.getInput(0);
        RexNode flashback = ossTableScan.getFlashback();
        assertNotNull(flashback);
        Long tso = ossTableScan.getFlashbackQueryTso(executionContext);
        assertEquals(tso.longValue(), expectedTso);
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }

    private static class MockFlashbackTransaction implements ITransaction {

        @Override
        public long getId() {
            return 0;
        }

        @Override
        public void commit() {

        }

        @Override
        public void rollback() {

        }

        @Override
        public ExecutionContext getExecutionContext() {
            return null;
        }

        @Override
        public void setExecutionContext(ExecutionContext executionContext) {

        }

        @Override
        public IConnectionHolder getConnectionHolder() {
            return null;
        }

        @Override
        public void tryClose(IConnection conn, String groupName) throws SQLException {

        }

        @Override
        public void tryClose() throws SQLException {

        }

        @Override
        public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw) throws SQLException {
            return null;
        }

        @Override
        public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw, ExecutionContext ec)
            throws SQLException {
            return null;
        }

        @Override
        public IConnection getConnection(String schemaName, String group, Long grpConnId, IDataSource ds, RW rw,
                                         ExecutionContext ec) throws SQLException {
            return null;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public void updateStatisticsWhenStatementFinished(AtomicLong rowCount) {

        }

        @Override
        public void setMdlWaitTime(long mdlWaitTime) {

        }

        @Override
        public void setStartTimeInMs(long startTime) {

        }

        @Override
        public void setStartTime(long startTime) {

        }

        @Override
        public void setSqlStartTime(long sqlStartTime) {

        }

        @Override
        public void setSqlFinishTime(long t) {

        }

        @Override
        public void kill() throws SQLException {

        }

        @Override
        public void savepoint(String savepoint) {

        }

        @Override
        public void rollbackTo(String savepoint) {

        }

        @Override
        public void release(String savepoint) {

        }

        @Override
        public void clearTrxContext() {

        }

        @Override
        public void setCrucialError(ErrorCode errorCode, String cause) {

        }

        @Override
        public ErrorCode getCrucialError() {
            return null;
        }

        @Override
        public void checkCanContinue() {

        }

        @Override
        public boolean isDistributed() {
            return false;
        }

        @Override
        public boolean isStrongConsistent() {
            return false;
        }

        @Override
        public State getState() {
            return null;
        }

        @Override
        public ITransactionPolicy.TransactionClass getTransactionClass() {
            return ITransactionPolicy.TransactionClass.TSO;
        }

        @Override
        public long getStartTimeInMs() {
            return 0;
        }

        @Override
        public boolean isBegun() {
            return false;
        }

        @Override
        public void setInventoryMode(InventoryMode inventoryMode) {

        }

        @Override
        public ITransactionManagerUtil getTransactionManagerUtil() {
            return new MockTransactionManagerUtil();
        }

        @Override
        public boolean handleStatementError(Throwable t) {
            return false;
        }

        @Override
        public void releaseAutoSavepoint() {

        }

        @Override
        public boolean isUnderCommitting() {
            return false;
        }

        @Override
        public boolean isAsyncCommit() {
            return false;
        }

        @Override
        public TransactionStatistics getStat() {
            return null;
        }

        @Override
        public TransactionType getType() {
            return null;
        }

        @Override
        public void setLastActiveTime() {

        }

        @Override
        public long getLastActiveTime() {
            return 0;
        }

        @Override
        public void resetLastActiveTime() {

        }

        @Override
        public long getIdleTimeout() {
            return 0;
        }

        @Override
        public long getIdleROTimeout() {
            return 0;
        }

        @Override
        public long getIdleRWTimeout() {
            return 0;
        }

        @Override
        public void releaseDirtyReadConnections() {
        }
    }

    private static class MockTransactionManagerUtil implements ITransactionManagerUtil {

        @Override
        public ITimestampOracle getTimestampOracle() {
            return new ITimestampOracle() {
                @Override
                public void setTimeout(long timeout) {

                }

                @Override
                public long nextTimestamp() {
                    return MOCK_TSO;
                }

                @Override
                public void init() {

                }

                @Override
                public void destroy() {

                }

                @Override
                public boolean isInited() {
                    return false;
                }
            };
        }
    }
}
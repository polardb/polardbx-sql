package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.logical.ITPrepareStatement;
import com.alibaba.polardbx.common.logical.ITStatement;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;
import com.alibaba.polardbx.optimizer.utils.InventoryMode;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.stats.TransactionStatistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class LogicalShowVariablesMyHandlerTest {
    private MockedStatic<MetaDbInstConfigManager> mockMetaDbInstConfigManager;

    @After
    public void cleanup() {
        if (mockMetaDbInstConfigManager != null) {
            mockMetaDbInstConfigManager.close();
        }
    }

    @Test
    public void testShowTransactionVariables() {
        MyRepository repository = new MyRepository();
        LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
        TreeMap<String, Object> variables = new TreeMap<>();

        ExecutionContext executionContext = new ExecutionContext();
        ITConnection mockTConnection = new MockTConnection();
        executionContext.setConnection(mockTConnection);

        MetaDbInstConfigManager manager = new MockMetaDbInstConfigManager();
        mockMetaDbInstConfigManager = Mockito.mockStatic(MetaDbInstConfigManager.class);
        mockMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance).thenAnswer(i -> manager);
        ITransaction trx = new MockTransaction();
        executionContext.setTransaction(trx);

        // Default value.
        handler.collectCnVariables(variables, executionContext);
        for (Map.Entry<String, Object> kv : variables.entrySet()) {
            if ("enable_auto_commit_tso".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(true, kv.getValue());
            }
            if ("enable_auto_savepoint".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(true, kv.getValue());
            }
            if ("enable_transaction_recover_task".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(true, kv.getValue());
            }
            if ("enable_x_proto_opt_for_auto_sp".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(false, kv.getValue());
            }
            if ("enable_xa_tso".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(true, kv.getValue());
            }
            if ("trx_log_method".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(0, kv.getValue());
            }
            if ("trx_class".equalsIgnoreCase(kv.getKey())) {
                Assert.assertTrue("MockTransaction".equalsIgnoreCase((String) kv.getValue()));
            }
        }
    }

    private static class MockTransaction implements ITransaction {

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
            return null;
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
            return null;
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
    }

    private static class MockMetaDbInstConfigManager extends MetaDbInstConfigManager {
        @Override
        protected void doInit() {
        }

        public void setProperties(Properties properties) {
            this.propertiesInfoMap = properties;
        }
    }

    private static class MockTConnection implements ITConnection {
        @Override
        public ITStatement createStatement() throws SQLException {
            return null;
        }

        @Override
        public ITPrepareStatement prepareStatement(String sql) throws SQLException {
            return null;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public long getLastInsertId() {
            return 0;
        }

        @Override
        public void setLastInsertId(long id) {

        }

        @Override
        public long getReturnedLastInsertId() {
            return 0;
        }

        @Override
        public void setReturnedLastInsertId(long id) {

        }

        @Override
        public String getUser() {
            return null;
        }

        @Override
        public ITransactionPolicy getTrxPolicy() {
            return ITransactionPolicy.TSO;
        }

        @Override
        public void setTrxPolicy(ITransactionPolicy trxPolicy, boolean check) {

        }

        @Override
        public BatchInsertPolicy getBatchInsertPolicy(Map<String, Object> extraCmds) {
            return BatchInsertPolicy.NONE;
        }

        @Override
        public void setBatchInsertPolicy(BatchInsertPolicy policy) {

        }

        @Override
        public long getFoundRows() {
            return 0;
        }

        @Override
        public void setFoundRows(long foundRows) {

        }

        @Override
        public long getAffectedRows() {
            return 0;
        }

        @Override
        public void setAffectedRows(long affectedRows) {

        }

        @Override
        public List<Long> getGeneratedKeys() {
            return null;
        }

        @Override
        public void setGeneratedKeys(List<Long> ids) {

        }

        @Override
        public boolean isMppConnection() {
            return false;
        }
    }
}

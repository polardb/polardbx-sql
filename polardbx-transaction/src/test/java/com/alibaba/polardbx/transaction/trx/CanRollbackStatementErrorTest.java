package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CanRollbackStatementErrorTest {
    @Test
    public void test() {
        TransactionManager transactionManager = mock(TransactionManager.class);
        GlobalTxLogManager txLogManager = mock(GlobalTxLogManager.class);
        when(transactionManager.getGlobalTxLogManager()).thenReturn(txLogManager);
        when(transactionManager.generateTxid(any())).thenReturn(100L);
        doNothing().when(transactionManager).register(any());

        ExecutionContext ec = new ExecutionContext();
        ec.setSqlType(SqlType.DELETE);
        MockTransaction tx = new MockTransaction(ec, transactionManager);
        tx.setCrucialError(ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL, "");

        SQLException e;
        e = new SQLException(
            "Error occurs when execute on GROUP 'DRDS_POLARX1_QATEST_APP_000002_GROUP' ATOM 'dskey_drds_polarx1_qatest_app_000002_group#polardbx-storage-0-master#11.167.60.147-3777#drds_polarx1_qatest_app_000002': Data too long for column 'c' at row 1 ");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "Error occurs when execute on GROUP 'DRDS_POLARX1_QATEST_APP_000000_GROUP' ATOM 'dskey_drds_polarx1_qatest_app_000000_group#polardbx-storage-0-master#11.167.60.147-3777#drds_polarx1_qatest_app_000000': Duplicate entry '1' for key 'PRIMARY'");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "Error occurs when execute on GROUP 'DRDS_POLARX1_QATEST_APP_000000_GROUP' ATOM 'dskey_drds_polarx1_qatest_app_000000_group#polardbx-storage-0-master#11.167.60.147-3777#drds_polarx1_qatest_app_000000': Duplicate entry '1' for key 'PRIMARY'");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "Error occurs when execute on GROUP 'DRDS_POLARX1_QATEST_APP_000000_GROUP' ATOM 'dskey_drds_polarx1_qatest_app_000000_group#polardbx-storage-0-master#11.167.60.147-3777#drds_polarx1_qatest_app_000000': Duplicate entry '1' for key UGSI 'auto_savepoint_test_gsi_tablexa'");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "Error occurs when execute on GROUP 'DRDS_POLARX1_QATEST_APP_000002_GROUP' ATOM 'dskey_drds_polarx1_qatest_app_000002_group#polardbx-storage-0-master#11.167.60.147-3777#drds_polarx1_qatest_app_000002': Out of range value for column 'd' at row 1 ");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "[TDDL-4602][ERR_CONVERTOR] convertor error: java.lang.Long value '10000000000000' is too large for java.lang.Integer ");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "Error occurs when execute on GROUP 'DRDS_POLARX1_QATEST_APP_000000_GROUP' ATOM 'dskey_drds_polarx1_qatest_app_000000_group#polardbx-storage-0-master#11.167.60.147-3777#drds_polarx1_qatest_app_000000': Field 'd' doesn't have a default value ");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "Error occurs when execute on GROUP 'DRDS_POLARX1_QATEST_APP_000001_GROUP' ATOM 'dskey_drds_polarx1_qatest_app_000001_group#polardbx-storage-1-master#11.167.60.147-3777#drds_polarx1_qatest_app_000001': Incorrect datetime value: 'bad' for column 'time1' at row 1 ");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "Error occurs when execute on GROUP 'DRDS_POLARX1_QATEST_APP_000000_GROUP' ATOM 'dskey_drds_polarx1_qatest_app_000000_group#polardbx-storage-0-master#11.167.60.147-3777#drds_polarx1_qatest_app_000000': Incorrect time value: 'bad' for column 'time2' at row 1 ");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException(
            "Error occurs when execute on GROUP 'DB2_P00000_GROUP' ATOM 'dskey_db2_p00000_group#polardbx-storage-0-master#11.167.60.147-3777#db2_p00000': Column 'name' cannot be null");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException("Cannot delete or update a parent row: a foreign key constraint fails");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        e = new SQLException("Option SET_DEFAULT");
        Assert.assertTrue(tx.shouldRollbackStatement(e));

        // Connection error cannot be rolled back.
        e = new SQLException("No operations allowed after connection closed");
        Assert.assertFalse(tx.shouldRollbackStatement(e));

        e = new SQLException("Connection killed");
        Assert.assertFalse(tx.shouldRollbackStatement(e));

        e = new SQLException("Communications link failure");
        Assert.assertFalse(tx.shouldRollbackStatement(e));
    }

    private static class MockTransaction extends AbstractTransaction {
        public MockTransaction(ExecutionContext executionContext, TransactionManager manager) {
            super(executionContext, manager);
        }

        @Override
        public void begin(String schema, String group, IConnection conn) throws SQLException {
        }

        @Override
        protected void cleanup(String group, IConnection conn) throws SQLException {

        }

        @Override
        protected void innerCleanupAllConnections(String group, IConnection conn,
                                                  TransactionConnectionHolder.ParticipatedState participatedState) {

        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public ITransactionPolicy.TransactionClass getTransactionClass() {
            return null;
        }

        @Override
        public TransactionType getType() {
            return null;
        }
    }
}

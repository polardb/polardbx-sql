package com.alibaba.polardbx.transaction.connection;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.transaction.SavepointAction;

import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 不允许做任何跨库事务
 *
 * @author mengshi.sunmengshi 2014年5月21日 下午4:13:47
 * @since 5.1.0
 */
public class StrictConnectionHolder extends BaseConnectionHolder {

    private final static Logger logger = LoggerFactory.getLogger(StrictConnectionHolder.class);

    private IConnection trxConn = null;
    private boolean inUse = false;
    private Queue<SavepointAction> savepointActions = new LinkedBlockingQueue<SavepointAction>();

    public StrictConnectionHolder() {
    }

    public void addSavepointAction(SavepointAction action) {
        savepointActions.add(action);
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds) throws SQLException {
        // checkClosed();
        if (trxConn == null) {

            trxConn = ds.getConnection();
            trxConn.setAutoCommit(false);

            while (!savepointActions.isEmpty()) {
                savepointActions.poll().doAction(trxConn);
            }

            connections.add(trxConn);

        }

        if (inUse) {
            // Assume that inUse is always false before every logical SQL.
            // Now that the logical SQL will fail anyway, restore the state set by previous physical SQL.
            inUse = false;
            throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, groupName, "connection in use");
        }

        inUse = true;
        if (logger.isDebugEnabled()) {
            logger.debug("getConnection:" + trxConn);
        }

        heldSchema.add(schemaName);
        return trxConn;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        if (this.trxConn != null && conn != this.trxConn) {
            throw new IllegalAccessError("impossible");
        }

        inUse = false;
        if (logger.isDebugEnabled()) {
            logger.debug("tryClose:" + trxConn);
        }
    }

    /**
     * 无条件关闭所有连接
     */
    @Override
    public void closeAllConnections() {
        super.closeAllConnections();
        this.trxConn = null;
    }

}

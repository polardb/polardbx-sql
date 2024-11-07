package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.utils.XAUtils;

/**
 * This transaction is used for archive operations, its xa format id is 3.
 *
 * @author yaozhili
 */
public class IgnoreBinlogTransaction extends TsoTransaction {
    private final static long IGNORE_BINLOG_FORMAT_ID = 4;
    private final static String TRX_LOG_PREFIX =
        "[" + ITransactionPolicy.TransactionClass.IGNORE_BINLOG_TRANSACTION + "]";

    public IgnoreBinlogTransaction(ExecutionContext executionContext,
                                   TransactionManager manager) {
        super(executionContext, manager);
    }

    @Override
    protected String getXid(String group, IConnection conn) {
        if (conn.getTrxXid() != null) {
            return conn.getTrxXid();
        }
        conn.setInShareReadView(shareReadView);
        String xid;
        if (shareReadView) {
            xid = XAUtils.toXidStringWithFormatId(id, group, primaryGroupUid, getReadViewSeq(group),
                IGNORE_BINLOG_FORMAT_ID);
        } else {
            xid = XAUtils.toXidStringWithFormatId(id, group, primaryGroupUid, IGNORE_BINLOG_FORMAT_ID);
        }
        conn.setTrxXid(xid);
        return xid;
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.IGNORE_BINLOG_TRANSACTION;
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }
}

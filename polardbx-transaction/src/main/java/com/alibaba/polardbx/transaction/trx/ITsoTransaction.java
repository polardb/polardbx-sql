package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.utils.IMppTsoTransaction;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author zhuangtianyi
 */
public interface ITsoTransaction extends IMppTsoTransaction {
    long getSnapshotSeq();

    boolean snapshotSeqIsEmpty();

    TransactionManager getManager();

    @Override
    default long nextTimestamp(Consumer<Long> updateGetTsoTime) {
        long getTsoStartTime = System.nanoTime();
        long tso = getManager().getTimestampOracle().nextTimestamp();
        updateGetTsoTime.accept(System.nanoTime() - getTsoStartTime);
        return tso;
    }

    default void updateSnapshotTimestamp() {
        // do nothing
    }

    default void useCtsTransaction(IConnection conn, boolean lizard1PC) throws SQLException {
        XConnection xConnection;
        if (conn.isWrapperFor(XConnection.class) &&
            (xConnection = conn.unwrap(XConnection.class)).supportSingleShardOptimization()) {
            conn.flushUnsent();
            xConnection.setLazyCtsTransaction();
        } else {
            if (lizard1PC) {
                conn.executeLater("SET innodb_current_snapshot_seq = ON");
            } else {
                conn.executeLater("SET innodb_cts_transaction = ON");
            }
        }
    }

    default void sendSnapshotSeq(IConnection conn) throws SQLException {
        long snapshotSeq = getSnapshotSeq();
        XConnection xConnection;
        if (conn.isWrapperFor(XConnection.class) &&
            (xConnection = conn.unwrap(XConnection.class)).supportMessageTimestamp()) {
            conn.flushUnsent();
            xConnection.setLazySnapshotSeq(snapshotSeq);
        } else {
            conn.executeLater("SET innodb_snapshot_seq = " + snapshotSeq);
        }
    }

}

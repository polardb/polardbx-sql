/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.optimizer.utils.IMppTsoTransaction;

import java.sql.SQLException;

/**
 * @author zhuangtianyi
 */
public interface ITsoTransaction extends IMppTsoTransaction {
    long getSnapshotSeq();

    boolean snapshotSeqIsEmpty();

    TransactionManager getManager();

    @Override
    default long nextTimestamp() {
        return getManager().getTimestampOracle().nextTimestamp();
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

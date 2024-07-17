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
public class ArchiveTransaction extends TsoTransaction {
    private final static long ARCHIVE_FORMAT_ID = 3;
    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.ARCHIVE + "]";

    public ArchiveTransaction(ExecutionContext executionContext,
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
            xid = XAUtils.toXidStringWithFormatId(id, group, primaryGroupUid, getReadViewSeq(group), ARCHIVE_FORMAT_ID);
        } else {
            xid = XAUtils.toXidStringWithFormatId(id, group, primaryGroupUid, ARCHIVE_FORMAT_ID);
        }
        conn.setTrxXid(xid);
        return xid;
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.ARCHIVE;
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }
}

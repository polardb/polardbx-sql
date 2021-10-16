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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.async.RotateGlobalTxLogTask;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;

public class PurgeTransHandler extends AbstractTransHandler {

    private final static Logger logger = LoggerFactory.getLogger(GlobalTxLogManager.class);

    private final String stmt;
    private final int offset;

    public PurgeTransHandler(String stmt, int offset, ServerConnection c) {
        super(c);
        this.stmt = stmt;
        this.offset = offset;
    }

    @Override
    protected void doExecute() {

        int before = -1;
        if (offset > 0) {
            String str = stmt.substring(offset);
            try {
                before = Integer.parseInt(str);
            } catch (NumberFormatException ex) {
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Incorrect number of seconds: " + str);
                return;
            }
        }

        TransactionExecutor executor = transactionManager.getTransactionExecutor();
        AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

        int purgedCount;
        try {
            RotateGlobalTxLogTask task =
                new RotateGlobalTxLogTask(executor, before, 0, asyncQueue);
            task.run();
            purgedCount = task.getPurgedCount();
        } catch (Throwable ex) {
            c.writeErrMessage(ErrorCode.ERR_TRANS_LOG, "Purge trans failed: " + ex.getMessage());
            logger.error("Purge trans failed", ex);
            return;
        }

        OkPacket packet = new OkPacket();
        packet.packetId = 1;
        packet.affectedRows = purgedCount;
        packet.serverStatus = 2;
        packet.write(PacketOutputProxyFactory.getInstance().createProxy(c));
    }

}

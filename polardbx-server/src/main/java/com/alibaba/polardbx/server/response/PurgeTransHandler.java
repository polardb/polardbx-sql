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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableTask;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.async.RotateGlobalTxLogTask;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;

import java.util.Calendar;

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
    protected boolean doExecute() {
        int before = -1;
        // 0 purge both new and legacy trx log table.
        // 1 purge legacy table.
        // 2 purge new table.
        int v = 0;
        if (offset > 0) {
            char str0 = stmt.charAt(offset - 1);
            String str = stmt.substring(offset);
            try {
                before = Integer.parseInt(str);
            } catch (NumberFormatException ex) {
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Incorrect number of seconds: " + str);
                return false;
            }

            if (str0 == 'v' || str0 == 'V') {
                if (before != 1 && before != 2) {
                    c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Incorrect version, expect 1 or 2, but was " + before);
                    return false;
                }
                v = before;
                before = -1;
            }
        }

        TransactionExecutor executor = transactionManager.getTransactionExecutor();
        AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

        long purgedCount = 0;
        if (1 == v || 0 == v) {
            try {
                final Calendar startTime = Calendar.getInstance();
                final Calendar endTime = Calendar.getInstance();
                startTime.set(Calendar.HOUR_OF_DAY, 0);
                startTime.set(Calendar.MINUTE, 0);
                startTime.set(Calendar.SECOND, 0);

                endTime.set(Calendar.HOUR_OF_DAY, 23);
                endTime.set(Calendar.MINUTE, 59);
                endTime.set(Calendar.SECOND, 59);
                RotateGlobalTxLogTask task =
                    new RotateGlobalTxLogTask(executor, startTime, endTime, before, 0, asyncQueue);
                task.run();
                purgedCount = task.getPurgedCount();
            } catch (Throwable ex) {
                c.writeErrMessage(ErrorCode.ERR_TRANS_LOG, "Purge trans failed: " + ex.getMessage());
                logger.error("Purge trans failed", ex);
                return false;
            }
        }

        if (2 == v || 0 == v) {
            StringBuffer remark = new StringBuffer();
            try {
                purgedCount += CleanLogTableTask.run(true, remark);
            } catch (Throwable ex) {
                c.writeErrMessage(ErrorCode.ERR_TRANS_LOG, "Purge trans failed on trx log table v2: "
                    + ex.getMessage());
                logger.error("Purge trans failed on trx log table v2:", ex);
                return false;
            } finally {
                logger.warn("Run purge trans V2 manually, result: " + remark);
            }
        }

        OkPacket packet = new OkPacket();
        packet.packetId = 1;
        packet.affectedRows = purgedCount;
        packet.serverStatus = 2;
        packet.write(PacketOutputProxyFactory.getInstance().createProxy(c));
        return true;
    }

}

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

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;

/**
 * 查询执行时间超过设定阈值的SQL
 *
 * @author wenfeng.cenwf 2011-4-20
 */
public final class ClearSQLSlow {

    public static boolean response(ServerConnection c, boolean hasMore) {
        // 取得SCHEMA
        String db = c.getSchema();
        if (db == null) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return false;
        }

        SchemaConfig schema = c.getSchemaConfig();
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return false;
        }

        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            try {
                ds.init();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_HANDLE_DATA, e);
                return false;
            }
        }

        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());
        SyncManagerHelper.sync(new ClearSQLSlowSyncAction(db), c.getSchema(), SyncScope.CURRENT_ONLY);
        PacketOutputProxyFactory.getInstance().createProxy(c)
            .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        return true;
    }

}

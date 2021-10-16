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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.sync.ReloadSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ReloadUtils;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReloadHandler {
    public static void handle(ByteString sqlBytes, ServerConnection c) {
        final String stmt = sqlBytes.toString();
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            // 取得SCHEMA
            String db = c.getSchema();
            if (db == null) {
                c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
                return;
            }

            SchemaConfig schema = CobarServer.getInstance().getConfig().getSchemas().get(db);
            if (schema == null) {
                c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
                return;
            }

            TDataSource ds = schema.getDataSource();
            if (!ds.isInited()) {
                try {
                    ds.init();
                } catch (Throwable e) {
                    c.handleError(ErrorCode.ERR_HANDLE_DATA, e);
                    return;
                }
            }

            OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());

            String pattern;
            Pattern r;
            Matcher m;

            pattern = "RELOAD[\\s]+DATASOURCES";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .sync(new ReloadSyncAction(ReloadUtils.ReloadType.DATASOURCES, c.getSchema()), c.getSchema());
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return;
            }

            pattern = "RELOAD[\\s]+SCHEMA";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .sync(new ReloadSyncAction(ReloadUtils.ReloadType.SCHEMA, c.getSchema()), c.getSchema());
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return;
            }

            pattern = "RELOAD[\\s]+USER";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .sync(new ReloadSyncAction(ReloadUtils.ReloadType.USERS, c.getSchema()), c.getSchema());
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return;
            }

            c.execute(sqlBytes, false);
            recordSql = false;
        } catch (Throwable ex) {
            sqlEx = ex;
            throw ex;
        } finally {
            if (recordSql) {
                LogUtils.recordSql(c, sqlBytes, sqlEx);
            }
        }
    }
}

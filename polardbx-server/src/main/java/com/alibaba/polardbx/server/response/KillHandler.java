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
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.ClusterAcceptIdGenerator;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xianmao.hexm 2011-5-18 下午05:59:02
 */
public final class KillHandler {
    private static final Logger logger = LoggerFactory.getLogger(KillHandler.class);

    public static boolean response(ByteString stmt, int offset, boolean killQuery, ServerConnection c,
                                   boolean hasMore) {
        // this may cause problem when kill is in multi statements
        ServerThreadPool killExecutor = CobarServer.getInstance().getKillExecutor();
        killExecutor.submit(c.getSchema(), c.getTraceId(), () -> {
            try {
                runKill(stmt, offset, killQuery, c, hasMore);
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_HANDLE_DATA, e);
            }
        });

        return true;
    }

    public static void runKill(ByteString stmt, int offset, boolean killQuery, ServerConnection c, boolean hasMore) {

        int count = 0;

        String id = stmt.substring(offset).trim();

        String db = c.getSchema();
        if (db == null) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }

        SchemaConfig schema = c.getSchemaConfig();
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

        long connId = Long.parseLong(id);

        KillSyncAction action = new KillSyncAction(c.getUser(), connId, killQuery, c.isSuperUserOrAllPrivileges(),
            com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_USER_CANCELED);

        if (connId >= 0 && ConfigDataMode.isPolarDbX()) {
            int nodeIndex = ClusterAcceptIdGenerator.getInstance().extractNodeIndexFromConnId(connId);
            GmsNodeManager.GmsNode node = null;

            if (nodeIndex >= 0) {
                node = GmsNodeManager.getInstance().getAllNodes().get(nodeIndex);
            }

            if (node != null) {
                List<Map<String, Object>> result = SyncManagerHelper.sync(action, c.getSchema(),
                    node.getServerKey());
                if (result != null) {
                    count += (Integer) result.iterator().next().get(ResultCursor.AFFECT_ROW);
                }
            }
        }

        if (count == 0) {
            List<List<Map<String, Object>>> results =
                SyncManagerHelper.sync(action, c.getSchema(), SyncScope.CURRENT_ONLY);

            for (List<Map<String, Object>> result : results) {
                if (result != null) {
                    count += (Integer) result.iterator().next().get(ResultCursor.AFFECT_ROW);
                }
            }
        }

        if (count > 0) {
            OkPacket packet = new OkPacket();
            packet.packetId = 1;
            packet.affectedRows = count;
            packet.serverStatus = 2;
            if (hasMore) {
                packet.serverStatus |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
            }
            packet.write(PacketOutputProxyFactory.getInstance().createProxy(c));
        } else {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD,
                "Unknown thread id: " + id + ", or you are not owner of thread " + id);
        }
    }

    private static List<FrontendConnection> getList(String stmt, int offset, ServerConnection sc) {
        String ids = stmt.substring(offset).trim();
        if (ids.length() > 0) {
            String[] idList = StringUtil.split(ids, ',', true);
            List<FrontendConnection> fcList = new ArrayList<FrontendConnection>(idList.length);
            NIOProcessor[] processors = CobarServer.getInstance().getProcessors();
            for (String id : idList) {
                long value = 0;
                try {
                    value = Long.parseLong(id);
                } catch (NumberFormatException e) {
                    continue;
                }
                FrontendConnection fc = null;
                for (NIOProcessor p : processors) {
                    if ((fc = p.getFrontends().get(value)) != null) {
                        fcList.add(fc);
                        break;
                    }
                }
            }
            return fcList;
        }
        return null;
    }

}

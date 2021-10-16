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
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateRowCountSyncAction;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;

import java.util.HashMap;
import java.util.Map;

public class CollectStatistic {
    public static void response(ServerConnection c) {
        SchemaConfig schema = c.getSchemaConfig();
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + c.getSchema() + "'");
            return;
        }

        if (SystemDbHelper.isDBBuildIn(schema.getName())) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR,
                "not support statistic collection for build-in database '" + c.getSchema() + "'");
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
        StatisticManager statisticManager = ds.getConfigHolder().getStatisticManager();
        statisticManager.startCollectOnceSync();
        Map<String, Long> rowCountMap = new HashMap<>();
        for (String logicalTableName : statisticManager.getStatisticCache().keySet()) {
            rowCountMap.put(logicalTableName, statisticManager.getCacheLine(logicalTableName).getRowCount());
        }
        SyncManagerHelper.sync(new UpdateRowCountSyncAction(c.getSchema(), rowCountMap), c.getSchema());
        PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
    }
}

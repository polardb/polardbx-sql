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
import com.alibaba.polardbx.executor.gms.util.StatisticFullProcessUtils;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.server.ServerConnection;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;

public class CollectStatistic {
    private static final Logger logger = LoggerFactory.getLogger(CollectStatistic.class);

    public static boolean response(ServerConnection c) {
        List<String> schemas = DbInfoManager.getInstance().getDbList();
        for (String schema : schemas) {
            schema = schema.toLowerCase(Locale.ROOT);
            if (SystemDbHelper.isDBBuildIn(schema)) {
                continue;
            }

            Set<String> logicalTableSet = Sets.newHashSet();
            for (TableMeta tableMeta : OptimizerContext.getContext(schema).getLatestSchemaManager()
                .getAllUserTables()) {
                logicalTableSet.add(tableMeta.getTableName().toLowerCase());
            }
            for (String logicalTableName : logicalTableSet) {
                // check table if exists
                if (OptimizerContext.getContext(schema).getLatestSchemaManager()
                    .getTableWithNull(logicalTableName) == null) {
                    continue;
                }
                // don't sample oss table
                if (StatisticUtils.isFileStore(schema, logicalTableName)) {
                    continue;
                }
                // sample
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        PROCESS_START,
                        new String[] {
                            "sample table ",
                            schema + "," + logicalTableName
                        },
                        NORMAL);
                StatisticFullProcessUtils.sampleOneTable(schema, logicalTableName);
            }
        }

        PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
        return true;
    }
}

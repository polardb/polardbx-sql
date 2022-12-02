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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.FetchDeadlockInfoSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.DeadlockParser;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.GLOBAL_DEADLOCK;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.MDL_DEADLOCK;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.NO_DEADLOCKS_DETECTED;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.containsMetaDb;

/**
 * @author wuzhe
 */
public class LogicalShowGlobalDeadlocksHandler extends HandlerCommon {
    public LogicalShowGlobalDeadlocksHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final ArrayResultCursor result = new ArrayResultCursor("GLOBAL_DEADLOCKS");
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("LOG", DataTypes.StringType);
        result.initMeta();

        List<List<Map<String, Object>>> results;
        final String schemaName = executionContext.getSchemaName();
        if (ExecUtils.hasLeadership(schemaName)) {
            // If I am the leader, just get deadlock information from StorageInfoManager
            final String deadlockInfo = StorageInfoManager.getDeadlockInfo();
            final String mdlDeadlockInfo = StorageInfoManager.getMdlDeadlockInfo();
            results = new LinkedList<>();
            if (null != deadlockInfo && null != mdlDeadlockInfo) {
                results.add(ImmutableList.of(
                    ImmutableMap.of("TYPE", GLOBAL_DEADLOCK, "LOG", deadlockInfo),
                    ImmutableMap.of("TYPE", MDL_DEADLOCK, "LOG", mdlDeadlockInfo)));
            }
        } else {
            // Otherwise, get deadlock information from leader
            results = SyncManagerHelper.sync(new FetchDeadlockInfoSyncAction(schemaName), schemaName);
        }

        if (CollectionUtils.isNotEmpty(results)) {
            for (List<Map<String, Object>> deadlockInfo : results) {
                if (null == deadlockInfo) {
                    continue;
                }
                for (Map<String, Object> row : deadlockInfo) {
                    final String type = (String) row.get("TYPE");
                    String log = (String) row.get("LOG");
                    // Inject logical tables
                    final Map<String, String> physicalToLogical = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    log = DeadlockParser.injectLogicalTables(log, physicalToLogical);

                    // User should not see deadlocks in meta-db.
                    if (containsMetaDb(log)) {
                        result.addRow(new Object[] {type, NO_DEADLOCKS_DETECTED});
                    } else {
                        result.addRow(new Object[] {type, log});
                    }

                }
            }
        }

        return result;
    }
}

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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.DeadlockParser;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

/**
 * @author wuzhe
 */
public class LogicalShowLocalDeadlocksHandler extends HandlerCommon {

    public final static String SHOW_ENGINE_INNODB_STATUS = "show engine innodb status";

    public LogicalShowLocalDeadlocksHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final ArrayResultCursor result = new ArrayResultCursor("LOCAL_DEADLOCKS");
        result.addColumn("DN ID", DataTypes.StringType);
        result.addColumn("LOG", DataTypes.StringType);
        result.initMeta();

        // 1. Get all DN's storage id
        Set<String> allDnId = ExecUtils.getAllDnStorageId();

        // 2. Query each DN for deadlock information, and add it to the result
        generateDeadlockLogs(allDnId, result);

        return result;
    }

    /**
     * @param allDnId is a set of all dn's storage instance id
     * @param result is updated in this method
     */
    private void generateDeadlockLogs(Set<String> allDnId,
                                      ArrayResultCursor result) {
        for (String dnId : allDnId) {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery(SHOW_ENGINE_INNODB_STATUS);
                if (rs.next()) {
                    final String status = rs.getString("Status");
                    if (null != status) {
                        // Skip metaDB
                        if (StringUtils.containsIgnoreCase(dnId, "pxc-xdb-m-")) {
                            continue;
                        }

                        // Parse the {status} to get deadlock information,
                        final String deadlockLog =
                            DeadlockParser.parseLocalDeadlock(status);

                        result.addRow(new Object[] {dnId, deadlockLog});
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to fetch deadlock information on dn " + dnId, e);
            }
        }
    }
}

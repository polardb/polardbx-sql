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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * For PolarDB-X only.
 */
public class TablesMetaChangeCrossDBPreemptiveSyncAction implements ISyncAction {
    private String schemaName;

    private List<String> multiSchemas;
    private List<List<String>> logicalTables;
    private PreemptiveTime preemptiveTime;

    public TablesMetaChangeCrossDBPreemptiveSyncAction() {

    }

    public TablesMetaChangeCrossDBPreemptiveSyncAction(String schemaName, List<String> schemas,
                                                       List<List<String>> logicalTables,
                                                       PreemptiveTime preemptiveTime) {
        this.schemaName = schemaName;
        this.multiSchemas = schemas;
        this.logicalTables = logicalTables;
        this.preemptiveTime = preemptiveTime;
    }

    @Override
    public ResultCursor sync() {

        // sort schema to avoid deadlock
        List<Integer> ranks = new ArrayList<>(multiSchemas.size());
        for (int i = 0; i < multiSchemas.size(); i++) {
            if (OptimizerContext.getContext(multiSchemas.get(i)) != null) {
                ranks.add(i);
            }
        }
        ranks.sort((x, y) -> multiSchemas.get(x).compareToIgnoreCase(multiSchemas.get(y)));
        recursiveLock(ranks, 0);
        return null;
    }

    void recursiveLock(List<Integer> ranks, int depth) {
        if (depth == ranks.size()) {

            Map<String, MdlInfo> mdlInfoMap = TreeMaps.caseInsensitiveMap();

            try (java.sql.Connection metaDbConn = MetaDbUtil.getConnection()) {
                metaDbConn.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
                // collect new schema manager
                for (int rank : ranks) {
                    String localSchema = multiSchemas.get(rank);
                    List<String> tableNameList = logicalTables.get(rank);
                    SchemaManager oldSchemaManager = OptimizerContext.getContext(localSchema).getLatestSchemaManager();
                    Map<String, Long> staleTables =
                        oldSchemaManager.getStaleTables(logicalTables.get(rank), metaDbConn);
                    if (staleTables.isEmpty()) {
                        continue;
                    }

                    // Load new TableMeta
                    GmsTableMetaManager newSchemaManager =
                        new GmsTableMetaManager((GmsTableMetaManager) oldSchemaManager,
                            tableNameList,
                            oldSchemaManager.getTddlRuleManager(), metaDbConn);

                    newSchemaManager.init();
                    //OrcColumnManager.getINSTANCE().rebuild(schemaName, staleTables.keySet());
                    mdlInfoMap.put(localSchema, new MdlInfo(oldSchemaManager, newSchemaManager, staleTables));
                }
            } catch (SQLException e) {
                throw new TddlNestableRuntimeException(e);
            }

            //replace schema manager(caution: the step is not atomic)
            for (Map.Entry<String, MdlInfo> entry : mdlInfoMap.entrySet()) {
                OptimizerContext.getContext(entry.getKey()).setSchemaManager(entry.getValue().newSchemaManager);
            }

            // print meta change log
            for (Map.Entry<String, MdlInfo> entry : mdlInfoMap.entrySet()) {
                String localSchema = entry.getKey();
                MdlInfo mdlInfo = entry.getValue();
                String hashCode = String.valueOf(System.identityHashCode(mdlInfo.newSchemaManager));
                for (Map.Entry<String, Long> table : mdlInfo.staleTables.entrySet()) {
                    String localTable = table.getKey();
                    TableMeta currentMeta = mdlInfo.oldSchemaManager.getTableWithNull(localTable);
                    long oldVersion = currentMeta == null ? 0 : currentMeta.getVersion();
                    SQLRecorderLogger.ddlMetaLogger.info(MessageFormat.format(
                        "{0} reload table metas for {1}.[{2}]: since meta version of table {3} change from {4} to {5}",
                        hashCode, localSchema,
                        mdlInfo.staleTables.keySet(), localTable, oldVersion, table.getValue()));
                }
            }

            // Insert mdl barrier
            for (Map.Entry<String, MdlInfo> entry : mdlInfoMap.entrySet()) {
                GmsTableMetaManager oldSchemaManager = (GmsTableMetaManager) entry.getValue().oldSchemaManager;
                oldSchemaManager.mdlCriticalSection(true, preemptiveTime, null,
                    oldSchemaManager, entry.getValue().staleTables.keySet(),
                    DbInfoManager.getInstance().isNewPartitionDb(entry.getKey()), false, (x) -> {
                        oldSchemaManager.expire();
                        return null;
                    },1L);
            }
            return;
        }

        synchronized (Objects.requireNonNull(OptimizerContext.getContext(multiSchemas.get(ranks.get(depth))))) {
            recursiveLock(ranks, depth + 1);
        }
    }

    private static class MdlInfo {
        SchemaManager oldSchemaManager;
        SchemaManager newSchemaManager;
        Map<String, Long> staleTables;

        public MdlInfo(SchemaManager oldSchemaManager, SchemaManager newSchemaManager, Map<String, Long> staleTables) {
            this.oldSchemaManager = oldSchemaManager;
            this.newSchemaManager = newSchemaManager;
            this.staleTables = staleTables;
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<String> getMultiSchemas() {
        return multiSchemas;
    }

    public void setMultiSchemas(List<String> multiSchemas) {
        this.multiSchemas = multiSchemas;
    }

    public void setLogicalTables(List<List<String>> logicalTables) {
        this.logicalTables = logicalTables;
    }

    public List<List<String>> getLogicalTables() {
        return logicalTables;
    }

}

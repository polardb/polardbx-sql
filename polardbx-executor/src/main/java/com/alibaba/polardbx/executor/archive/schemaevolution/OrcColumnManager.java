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

package com.alibaba.polardbx.executor.archive.schemaevolution;

import com.alibaba.polardbx.executor.ddl.newengine.meta.SchemaEvolutionAccessorDelegate;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class OrcColumnManager {
    private static final class InstanceHolder {
        static final OrcColumnManager INSTANCE = new OrcColumnManager();
    }

    public static OrcColumnManager getINSTANCE() {
        return InstanceHolder.INSTANCE;
    }

    private final Map<String, ConcurrentSkipListMap<Long, ColumnMetaWithTs>> columnMap;

    public OrcColumnManager() {
        this.columnMap = new ConcurrentHashMap<>();
    }

    /**
     * get the history version of the column
     *
     * @param fieldId the unique physical name of the column
     * @param version the version hope to find
     * @return a column of version no larger than the version to find, null if the column is not available
     */
    private ColumnMeta getHistoryInner(String fieldId, long version) {
        Map.Entry<Long, ColumnMetaWithTs> entry = columnMap.get(fieldId).floorEntry(version);
        return entry == null ? null : entry.getValue().getMeta();
    }

    private ColumnMetaWithTs getHistoryWithTsInner(String fieldId, long version) {
        Map.Entry<Long, ColumnMetaWithTs> entry = columnMap.get(fieldId).floorEntry(version);
        return entry == null ? null : entry.getValue();
    }

    private ColumnMetaWithTs getFirstInnerWithTs(String fieldId) {
        Map.Entry<Long, ColumnMetaWithTs> entry = columnMap.get(fieldId).firstEntry();
        return entry == null ? null : entry.getValue();
    }

    public static ColumnMeta getHistory(String fieldId, OSSOrcFileMeta fileMeta) {
        return getINSTANCE().getHistoryInner(fieldId, fileMeta.getCommitTs());
    }

    public static ColumnMetaWithTs getHistoryWithTs(String fieldId, long version) {
        return getINSTANCE().getHistoryWithTsInner(fieldId, version);
    }

    public static ColumnMetaWithTs getFirst(String fieldId) {
        return getINSTANCE().getFirstInnerWithTs(fieldId);
    }

    /**
     * rebuild column evolution in a ddl for a table
     * There are four cases for each column:
     * 1. nothing happened
     * 2. a new column
     * 3. a new column evolution
     * 4. delete a column
     *
     * @param schemaName target schema
     * @param tableName target column
     */
    public void rebuild(String schemaName, String tableName) {
        new SchemaEvolutionAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                // don't continue the ddl if it was paused
                List<ColumnEvolutionRecord>
                    records = columnEvolutionAccessor.querySchemaTable(schemaName, tableName);
                for (ColumnEvolutionRecord record : records) {
                    long fieldId = record.getFieldId();
                    String fieldIdString = record.getFieldIdString();
                    // new column
                    if (!columnMap.containsKey(fieldIdString)) {
                        ConcurrentSkipListMap<Long, ColumnMetaWithTs> versionedColumnMetas =
                            new ConcurrentSkipListMap();
                        List<ColumnEvolutionRecord> realRecords =
                            columnEvolutionAccessor.queryFieldTs(fieldId, -1L);
                        for (ColumnEvolutionRecord realRecord : realRecords) {
                            ColumnsRecord columnsRecord = realRecord.getColumnRecord();
                            ColumnMeta columnMeta = GmsTableMetaManager.buildColumnMeta(
                                columnsRecord,
                                tableName,
                                columnsRecord.collationName,
                                columnsRecord.characterSetName);
                            versionedColumnMetas.put(realRecord.getTs(),
                                new ColumnMetaWithTs(realRecord.getCreate(), columnMeta));
                        }
                        columnMap.put(fieldIdString, versionedColumnMetas);
                        continue;
                    }

                    ConcurrentSkipListMap<Long, ColumnMetaWithTs> metas = columnMap.get(fieldIdString);
                    Long maxTso = metas.lastKey();
                    // new evolution
                    if (maxTso <= record.getTs()) {
                        List<ColumnEvolutionRecord> realRecords =
                            columnEvolutionAccessor.queryFieldTs(fieldId, maxTso);
                        for (ColumnEvolutionRecord realRecord : realRecords) {
                            ColumnsRecord columnsRecord = realRecord.getColumnRecord();
                            ColumnMeta columnMeta = GmsTableMetaManager.buildColumnMeta(
                                columnsRecord,
                                tableName,
                                columnsRecord.collationName,
                                columnsRecord.characterSetName);
                            metas.put(realRecord.getTs(), new ColumnMetaWithTs(realRecord.getCreate(), columnMeta));
                        }
                    }
                }
                // delete column
                for (ColumnMappingRecord mappingRecord :
                    columnMappingAccessor.querySchemaTableAbsent(schemaName, tableName)) {
                    columnMap.remove(mappingRecord.getFieldIdString());
                }
                return 0;
            }
        }.execute();
    }

}

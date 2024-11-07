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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableDetail;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaTableDetailHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaTableDetailHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    private static char finishChar = '█';
    private static char unFinishChar = '-';

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTableDetail;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        // only new partitioning db
        Set<String> schemaNames = new TreeSet<>(String::compareToIgnoreCase);
        schemaNames.addAll(StatsUtils.getDistinctSchemaNames());

        final int schemaIndex = InformationSchemaTableDetail.getTableSchemaIndex();
        final int tableIndex = InformationSchemaTableDetail.getTableNameIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        schemaNames = virtualView.applyFilters(schemaIndex, params, schemaNames);

        // tableIndex
        Set<String> indexTableNames = virtualView.getEqualsFilterValues(tableIndex, params);
        // tableLike
        String tableLike = virtualView.getLikeString(tableIndex, params);

        List<TableGroupConfig> allTableGroupConfigs = StatsUtils.getTableGroupConfigs(schemaNames);

        List<TableGroupConfig> tableGroupConfigs =
            StatsUtils.getTableGroupConfigsWithFilter(allTableGroupConfigs, schemaNames);
        Set<String> gsiTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        queryStats(tableLike, indexTableNames, schemaNames, tableGroupConfigs, true, executionContext, gsiTableNames,
            cursor);
        if (GeneralUtil.isNotEmpty(gsiTableNames)) {
            queryStats(null, gsiTableNames, schemaNames, tableGroupConfigs, false, executionContext, gsiTableNames,
                cursor);
        }
        return cursor;
    }

    private void queryStats(String tableLike,
                            Set<String> logicalTableNames,
                            Set<String> schemaNames,
                            List<TableGroupConfig> tableGroupConfigs,
                            boolean isPrimaryTable,
                            ExecutionContext executionContext,
                            Set<String> gsiNames,
                            ArrayResultCursor cursor) {
        Map<String/**phyDbName**/, Pair<String/**storageInstId**/, String/**groupName**/>> storageInstIdGroupNames =
            new HashMap<>();

        // get all phy tables(partitions) info from all DNs
        Map<String/** dbName **/, Map<String, List<Object>> /** phy tables **/> phyDbTablesInfo =
            StatsUtils.queryTableSchemaStats(schemaNames, logicalTableNames, tableLike, storageInstIdGroupNames, null);

        for (TableGroupConfig tableGroupConfig : tableGroupConfigs) {
            if (tableGroupConfig.getTableCount() == 0) {
                continue;
            }
            String schemaName = tableGroupConfig.getTableGroupRecord().schema;

            Map<String, Map<String, Map<String, Object>>> tableGroupStatInfo =
                StatsUtils.queryTableGroupStatInfos(tableGroupConfig, logicalTableNames, tableLike, phyDbTablesInfo);

            if (MapUtils.isEmpty(tableGroupStatInfo)) {
                continue;
            }
            List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
            /**
             * key: partName
             * val: phyDb
             * Prepare the partName->phyDb map for table group
             */
            Map<String, String> partitionPyhDbMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            if (CollectionUtils.isNotEmpty(partitionGroupRecords)) {
                partitionPyhDbMap.putAll(partitionGroupRecords.stream().collect(Collectors.toMap(
                    PartitionGroupRecord::getPartition_name, PartitionGroupRecord::getPhy_db)));
            }
            for (String tableName : tableGroupConfig.getAllTables()) {
                String logicalTableName = tableName.toLowerCase();
                TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);
                String indexName = StringUtils.EMPTY;
                if (tableMeta.isGsi()) {
                    if (isPrimaryTable) {
                        continue;
                    }
                    logicalTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                    indexName = TddlSqlToRelConverter.unwrapGsiName(tableName.toLowerCase());
                }
                if (!StatsUtils.isFilterTable(logicalTableNames, tableLike, logicalTableName) && isPrimaryTable) {
                    continue;
                }

                Map<String, Map<String, Object>> phyTblStatInfoOfOneLogTb =
                    tableGroupStatInfo.get(tableName.toLowerCase());

                if (phyTblStatInfoOfOneLogTb == null) {
                    continue;
                }

                if (isPrimaryTable && tableMeta.withGsi()) {
                    gsiNames.addAll(tableMeta.getGsiTableMetaBean().indexMap.keySet());
                }

                Objects.requireNonNull(phyTblStatInfoOfOneLogTb,
                    String.format("table meta corrupted: %s.%s", schemaName, tableName));

                Long totalRows = 0L;
                for (Map.Entry<String, Map<String, Object>> phyEntry : phyTblStatInfoOfOneLogTb.entrySet()) {
                    totalRows += DataTypes.LongType.convertFrom(phyEntry.getValue().get("physicalTableRows"));
                }

                /**
                 * Fetch all the phyPartRecords of metadb
                 */
                List<PartitionSpec> partitionSpecs =
                    tableMeta.getPartitionInfo().getPartitionBy().getPhysicalPartitions();
                for (int i = 0; i < partitionSpecs.size(); i++) {
                    /**
                     * record is a record of phySpec
                     */
                    PartitionSpec record = partitionSpecs.get(i);

                    Map<String, Object> tableStatRow =
                        phyTblStatInfoOfOneLogTb.get(record.getLocation().getPhyTableName().toLowerCase());

                    Objects.requireNonNull(tableStatRow,
                        String.format("physical table meta corrupted: %s.%s.%s",
                            schemaName, tableMeta.getTableName(), record.getLocation().getPhyTableName()));

                    String partName = DataTypes.StringType.convertFrom(tableStatRow.get("partName"));
                    String subpartName = DataTypes.StringType.convertFrom(tableStatRow.get("subpartName"));
                    String subpartTemplateName =
                        DataTypes.StringType.convertFrom(tableStatRow.get("subpartTemplateName"));
                    Long phyPartPosition = DataTypes.LongType.convertFrom(tableStatRow.get("phyPartPosition"));
                    String boundValue = DataTypes.StringType.convertFrom(tableStatRow.get("boundValue"));
                    String subBoundValue = DataTypes.StringType.convertFrom(tableStatRow.get("subBoundValue"));

                    Long physicalTableRows = DataTypes.LongType.convertFrom(tableStatRow.get("physicalTableRows"));
                    Long physicalDataLength = DataTypes.LongType.convertFrom(tableStatRow.get("physicalDataLength"));
                    Long physicalIndexLength = DataTypes.LongType.convertFrom(tableStatRow.get("physicalIndexLength"));
                    Long physicalDataFree = DataTypes.LongType.convertFrom(tableStatRow.get("physicalDataFree"));

                    Long physicalRowsRead = 0L;
                    if (tableStatRow.containsKey("physicalRowsRead")) {
                        physicalRowsRead = DataTypes.LongType.convertFrom(tableStatRow.get("physicalRowsRead"));
                    }

                    Long physicalRowsInserted = 0L;
                    if (tableStatRow.containsKey("physicalRowsInserted")) {
                        physicalRowsInserted = DataTypes.LongType.convertFrom(tableStatRow.get("physicalRowsInserted"));
                    }

                    Long physicalRowsUpdated = 0L;
                    if (tableStatRow.containsKey("physicalRowsUpdated")) {
                        physicalRowsUpdated = DataTypes.LongType.convertFrom(tableStatRow.get("physicalRowsUpdated"));
                    }

                    Long physicalRowsDeleted = 0L;
                    if (tableStatRow.containsKey("physicalRowsDeleted")) {
                        physicalRowsDeleted = DataTypes.LongType.convertFrom(tableStatRow.get("physicalRowsDeleted"));
                    }

                    double percent = Math.min(100.0, physicalTableRows / Math.max(totalRows.doubleValue(), 1));
                    /**
                     * fetch phyDb by the phyPartName of phySpec
                     */
                    String phyDb = partitionPyhDbMap.get(record.getName());

                    Pair<String/**storageInstId**/, String/**groupName**/> pair = storageInstIdGroupNames.get(phyDb);
                    String storageInstId = pair.getKey();
                    String groupName = pair.getValue();
                    String phyTblName = record.getLocation().getPhyTableName();

                    Object[] row = new Object[22];
                    cursor.addRow(row);
                    int index = 0;

                    row[index++] = DataTypes.StringType.convertFrom(schemaName);
                    row[index++] = DataTypes.StringType.convertFrom(tableGroupConfig.getTableGroupRecord().tg_name);
                    row[index++] = DataTypes.StringType.convertFrom(logicalTableName);
                    row[index++] = tableMeta.isGsi() ? indexName : StringUtils.EMPTY;
                    row[index++] = DataTypes.StringType.convertFrom(phyTblName);

                    // PARTITION_SEQ
                    row[index++] = DataTypes.ULongType.convertFrom(phyPartPosition);
                    // PARTITION_NAME
                    row[index++] = DataTypes.StringType.convertFrom(partName);
                    // SUBPARTITION_NAME
                    row[index++] = DataTypes.StringType.convertFrom(subpartName);
                    // SUBPARTITION_TEMPLATE_NAME
                    row[index++] = DataTypes.StringType.convertFrom(subpartTemplateName);

                    // TABLE_ROWS
                    row[index++] = DataTypes.ULongType.convertFrom(physicalTableRows);
                    // DATA_LENGTH
                    row[index++] = DataTypes.ULongType.convertFrom(physicalDataLength);
                    // INDEX_LENGTH
                    row[index++] = DataTypes.ULongType.convertFrom(physicalIndexLength);
                    // DATA_FREE
                    row[index++] = DataTypes.ULongType.convertFrom(physicalDataFree);

                    // BOUND_VALUE, a complete interval with lower_bnd and upper_bnd for part
                    row[index++] = DataTypes.StringType.convertFrom(boundValue);
                    // SUB_BOUND_VALUE, a complete interval with lower_bnd and upper_bnd for subpart
                    row[index++] = DataTypes.StringType.convertFrom(subBoundValue);

                    // PERCENT
                    row[index++] = DataTypes.StringType.convertFrom(getPercentString(percent));
                    // STORAGE_INST_ID
                    row[index++] = DataTypes.StringType.convertFrom(storageInstId);
                    // GROUP_NAME
                    row[index++] = DataTypes.StringType.convertFrom(groupName);

                    // ROWS_READ
                    row[index++] = DataTypes.ULongType.convertFrom(physicalRowsRead);

                    // ROWS_INSERTED
                    row[index++] = DataTypes.ULongType.convertFrom(physicalRowsInserted);

                    // ROWS_UPDATED
                    row[index++] = DataTypes.ULongType.convertFrom(physicalRowsUpdated);

                    // ROWS_DELETED
                    row[index++] = DataTypes.ULongType.convertFrom(physicalRowsDeleted);

                }
            }
        }
    }

    private String getPercentString(double percent) {
        StringBuilder finish = new StringBuilder();
        StringBuilder unFinish = new StringBuilder();

        for (int i = 0; i < percent * 100 / 4; i++) {
            finish.append(finishChar);
        }
        for (int i = 0; i < (100 - percent * 100) / 4; i++) {
            unFinish.append(unFinishChar);
        }
        return String.format("%.2f%%├%s%s┤", percent * 100, finish.toString(), unFinish.toString());
    }
}

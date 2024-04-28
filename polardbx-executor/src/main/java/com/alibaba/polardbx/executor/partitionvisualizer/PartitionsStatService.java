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

package com.alibaba.polardbx.executor.partitionvisualizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.partitionvisualizer.model.PartitionHeatInfo;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * 采集分区原始统计信息的服务
 *
 * @author ximing.yd
 */
public class PartitionsStatService {

    private static final Logger logger = LoggerFactory.getLogger(PartitionsStatService.class);

    private final Integer MAX_PARTITIONS_NAME_LENGTH = 50;

    public static Map<String/**originBound**/, Pair<Long/**tableRows**/, String/**storageInstId**/>> BOUND_META_MAP =
        new HashMap<>();

    public static Map<String/**phyDbName,phyTbName**/, Long/**tableRows**/> PHY_TABLE_ROWS_MAP = new HashMap<>();

    public static String LAST_COLLECTION_ONLY_PARAM = "";

    public static boolean HAS_CHANGED_COLLECTION_ONLY_PARAM = false;

    public List<PartitionHeatInfo> queryPartitionsStat(boolean usePhyTableRowsCache) {
        Set<String> schemaNames = new TreeSet<>(String::compareToIgnoreCase);
        schemaNames.addAll(StatsUtils.getDistinctSchemaNames());
        List<TableGroupConfig> allTableGroupConfigs = StatsUtils.getTableGroupConfigs(schemaNames);

        Map<String/**phyDbName**/, Pair<String/**storageInstId**/, String/**groupName**/>> storageInstIdGroupNames
            = new HashMap<>();
        // get all phy tables(partitions) info from all DNs
        Map<String/** phyDbName **/, Map<String/** phyTbName **/, List<Object>/** statics **/>>
            phyDbTablesInfoForHeatmap = new HashMap<>();

        Integer maxScan = getPartitionsHeatmapCollectionMaxScan();
        Integer maxSingleLogicSchemaCount = getPartitionsHeatmapCollectionMaxSingleLogicSchemaCount();
        HashMap<String, Set<String>> onlySchemaTables = getPartitionsHeatmapCollectionOnly();
        if (!MapUtils.isEmpty(onlySchemaTables)) {
            Set<String> anyTables = onlySchemaTables.get("");
            if (!CollectionUtils.isEmpty(anyTables) && onlySchemaTables.size() == 1) {
                //only tables of all schemas
                if (PHY_TABLE_ROWS_MAP.isEmpty() || !usePhyTableRowsCache || HAS_CHANGED_COLLECTION_ONLY_PARAM) {
                    PHY_TABLE_ROWS_MAP = new HashMap<>();
                    phyDbTablesInfoForHeatmap = StatsUtils.queryTableSchemaStatsForHeatmap(schemaNames, anyTables,
                        storageInstIdGroupNames, maxScan, maxSingleLogicSchemaCount, PHY_TABLE_ROWS_MAP);
                } else {
                    phyDbTablesInfoForHeatmap = StatsUtils.queryTableSchemaStaticsWithoutRowsForHeatmap(schemaNames,
                        anyTables, storageInstIdGroupNames, maxScan, maxSingleLogicSchemaCount, PHY_TABLE_ROWS_MAP);
                }
            } else {
                //only schemas
                PHY_TABLE_ROWS_MAP = new HashMap<>();
                Set<String> schemaNamesNew = new TreeSet<>(String::compareToIgnoreCase);
                for (Map.Entry<String, Set<String>> entry : onlySchemaTables.entrySet()) {
                    String schema = entry.getKey();
                    if (StringUtils.isEmpty(schema) || !schemaNames.contains(schema)) {
                        continue;
                    }
                    schemaNamesNew.add(schema);
                    Set<String> schemaSet = new TreeSet<>(String::compareToIgnoreCase);
                    schemaSet.add(schema);
                    Set<String> tables = entry.getValue();
                    if (!CollectionUtils.isEmpty(anyTables)) {
                        tables.addAll(anyTables);
                    }
                    phyDbTablesInfoForHeatmap.putAll(StatsUtils.queryTableSchemaStatsForHeatmap(schemaSet, tables,
                        storageInstIdGroupNames, maxScan, maxSingleLogicSchemaCount, PHY_TABLE_ROWS_MAP));
                }
                schemaNames = schemaNamesNew;
            }
        } else {
            if (PHY_TABLE_ROWS_MAP.isEmpty() || !usePhyTableRowsCache || HAS_CHANGED_COLLECTION_ONLY_PARAM) {
                PHY_TABLE_ROWS_MAP = new HashMap<>();
                phyDbTablesInfoForHeatmap = StatsUtils.queryTableSchemaStatsForHeatmap(schemaNames, null,
                    storageInstIdGroupNames, maxScan, maxSingleLogicSchemaCount, PHY_TABLE_ROWS_MAP);
            } else {
                phyDbTablesInfoForHeatmap = StatsUtils.queryTableSchemaStaticsWithoutRowsForHeatmap(schemaNames,
                    null, storageInstIdGroupNames, maxScan, maxSingleLogicSchemaCount, PHY_TABLE_ROWS_MAP);
            }
        }

        List<TableGroupConfig> tableGroupConfigs =
            StatsUtils.getTableGroupConfigsWithFilter(allTableGroupConfigs, schemaNames);

        Integer partitionsNum = 0;
        Set<String> storageInstIdSet = new HashSet<>();
        Map<String/**schema,logicTbName**/, Integer/**partitionsNum**/> partitionsNumMap = new HashMap<>();

        List<PartitionHeatInfo> partitionHeatInfos = new ArrayList<>();
        List<Long> tableRowList = new ArrayList<>();

        for (TableGroupConfig tableGroupConfig : tableGroupConfigs) {
            if (tableGroupConfig.getTableCount() == 0) {
                logger.warn("tableGroupConfig is null");
                continue;
            }
            String schemaName = tableGroupConfig.getTableGroupRecord().schema;

            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " not exists");
            PartitionInfoManager pm = oc.getPartitionInfoManager();

            Set<String> indexTableNames = new TreeSet<>();
            if (!MapUtils.isEmpty(onlySchemaTables)) {
                Set<String> anyTables = onlySchemaTables.get("");
                if (!CollectionUtils.isEmpty(anyTables)) {
                    indexTableNames.addAll(anyTables);
                }
                Set<String> schemaTables = onlySchemaTables.get(schemaName);
                if (!CollectionUtils.isEmpty(schemaTables)) {
                    indexTableNames.addAll(schemaTables);
                }
            }

            Map<String/**logicalTableName**/, Map<String/**phyTableName**/, List<Object>>> tablesStatInfoForHeatmap =
                StatsUtils.queryTableGroupStatsForHeatmap(tableGroupConfig, indexTableNames, null,
                    phyDbTablesInfoForHeatmap);

            if (MapUtils.isEmpty(tablesStatInfoForHeatmap)) {
                continue;
            }
            List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
            HashMap<String, String> partitionPyhDbMap = new HashMap<>();
            if (CollectionUtils.isNotEmpty(partitionGroupRecords)) {
                partitionPyhDbMap.putAll(partitionGroupRecords.stream().collect(Collectors.toMap(
                    PartitionGroupRecord::getPartition_name, PartitionGroupRecord::getPhy_db)));
            }
            for (String logicalTableName : tableGroupConfig.getAllTables()) {
                logicalTableName = logicalTableName.toLowerCase();
                if (!StatsUtils.isFilterTable(indexTableNames, null, logicalTableName)) {
                    continue;
                }
                Map<String, List<Object>> tableStatInfoForHeatmap =
                    tablesStatInfoForHeatmap.get(logicalTableName);
                if (tableStatInfoForHeatmap == null) {
                    logger.warn(String.format("table meta tableStatInfo is null: schemaName:%s, tableName:%s",
                        schemaName, logicalTableName));
                    continue;
                }
                PartitionInfo partitionInfo = pm.getPartitionInfo(logicalTableName);

                List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPhysicalPartitions();
                for (int i = 0; i < partitionSpecs.size(); i++) {
                    PartitionSpec record = partitionSpecs.get(i);
                    List<Object> tableStatRowForHeatmap =
                        tableStatInfoForHeatmap.get(record.getLocation().getPhyTableName().toLowerCase());
                    if (CollectionUtils.isEmpty(tableStatRowForHeatmap)) {
                        logger.warn(String.format(
                            "physical table meta tableStatRow is null: schemaName:%s, tableName:%s, phyTable:%s",
                            schemaName, schemaName, record.getLocation().getPhyTableName()));
                        continue;
                    }

                    long tableRow = DataTypes.LongType.convertFrom(tableStatRowForHeatmap.get(2));
                    String phyTableRowsKey =
                        StatsUtils.getPhyTableRowsKey(((String) tableStatRowForHeatmap.get(1)).toLowerCase(),
                            ((String) tableStatRowForHeatmap.get(0)).toLowerCase());
                    Long tableRowCache = PHY_TABLE_ROWS_MAP.get(phyTableRowsKey);
                    if (tableRowCache != null) {
                        tableRow = tableRowCache;
                    }
                    tableRowList.add(tableRow);

                    String phyDb = partitionPyhDbMap.get(record.getName());
                    Pair<String/**storageInstId**/, String/**groupName**/> pair = storageInstIdGroupNames.get(phyDb);
                    String storageInstId = pair.getKey();

                    partitionsNum++;
                    String tableKey = VisualConvertUtil.genTableKey(schemaName, record.getLocation().getPhyTableName());
                    storageInstIdSet.add(storageInstId);
                    partitionsNumMap.merge(tableKey, 1, Integer::sum);

                    PartitionHeatInfo partitionHeatInfo = generatePartitionHeatInfo(schemaName, logicalTableName,
                        record.getName(), storageInstId,
                        i, tableRow, tableStatRowForHeatmap);
                    partitionHeatInfos.add(partitionHeatInfo);
                }
            }
        }
        List<PartitionHeatInfo> mergedPartitionHeatInfos =
            mergePartitionHeatInfo(partitionHeatInfos, tableRowList, partitionsNum, storageInstIdSet.size(),
                partitionsNumMap);

        BOUND_META_MAP = getBoundRowsMap(mergedPartitionHeatInfos);

        return mergedPartitionHeatInfos;
    }

    private PartitionHeatInfo generatePartitionHeatInfo(String schemaName, String logicalTableName,
                                                        String partitionName,
                                                        String storageInstId, int index, long tableRow,
                                                        List<Object> tableStatRow) {
        PartitionHeatInfo partitionHeatInfo = new PartitionHeatInfo();
        partitionHeatInfo.setSchemaName(schemaName);
        partitionHeatInfo.setLogicalTable(logicalTableName);
        partitionHeatInfo.setPartitionName(partitionName);
        partitionHeatInfo.setStorageInstId(storageInstId);
        partitionHeatInfo.setPartitionSeq(index);
        partitionHeatInfo.setTableRows(tableRow);

        Long rowRead = 0L;
        Long rowWritten = 0L;
        Long ReadWritten = 0L;
        if (tableStatRow != null && tableStatRow.size() > 3) {
            Object rowReadObj = DataTypes.ULongType.convertFrom(tableStatRow.get(3));
            Object rowInsertedObj = DataTypes.ULongType.convertFrom(tableStatRow.get(4));
            Object rowUpdatedobj = DataTypes.ULongType.convertFrom(tableStatRow.get(5));
            rowRead = VisualConvertUtil.getObjLong(rowReadObj);
            rowWritten = VisualConvertUtil.sumRows(VisualConvertUtil.getObjLong(rowInsertedObj),
                VisualConvertUtil.getObjLong(rowUpdatedobj));
            ReadWritten = VisualConvertUtil.sumRows(rowRead, rowWritten);
        }

        partitionHeatInfo.setRowsRead(rowRead);
        partitionHeatInfo.setRowsWritten(rowWritten);
        partitionHeatInfo.setRowsReadWritten(ReadWritten);
        return partitionHeatInfo;
    }

    private Map<String, Pair<Long, String>> getBoundRowsMap(List<PartitionHeatInfo> partitionHeatInfos) {
        Map<String, Pair<Long, String>> map = new HashMap<>();
        if (CollectionUtils.isEmpty(partitionHeatInfos)) {
            return map;
        }
        for (PartitionHeatInfo info : partitionHeatInfos) {
            String bound = VisualConvertUtil.generateBound(info);
            Long tableRows = info.getTableRows() == null ? 0L : info.getTableRows();
            String storageInstId = info.getStorageInstId();
            Pair<Long/**tableRows**/, String/**storageInstId**/> pair = new Pair<>(tableRows, storageInstId);
            map.put(bound, pair);
        }
        return map;
    }

    public List<PartitionHeatInfo> mergePartitionHeatInfo(List<PartitionHeatInfo> originPartitionHeatInfos,
                                                          List<Long> tableRowList,
                                                          Integer partitionsNum,
                                                          Integer storageInstIdNum,
                                                          Map<String, Integer> partitionsNumMap) {
        Integer maxMergeNum = getPartitionsHeatmapCollectionMaxMergeNum();
        if (CollectionUtils.isEmpty(originPartitionHeatInfos) ||
            partitionsNum <= maxMergeNum) {
            return originPartitionHeatInfos;
        }
        int minMergeRatio = partitionsNum / maxMergeNum;
        if (partitionsNum % maxMergeNum > 0) {
            minMergeRatio++;
        }
        Collections.sort(tableRowList);
        Long percentile50 = percentile(tableRowList, 50);
        Long percentile90 = percentile(tableRowList, 90);
        boolean isMergeByPercentile = true;
        if (tableRowList != null && tableRowList.get(0).equals(tableRowList.get(tableRowList.size() - 1))) {
            //如果所有分区数据量相同，就使用相邻分区合并算法
            isMergeByPercentile = false;
        }

        List<PartitionHeatInfo> mergedPartitionHeatInfos = new ArrayList<>();

        //单个逻辑表的所有逻辑分区
        List<PartitionHeatInfo> tablePartitionHeatInfos = new ArrayList<>();
        String lastKey = null;
        for (PartitionHeatInfo info : originPartitionHeatInfos) {
            String key = VisualConvertUtil.genTableKey(info.getSchemaName(), info.getLogicalTable());
            if (lastKey == null) {
                lastKey = key;
            }

            if (lastKey.equalsIgnoreCase(key)) {
                tablePartitionHeatInfos.add(info);
            } else {
                List<PartitionHeatInfo> merged = isMergeByPercentile ?
                    mergeTablePartitionHeatInfosByPercentile(tablePartitionHeatInfos, lastKey,
                        storageInstIdNum, minMergeRatio, percentile50, percentile90, partitionsNumMap, maxMergeNum) :
                    mergeTablePartitionHeatInfosByAdjacent(tablePartitionHeatInfos, lastKey,
                        storageInstIdNum, minMergeRatio, partitionsNumMap, maxMergeNum);

                lastKey = key;
                tablePartitionHeatInfos = new ArrayList<>();
                tablePartitionHeatInfos.add(info);

                if (merged.size() + mergedPartitionHeatInfos.size() > maxMergeNum) {
                    break;
                }
                mergedPartitionHeatInfos.addAll(merged);

            }
        }
        return mergedPartitionHeatInfos;
    }

    private Long percentile(List<Long> ascSortedDatas, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * ascSortedDatas.size());
        return ascSortedDatas.get(index - 1);
    }

    /**
     * 数据量大于等于90分位的分区不进行合并，
     * 数据量小于50分位的分区全部合并成一个分区，
     * 数据量大于等于50分位且小于90分位的按照压缩率合并
     */
    private List<PartitionHeatInfo> mergeTablePartitionHeatInfosByPercentile(List<PartitionHeatInfo> partitionHeatInfos,
                                                                             String key,
                                                                             Integer storageInstIdNum,
                                                                             int minMergeRatio,
                                                                             Long percentile50,
                                                                             Long percentile90,
                                                                             Map<String, Integer> partitionsNumMap,
                                                                             Integer maxMergeNum) {
        Integer partNum = partitionsNumMap.get(key);
        if (partNum == null || partNum <= storageInstIdNum) {
            return partitionHeatInfos;
        }

        int mergeRatio = (int) Math.ceil((double) (minMergeRatio * 10 - 1) / 4);
        mergeRatio = checkMaxPartitionsNumMergeRatio(mergeRatio, partNum, storageInstIdNum, maxMergeNum);

        Map<String, List<PartitionHeatInfo>> groupInfosMapG90 = new HashMap<>();
        Map<String, List<PartitionHeatInfo>> groupInfosMapG50L90 = new HashMap<>();
        Map<String, List<PartitionHeatInfo>> groupInfosMapL50 = new HashMap<>();

        for (PartitionHeatInfo info : partitionHeatInfos) {
            String storeIdTableKey = VisualConvertUtil.genStoreIdTableKey(info);
            if (info.getTableRows() >= percentile90) {
                storeIdTableKey = String.format("%s,%s", storeIdTableKey, info.getPartitionName());
                List<PartitionHeatInfo> groupInfos = groupInfosMapG90.get(storeIdTableKey);
                if (groupInfos == null) {
                    groupInfos = new ArrayList<>();
                    groupInfos.add(info);
                    groupInfosMapG90.put(storeIdTableKey, groupInfos);
                    continue;
                }
                groupInfos.add(info);
            }
            if (info.getTableRows() < percentile90 && info.getTableRows() >= percentile50) {
                List<PartitionHeatInfo> groupInfos = groupInfosMapG50L90.get(storeIdTableKey);
                if (groupInfos == null) {
                    groupInfos = new ArrayList<>();
                    groupInfos.add(info);
                    groupInfosMapG50L90.put(storeIdTableKey, groupInfos);
                    continue;
                }
                groupInfos.add(info);
            }
            if (info.getTableRows() < percentile50) {
                List<PartitionHeatInfo> groupInfos = groupInfosMapL50.get(storeIdTableKey);
                if (groupInfos == null) {
                    groupInfos = new ArrayList<>();
                    groupInfos.add(info);
                    groupInfosMapL50.put(storeIdTableKey, groupInfos);
                    continue;
                }
                groupInfos.add(info);
            }
        }

        List<PartitionHeatInfo> mergedPartitionHeatInfos = new ArrayList();
        mergedPartitionHeatInfos.addAll(groupMergedPartitionHeatInfos(groupInfosMapG90, 1));
        mergedPartitionHeatInfos.addAll(groupMergedPartitionHeatInfos(groupInfosMapG50L90, mergeRatio));
        mergedPartitionHeatInfos.addAll(groupMergedPartitionHeatInfos(groupInfosMapL50,
            ((int) Math.ceil((double) partNum / (double) storageInstIdNum) + 1)));

        return mergedPartitionHeatInfos;
    }

    /**
     * 单个逻辑表的分区数超大，讲该表进行最大化压缩
     */
    private int checkMaxPartitionsNumMergeRatio(int mergeRatio, Integer partNum, Integer storageInstIdNum,
                                                Integer maxMergeNum) {
        if (partNum >= maxMergeNum) {
            //一个DN上的所有逻辑分区都合并到同一个分区
            mergeRatio = ((int) Math.ceil((double) partNum / (double) storageInstIdNum)) + 1;
        }
        if (partNum < maxMergeNum && partNum >= (maxMergeNum / 2)) {
            //一个DN上的所有逻辑分区都合并到2个分区
            mergeRatio = (int) Math.ceil((double) partNum / (double) (storageInstIdNum * 2));
        }
        return mergeRatio;
    }

    /**
     * 按照目标压缩率，对相邻分区合并
     */
    private List<PartitionHeatInfo> mergeTablePartitionHeatInfosByAdjacent(List<PartitionHeatInfo> partitionHeatInfos,
                                                                           String key,
                                                                           Integer storageInstIdNum,
                                                                           int minMergeRatio,
                                                                           Map<String, Integer> partitionsNumMap,
                                                                           Integer maxMergeNum) {
        Integer partNum = partitionsNumMap.get(key);
        if (partNum == null || partNum <= storageInstIdNum) {
            return partitionHeatInfos;
        }
        int mergeRatio = minMergeRatio;
        mergeRatio = checkMaxPartitionsNumMergeRatio(mergeRatio, partNum, storageInstIdNum, maxMergeNum);

        Map<String, List<PartitionHeatInfo>> groupInfosMap = new HashMap<>();
        for (PartitionHeatInfo info : partitionHeatInfos) {
            String storeIdTableKey = VisualConvertUtil.genStoreIdTableKey(info);
            List<PartitionHeatInfo> groupInfos = groupInfosMap.get(storeIdTableKey);
            if (groupInfos == null) {
                groupInfos = new ArrayList<>();
                groupInfos.add(info);
                groupInfosMap.put(storeIdTableKey, groupInfos);
                continue;
            }
            groupInfos.add(info);
        }

        return groupMergedPartitionHeatInfos(groupInfosMap, mergeRatio);
    }

    private List<PartitionHeatInfo> groupMergedPartitionHeatInfos(Map<String, List<PartitionHeatInfo>> groupInfosMap,
                                                                  int mergeRatio) {
        List<PartitionHeatInfo> mergedPartitionHeatInfos = new ArrayList<>();
        int seq = 0;
        for (Map.Entry<String, List<PartitionHeatInfo>> entry : groupInfosMap.entrySet()) {
            List<PartitionHeatInfo> infos = entry.getValue();
            if (infos.size() == 1) {
                mergedPartitionHeatInfos.add(infos.get(0));
                continue;
            }
            long rowsRead = 0L;
            long rowsWritten = 0L;
            long rowsReadWritten = 0L;
            long tableRows = 0L;
            StringBuilder partNameSbr = new StringBuilder();
            boolean hasInitMeta = false;
            boolean hasMeetPartNameMax = false;
            PartitionHeatInfo partitionHeatInfo = new PartitionHeatInfo();
            for (int i = 0; i <= infos.size(); i++) {
                if ((i > 0 && (i + 1) % mergeRatio == 0) || i == infos.size()) {
                    partitionHeatInfo.setPartitionName(partNameSbr.toString());
                    partitionHeatInfo.setPartitionSeq(seq++);
                    partitionHeatInfo.setRowsRead(rowsRead);
                    partitionHeatInfo.setRowsWritten(rowsWritten);
                    partitionHeatInfo.setRowsReadWritten(rowsReadWritten);
                    partitionHeatInfo.setTableRows(tableRows);
                    mergedPartitionHeatInfos.add(partitionHeatInfo);

                    hasInitMeta = false;
                    hasMeetPartNameMax = false;
                    rowsRead = 0L;
                    rowsWritten = 0L;
                    rowsReadWritten = 0L;
                    tableRows = 0L;
                    partitionHeatInfo = new PartitionHeatInfo();
                    partNameSbr = new StringBuilder();
                }
                if (i == infos.size()) {
                    continue;
                }
                PartitionHeatInfo info = infos.get(i);
                if (!hasInitMeta) {
                    partitionHeatInfo.setStorageInstId(info.getStorageInstId());
                    partitionHeatInfo.setSchemaName(info.getSchemaName());
                    partitionHeatInfo.setLogicalTable(info.getLogicalTable());
                    hasInitMeta = true;
                }
                if (partNameSbr.length() < MAX_PARTITIONS_NAME_LENGTH) {
                    partNameSbr.append(info.getPartitionName());
                } else {
                    if (!hasMeetPartNameMax) {
                        partNameSbr.append("...");
                        hasMeetPartNameMax = true;
                    }
                }

                rowsRead = VisualConvertUtil.sumRows(rowsRead, info.getRowsRead());
                rowsWritten = VisualConvertUtil.sumRows(rowsWritten, info.getRowsWritten());
                rowsReadWritten = VisualConvertUtil.sumRows(rowsReadWritten, info.getRowsReadWritten());
                tableRows = VisualConvertUtil.sumRows(tableRows, info.getTableRows());
            }
        }
        return mergedPartitionHeatInfos;
    }

    /**
     * // 只采集 库tpcc_01的ordre1表和ordre2表，和库tpcc_02的user1表和user2表
     * set global PARTITIONS_HEATMAP_COLLECTION_ONLY = 'tpcc_01#ordre1&ordre2,tpcc_02#user1&user2';
     * 或者
     * // 只采集库 tpcc_01 和 tpcc_02
     * set global PARTITIONS_HEATMAP_COLLECTION_ONLY = 'tpcc_01,tpcc_02';
     * 或者
     * // 采集所有库的指定表ordre1 和 库tpcc_02的表user1和user2
     * set global PARTITIONS_HEATMAP_COLLECTION_ONLY = '#ordre1,tpcc_02#user1&user2';
     * 或者
     * // 全部采集
     * set global PARTITIONS_HEATMAP_COLLECTION_ONLY = '';
     */
    private HashMap<String, Set<String>> getPartitionsHeatmapCollectionOnly() {
        HashMap<String, Set<String>> schemaTablesMap = new HashMap<>();
        try {
            String val = MetaDbInstConfigManager.getInstance().getInstProperty(
                ConnectionProperties.PARTITIONS_HEATMAP_COLLECTION_ONLY);
            if (StringUtils.isEmpty(val) || StringUtils.isEmpty(val.trim())) {
                if (LAST_COLLECTION_ONLY_PARAM.equals("")) {
                    HAS_CHANGED_COLLECTION_ONLY_PARAM = false;
                } else {
                    HAS_CHANGED_COLLECTION_ONLY_PARAM = true;
                }
                LAST_COLLECTION_ONLY_PARAM = "";
                return schemaTablesMap;
            }

            if (LAST_COLLECTION_ONLY_PARAM.equalsIgnoreCase(val)) {
                HAS_CHANGED_COLLECTION_ONLY_PARAM = false;
            } else {
                HAS_CHANGED_COLLECTION_ONLY_PARAM = true;
            }
            LAST_COLLECTION_ONLY_PARAM = val;

            logger.warn(String.format("Partitions Heatmap Param PARTITIONS_HEATMAP_COLLECTION_ONLY=%s", val));
            String[] schemaTables = val.split(",");
            for (String schemaTable : schemaTables) {
                if (StringUtils.isEmpty(schemaTable) || StringUtils.isEmpty(schemaTable.trim())) {
                    continue;
                }
                String[] singleSchemaTables = schemaTable.split("#");
                String schema = "";
                String tables = "";
                if (singleSchemaTables.length == 1) {
                    schema = singleSchemaTables[0];
                    schemaTablesMap.computeIfAbsent(schema, k -> new TreeSet<>(String::compareToIgnoreCase));
                    continue;
                }
                if (singleSchemaTables.length == 2) {
                    schema = singleSchemaTables[0];
                    tables = singleSchemaTables[1];
                    String[] tableArrs = tables.split("&");
                    Set<String> tableSet = new TreeSet<>(String::compareToIgnoreCase);
                    ;
                    for (String table : tableArrs) {
                        if (!StringUtils.isEmpty(table)) {
                            tableSet.add(table);
                        }
                    }
                    if (schemaTablesMap.get(schema) == null) {
                        schemaTablesMap.put(schema, tableSet);
                    } else {
                        schemaTablesMap.get(schema).addAll(tableSet);
                    }
                }
            }
            return schemaTablesMap;
        } catch (Exception e) {
            logger.error("parse param:[PARTITIONS_HEATMAP_COLLECTION_ONLY=%s] error", e);
            return schemaTablesMap;
        }
    }

    private int getPartitionsHeatmapCollectionMaxScan() {
        int defaultVal = Integer.parseInt(ConnectionParams.PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN.getDefault());
        try {
            String val = MetaDbInstConfigManager.getInstance().getInstProperty(
                ConnectionProperties.PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN);
            if (StringUtils.isEmpty(val)) {
                return defaultVal;
            }
            return Integer.parseInt(val);
        } catch (Exception e) {
            logger.error(String.format("parse param:[PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN=%s] error", defaultVal), e);
            return defaultVal;
        }
    }

    private int getPartitionsHeatmapCollectionMaxMergeNum() {
        int defaultVal = Integer.parseInt(ConnectionParams.PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM.getDefault());
        try {
            String val = MetaDbInstConfigManager.getInstance().getInstProperty(
                ConnectionProperties.PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM);
            if (StringUtils.isEmpty(val)) {
                return defaultVal;
            }
            return Integer.parseInt(val);
        } catch (Exception e) {
            logger.error(
                String.format("parse param:[PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM=%s] error", defaultVal), e);
            return defaultVal;
        }
    }

    private int getPartitionsHeatmapCollectionMaxSingleLogicSchemaCount() {
        int defaultVal =
            Integer.parseInt(ConnectionParams.PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT.getDefault());
        try {
            String val = MetaDbInstConfigManager.getInstance().getInstProperty(
                ConnectionProperties.PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT);
            if (StringUtils.isEmpty(val)) {
                return defaultVal;
            }
            return Integer.parseInt(val);
        } catch (Exception e) {
            logger.error(
                String.format("parse param:[PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM=%s] error", defaultVal), e);
            return defaultVal;
        }
    }

}

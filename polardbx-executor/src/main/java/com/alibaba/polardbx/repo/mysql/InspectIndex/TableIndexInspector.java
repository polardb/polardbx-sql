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

package com.alibaba.polardbx.repo.mysql.InspectIndex;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.repo.mysql.InspectIndex.InspectIndexInfo.BadIndexKind.INEFFECTIVE_GSI;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class TableIndexInspector {
    final private String schema;
    final private String logicalTable;

    final private boolean isAutoPartitionTable;
    List<String> primaryColumns;

    private Map<String, InspectIndexInfo> globalIndexInspectInfo;

    private Map<String, InspectIndexInfo> localIndexInspectInfo;

    /**
     * 主表的拆分信息
     */
    private List<String> dbPartitionColumns;
    private String dbPartitionPolicy;
    private String dbPartitionCount;
    private List<String> tbPartitionColumns;
    private String tbPartitionPolicy;
    private String tbPartitionCount;

    static final Set<String> autoGenLsiPrefix = ImmutableSet.of(
        "auto_shard_key_"  //为拆分键自动生成的lsi
    );

    static final Set<String> autoPartitionLsiPrefix = ImmutableSet.of(
        "_local_"   //auto模式主键拆分表，为gsi自动生成的lsi
    );

    static final String autoPartitionIndexPrefix = "_local_";

    public String getSchema() {
        return schema;
    }

    public String getLogicalTable() {
        return logicalTable;
    }

    public Map<String, InspectIndexInfo> getGlobalIndexInspectInfo() {
        return globalIndexInspectInfo;
    }

    public Map<String, InspectIndexInfo> getLocalIndexInspectInfo() {
        return localIndexInspectInfo;
    }

    public TableIndexInspector(String schema, String logicalTable, Map<String, InspectIndexInfo> gsiInfosInTable,
                               Map<String, InspectIndexInfo> lsiInfosInTable) {
        this.schema = schema;
        this.logicalTable = logicalTable;
        this.globalIndexInspectInfo = gsiInfosInTable;
        this.localIndexInspectInfo = lsiInfosInTable;

        if (this.globalIndexInspectInfo == null) {
            this.globalIndexInspectInfo = new TreeMap<>(String::compareToIgnoreCase);
        }
        if (this.localIndexInspectInfo == null) {
            this.localIndexInspectInfo = new TreeMap<>(String::compareToIgnoreCase);
        }

        queryPrimaryTableRule(schema, logicalTable);

        this.isAutoPartitionTable = queryIsAutoPartitionTable(schema, logicalTable);

        if (localIndexInspectInfo.containsKey("PRIMARY")) {
            InspectIndexInfo primaryInfo = localIndexInspectInfo.get("PRIMARY");
            localIndexInspectInfo.remove("PRIMARY");
            primaryColumns = primaryInfo.indexColumns;
        } else {
            primaryColumns = new ArrayList<>();
        }

        if (this.isAutoPartitionTable) {
            for (String globalIndex : this.globalIndexInspectInfo.keySet()) {
                String logicalGsiName = this.globalIndexInspectInfo.get(globalIndex).getLogicalGsiName();
                if (this.localIndexInspectInfo.containsKey(logicalGsiName)) {
                    InspectIndexInfo lsiInfo = this.localIndexInspectInfo.get(logicalGsiName);
                    lsiInfo.indexName = autoPartitionIndexPrefix + logicalGsiName;
                    this.localIndexInspectInfo.remove(logicalGsiName);
                    this.localIndexInspectInfo.put(lsiInfo.indexName, lsiInfo);
                }
            }
        }

    }

    public static Map<String, TableIndexInspector> createTableInspectorsInSchema(String schema) {
        Map<String, TableIndexInspector> allInspectors = new TreeMap<>(String::compareToIgnoreCase);

        Set<String> tablesInSchema = queryTableNamesFromSchema(schema);

        List<InspectIndexInfo> allInfos = queryGsiInspectInfoRecordFromSchema(schema);
        Map<String, Map<String, InspectIndexInfo>> gsiInfoByTableNameGsiName =
            new TreeMap<>(String::compareToIgnoreCase);
        for (InspectIndexInfo info : allInfos) {
            String tableName = info.tableName;
            String gsiName = info.indexName;
            if (!gsiInfoByTableNameGsiName.containsKey(tableName)) {
                gsiInfoByTableNameGsiName.put(tableName, new TreeMap<>(String::compareToIgnoreCase));
            }
            gsiInfoByTableNameGsiName.get(tableName).put(gsiName, info);
        }

        Map<String, Map<String, InspectIndexInfo>> lsiInfoByTableNameGsiName =
            queryLocalIndexInspectInfoRecordFromSchema(schema);

        for (String logicalTable : tablesInSchema) {
            TableIndexInspector inspector =
                new TableIndexInspector(schema, logicalTable, gsiInfoByTableNameGsiName.get(logicalTable),
                    lsiInfoByTableNameGsiName.get(logicalTable));
            allInspectors.put(logicalTable, inspector);
        }

        return allInspectors;
    }

    public TableIndexInspector inspectUseFrequency() {
        final int REPORT_USE_FREQUENCY = 100;

        for (InspectIndexInfo info : globalIndexInspectInfo.values()) {
            if (info.useCount < REPORT_USE_FREQUENCY) {
                info.problem.put(InspectIndexInfo.BadIndexKind.LFU,
                    String.format("use count too low;", info.useCount));
            }
        }
        return this;
    }

    public TableIndexInspector inspectAccessTime() {
        final int REPORT_ACCESS_EXCEED_DAYS = 30;

        Date reportDate = Timestamp.valueOf(ZonedDateTime.now().minusDays(REPORT_ACCESS_EXCEED_DAYS).toLocalDateTime());

        for (InspectIndexInfo info : globalIndexInspectInfo.values()) {
            if (info.accessTime == null) {
                info.problem.put(InspectIndexInfo.BadIndexKind.LRU,
                    "index has never been accessed;");
            } else if (info.accessTime.before(reportDate)) {
                info.problem.put(InspectIndexInfo.BadIndexKind.LRU,
                    String.format("index has not been accessed for more than %s days;",
                        REPORT_ACCESS_EXCEED_DAYS));
            }
        }

        return this;
    }

    public TableIndexInspector inspectDiscrimination() {
        final Double minDiscrimination = 0.4;
        final Long minRowCountToReport = 10000L;

        for (InspectIndexInfo info : globalIndexInspectInfo.values()) {
            if (info.rowCount > minRowCountToReport && info.rowDiscrimination < minDiscrimination) {
                info.problem.put(InspectIndexInfo.BadIndexKind.LOW_DISCRIMINATION,
                    String.format("low discrimination on index column (%s);", String.join(", ", info.indexColumns)));
            }
        }
        return this;
    }

    /**
     * only support auto mode gsi (information_schema:table_detail doesn't support drds mode)
     */
    public TableIndexInspector inspectHotSpot() {
        this.globalIndexInspectInfo.forEach(((indexName, inspectIndexInfo) -> {
            this.inspectHotSpotForOneGsi(schema, logicalTable, inspectIndexInfo);
        }));

        return this;
    }

    /**
     * inspect hotspot:
     * 用分片的行数计算，当一个分片的行数占逻辑表的30%以上，且其z-score > 1.2时，被认为是热点分片
     */
    protected void inspectHotSpotForOneGsi(String schema, String table, InspectIndexInfo info) {
        final double zSroceReport = 1.5;
        final double ratioReport = 0.3;
        final double minTableRows = 100000;
        List<Pair<String, Long>> partitionRows = queryGsiPartitionRows(schema, table, info.getLogicalGsiName());
        if (partitionRows.size() <= 1) {
            return;
        }

        List<Double> rowSizeSequence =
            partitionRows.stream().map(Pair::getValue).map(Long::doubleValue).collect(Collectors.toList());
        double sum = rowSizeSequence.stream().mapToDouble(Double::doubleValue).sum();
        double mean = sum / partitionRows.size();
        double variance =
            rowSizeSequence.stream().map(n -> Math.pow(mean - n, 2)).mapToDouble(Double::doubleValue).sum();
        double sd = Math.sqrt(variance / partitionRows.size());

        if (sd == 0 || sum <= minTableRows) {
            return;
        }

        Set<String> partitions = new TreeSet<>(String::compareToIgnoreCase);

        for (int i = 0; i < rowSizeSequence.size(); i++) {
            double seq = rowSizeSequence.get(i);
            double z_score = (seq - mean) / sd;
            double ratio = seq / sum;

            if (z_score >= zSroceReport && ratio > ratioReport) {
                partitions.add(partitionRows.get(i).getKey()
                    + " "
                    + String.format("%.2f", ratio * 100)
                    + "%"
                );
            }
        }

        if (!partitions.isEmpty()) {
            info.problem.put(InspectIndexInfo.BadIndexKind.HOTSPOT, String.format(
                "found hotspot partition: %s;",
                String.join(", ", partitions)
            ));
        }
    }

    /**
     * 检测GSI表在key分区、list、hash分区上用秒级字段做索引列
     */
    protected void inspectLowEfficiencyGsiColumn() {
        for (Map.Entry<String, InspectIndexInfo> entry : globalIndexInspectInfo.entrySet()) {
            String gsiName = entry.getKey();
            InspectIndexInfo info = entry.getValue();
            if (!"key".equalsIgnoreCase(info.tbPartitionPolicy)) {
                continue;
            }
            String firstIndexColumn = info.indexColumns.get(0);
            if (OptimizerContext.getContext(schema) != null) {
                TableMeta primaryMeta =
                    OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(logicalTable);
                ColumnMeta column = primaryMeta.getColumn(firstIndexColumn);
                if (column.getDataType() instanceof DateTimeType
                    || column.getDataType() instanceof TimeType
                    || column.getDataType() instanceof TimestampType) {
                    info.problem.put(INEFFECTIVE_GSI,
                        String.format(
                            "ineffective gsi `%s`, time/timestamp/datetime type is ineffective in index column",
                            info.getLogicalGsiName()
                        ));
                    //防止其它后续GSI inspection流程继续检测该GSI
                    info.problem.put(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI, "");
                }
            }

        }
    }

    /**
     * 当
     */
    private boolean checkSamePartitionEffect(String dbPartitionPolicy1, int dbPartitionCount1,
                                             String tbPartitionPolicy1, int tbPartitionCount1,
                                             String dbPartitionPolicy2, int dbPartitionCount2,
                                             String tbPartitionPolicy2, int tbPartitionCount2) {
        return StringUtils.equalsIgnoreCase(dbPartitionPolicy1, dbPartitionPolicy2) && StringUtils.equalsIgnoreCase(
            tbPartitionPolicy1, tbPartitionPolicy2)
            || ("key".equalsIgnoreCase(tbPartitionPolicy1) && "hash".equalsIgnoreCase(tbPartitionPolicy2)
            && tbPartitionCount2 == 1)
            || ("key".equalsIgnoreCase(tbPartitionPolicy2) && "hash".equalsIgnoreCase(tbPartitionPolicy1)
            && tbPartitionCount1 == 1);
    }

    /**
     * 检测主表和GSI表拆分规则重复的情况
     */
    protected void inspectDuplicateGsiPartitionKeyWithPrimaryTable() {
        for (Map.Entry<String, InspectIndexInfo> entry : globalIndexInspectInfo.entrySet()) {
            String gsiName = entry.getKey();
            InspectIndexInfo info = entry.getValue();
            if (!checkSamePartitionEffect(dbPartitionPolicy, dbPartitionColumns.size(), tbPartitionPolicy,
                tbPartitionColumns.size(),
                info.dbPartitionPolicy, info.dbPartitionColumns.size(), info.tbPartitionPolicy,
                info.tbPartitionColumns.size())) {
                continue;
            }

            /**
             * 主表和index表都是key分区：当key分区的前导列相同时，若分区数一致，则认为该主表未进行热点分裂，gsi路由规则和主表相同
             * 说明gsi重复
             */
            if (StringUtils.equalsIgnoreCase("key", tbPartitionPolicy) || StringUtils.equalsIgnoreCase("key",
                info.tbPartitionPolicy)) {
                //两者都是key分区
                if (StringUtils.equalsIgnoreCase(tbPartitionCount, info.tbPartitionCount)
                    && StringUtils.equalsIgnoreCase(tbPartitionColumns.get(0), info.tbPartitionColumns.get(0))) {
                    info.problem.put(INEFFECTIVE_GSI,
                        String.format(
                            "ineffective gsi `%s` because it has the same rule as primary table",
                            info.getLogicalGsiName()
                        ));
                    //防止其它后续GSI inspection流程继续检测该GSI
                    info.problem.put(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI, "");
                }
            } else {
                if (tbPartitionColumns.equals(info.tbPartitionColumns) && dbPartitionColumns.equals(
                    info.dbPartitionColumns)) {
                    info.problem.put(INEFFECTIVE_GSI,
                        String.format(
                            "ineffective gsi `%s` because it has the same rule as primary table",
                            info.getLogicalGsiName()
                        ));
                    //防止其它后续GSI inspection流程继续检测该GSI
                    info.problem.put(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI, "");
                }
            }
        }
    }

    protected void inspectDuplicatedGsi() {
        //Map<indexColumns, gsiName>
        Map<String, List<String>> hashTableOnIndexColumns = new TreeMap<>(String::compareToIgnoreCase);

        //gather the same indexColumn gsi
        for (Map.Entry<String, InspectIndexInfo> entry : globalIndexInspectInfo.entrySet()) {
            if (entry.getValue().problem.containsKey(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI)) {
                continue;
            }
            String indexColumns = String.join(",", entry.getValue().indexColumns);
            if (!hashTableOnIndexColumns.containsKey(indexColumns)) {
                hashTableOnIndexColumns.put(indexColumns, new ArrayList<>());
            }
            hashTableOnIndexColumns.get(indexColumns).add(entry.getKey());
        }

        for (Map.Entry<String, List<String>> entry : hashTableOnIndexColumns.entrySet()) {
            List<String> indexes = entry.getValue();
            if (indexes.size() <= 1) {
                continue;
            }
            Set<String> coveringIndexes = new TreeSet<>(String::compareToIgnoreCase);
            for (String index : indexes) {
                InspectIndexInfo info = globalIndexInspectInfo.get(index);
                coveringIndexes.addAll(info.coveringColumns);
            }

            int reserveIdx = 0;
            boolean containUniqueIdx = indexes.stream().anyMatch(idx -> globalIndexInspectInfo.get(idx).unique);

            if (containUniqueIdx) {
                int uniqueIdx = -1;
                for (int i = 0; i < indexes.size(); i++) {
                    InspectIndexInfo info = globalIndexInspectInfo.get(indexes.get(i));
                    if (info.unique && info.coveringColumns.size() == coveringIndexes.size()) {
                        reserveIdx = i;
                    }
                    if (info.unique) {
                        uniqueIdx = i;
                    }
                }
                if (!globalIndexInspectInfo.get(indexes.get(reserveIdx)).unique) {
                    reserveIdx = uniqueIdx;
                }
            } else {
                for (int i = 0; i < indexes.size(); i++) {
                    InspectIndexInfo info = globalIndexInspectInfo.get(indexes.get(i));
                    if (info.coveringColumns.size() == coveringIndexes.size()) {
                        reserveIdx = i;
                    }
                }
            }

            for (int i = 0; i < indexes.size(); i++) {
                if (i == reserveIdx) {
                    InspectIndexInfo reserveInfo = globalIndexInspectInfo.get(indexes.get(i));
                    if (reserveInfo.coveringColumns.size() != coveringIndexes.size()) {
                        reserveInfo.problem.put(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI,
                            String.format(
                                "found duplicate global indexes: %s, covering column {%s} is recommended;",
                                String.join(", ",
                                    indexes.stream().map(idx -> globalIndexInspectInfo.get(idx).getLogicalGsiName())
                                        .collect(Collectors.toList())),
                                String.join(", ", coveringIndexes)));
                        reserveInfo.adviceCoveringColumns.addAll(coveringIndexes);
                    }
                } else {
                    InspectIndexInfo info = globalIndexInspectInfo.get(indexes.get(i));
                    info.problem.put(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI,
                        String.format(
                            "found duplicate global indexes: %s;",
                            String.join(", ",
                                indexes.stream().map(idx -> globalIndexInspectInfo.get(idx).getLogicalGsiName())
                                    .collect(Collectors.toList()))));
                }
            }
        }
    }

    protected void inspectSharedIndexColumnOnGsi() {
        //only consider the first index column
        Map<String, List<String>> hashTableOnSharedIndexColumns = new TreeMap<>(String::compareToIgnoreCase);

        //gather the gsi which share same indexColumn
        for (Map.Entry<String, InspectIndexInfo> entry : globalIndexInspectInfo.entrySet()) {
            String firstIndexColumn = entry.getValue().indexColumns.get(0);
            InspectIndexInfo info = entry.getValue();
            if (info.problem.containsKey(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI)) {
                continue;
            }
            if (!hashTableOnSharedIndexColumns.containsKey(firstIndexColumn)) {
                hashTableOnSharedIndexColumns.put(firstIndexColumn, new ArrayList<>());
            }
            hashTableOnSharedIndexColumns.get(firstIndexColumn).add(entry.getKey());
        }

        for (Map.Entry<String, List<String>> entry : hashTableOnSharedIndexColumns.entrySet()) {
            String sharedIndexColumn = entry.getKey();
            List<String> indexes = entry.getValue();
            if (indexes.size() <= 1) {
                continue;
            }
            Set<String> coveringIndexes = new TreeSet<>(String::compareToIgnoreCase);
            for (String index : indexes) {
                InspectIndexInfo info = globalIndexInspectInfo.get(index);
                if (info.problem.containsKey(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI)) {
                    continue;
                }
                coveringIndexes.addAll(info.coveringColumns);
                coveringIndexes.addAll(info.indexColumns);
            }

            int maxLengthOfIndexColumnIdx = -1;
            boolean findGoodIndex = false;
            boolean containUniqueIdx = indexes.stream().anyMatch(idx -> globalIndexInspectInfo.get(idx).unique);
            boolean containClusteredIdx = false;
            int goodIdx = 0;
            for (int j = 0; j < indexes.size(); j++) {
                InspectIndexInfo tempInfo = globalIndexInspectInfo.get(indexes.get(j));
                if (tempInfo.unique) {
                    goodIdx = j;
                    break;
                }
                if (tempInfo.clustered) {
                    goodIdx = j;
                    containClusteredIdx = true;
                }
            }
            for (int j = 0; j < indexes.size(); j++) {
                InspectIndexInfo tempInfo = globalIndexInspectInfo.get(indexes.get(j));
                if (tempInfo.problem.containsKey(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI)) {
                    continue;
                }
                if (containUniqueIdx && !tempInfo.unique || containClusteredIdx && !tempInfo.clustered) {
                    continue;
                }
                InspectIndexInfo goodInfo = globalIndexInspectInfo.get(indexes.get(goodIdx));
                Set<String> tempCoveringColumns = new TreeSet<>(String::compareToIgnoreCase);
                tempCoveringColumns.addAll(tempInfo.indexColumns);
                tempCoveringColumns.addAll(tempInfo.coveringColumns);
                if (tempCoveringColumns.equals(coveringIndexes)) {
                    findGoodIndex = true;
                }
                if (tempCoveringColumns.equals(coveringIndexes)
                    && tempInfo.indexColumns.size() > goodInfo.indexColumns.size()) {
                    goodIdx = j;
                }
                if (maxLengthOfIndexColumnIdx == -1 ||
                    tempInfo.indexColumns.size() > globalIndexInspectInfo.get(
                        indexes.get(maxLengthOfIndexColumnIdx)).indexColumns.size()) {
                    maxLengthOfIndexColumnIdx = j;
                }
            }

            if (!findGoodIndex) {
                goodIdx = maxLengthOfIndexColumnIdx;
            }

            for (int j = 0; j < indexes.size(); j++) {
                InspectIndexInfo tempInfo = globalIndexInspectInfo.get(indexes.get(j));
                if (tempInfo.problem.containsKey(InspectIndexInfo.BadIndexKind.DUPLICATE_GSI)) {
                    continue;
                }
                if (j == goodIdx) {
                    Set<String> tempCoveringColumns = new TreeSet<>(String::compareToIgnoreCase);
                    tempCoveringColumns.addAll(tempInfo.indexColumns);
                    tempCoveringColumns.addAll(tempInfo.coveringColumns);
                    if (!tempCoveringColumns.equals(coveringIndexes)) {
                        tempInfo.problem.put(InspectIndexInfo.BadIndexKind.DUP_GSI_INDEX_COLUMN,
                            String.format(
                                "index columns duplicate: %s, covering column {%s} is recommended;",
                                String.join(", ",
                                    indexes.stream().map(idx -> globalIndexInspectInfo.get(idx).getLogicalGsiName())
                                        .collect(Collectors.toList())),
                                String.join(", ",
                                    coveringIndexes.stream().filter(col -> !tempInfo.indexColumns.contains(col))
                                        .collect(
                                            Collectors.toList()))
                            ));
                        tempInfo.adviceIndexColumns.clear();
                        tempInfo.adviceCoveringColumns.clear();
                        tempInfo.adviceCoveringColumns.addAll(
                            coveringIndexes.stream().filter(col -> !tempInfo.indexColumns.contains(col))
                                .collect(
                                    Collectors.toList())
                        );
                    }
                } else {
                    //判断是否需要gen lsi
                    boolean needGenLsi = true;
                    for (int k = 0; k < indexes.size(); k++) {
                        if (k == j || k == goodIdx) {
                            continue;
                        }
                        InspectIndexInfo refInfo = globalIndexInspectInfo.get(indexes.get(k));
                        if (containInPrefix(tempInfo.indexColumns, refInfo.indexColumns)) {
                            needGenLsi = false;
                            break;
                        }
                    }
                    tempInfo.needGenLsi = needGenLsi;
                    tempInfo.problem.put(InspectIndexInfo.BadIndexKind.DUP_GSI_INDEX_COLUMN,
                        String.format("index columns duplicate: %s;",
                            String.join(", ",
                                indexes.stream().map(idx -> globalIndexInspectInfo.get(idx).getLogicalGsiName())
                                    .collect(Collectors.toList()))));
                }
            }
        }

    }

    protected void inspectDuplicateLsi() {
        Map<String, List<String>> hashTableOnIndexColumns = new TreeMap<>(String::compareToIgnoreCase);
        for (Map.Entry<String, InspectIndexInfo> entry : localIndexInspectInfo.entrySet()) {
            String indexColumns = String.join(",", entry.getValue().indexColumns);
            if (!hashTableOnIndexColumns.containsKey(indexColumns)) {
                hashTableOnIndexColumns.put(indexColumns, new ArrayList<>());
            }
            hashTableOnIndexColumns.get(indexColumns).add(entry.getKey());
        }

        for (Map.Entry<String, List<String>> entry : hashTableOnIndexColumns.entrySet()) {
            String indexColumns = entry.getKey();
            List<String> indexes = entry.getValue();

            if (StringUtil.isNullOrEmpty(indexColumns)) {
                for (String index : indexes) {
                    if (isAutoPartitionLsi(index)) {
                        continue;
                    }
                    InspectIndexInfo info = localIndexInspectInfo.get(index);
                    info.problem.put(InspectIndexInfo.BadIndexKind.DUPLICATE_LSI,
                        String.format(
                            "duplicate with primary key",
                            String.join(", ", indexes)
                        )
                    );
                }
                continue;
            }

            if (indexes.size() <= 1) {
                continue;
            }

            int reserveIdx = -1;
            //优先保留unique key
            for (int i = 0; i < indexes.size(); i++) {
                if (!isAutoGeneratedLsi(indexes.get(i))) {
                    reserveIdx = i;
                } else if (!isAutoGeneratedLsi(indexes.get(i)) && localIndexInspectInfo.get(indexes.get(i)).unique) {
                    reserveIdx = i;
                    break;
                }
            }

            if (reserveIdx == -1) {
                break;
            }

            for (int i = 0; i < indexes.size(); i++) {
                if (i == reserveIdx || isAutoPartitionLsi(indexes.get(i))) {
                    continue;
                }

                InspectIndexInfo info = localIndexInspectInfo.get(indexes.get(i));
                info.problem.put(InspectIndexInfo.BadIndexKind.DUPLICATE_LSI,
                    String.format(
                        "found duplicate local indexes: %s;",
                        String.join(", ", indexes)
                    )
                );
            }
        }
    }

    protected void inspectPrefixWithPrimaryKeyLsi() {
        for (Map.Entry<String, InspectIndexInfo> entry : localIndexInspectInfo.entrySet()) {
            String lsiName = entry.getKey();
            if (isAutoPartitionLsi(lsiName)) {
                continue;
            }
            InspectIndexInfo info = entry.getValue();
            if (containInPrefix(primaryColumns, info.indexColumns)) {
                info.problem.put(
                    InspectIndexInfo.BadIndexKind.PRIMERY_INDEX_AS_PREFIX_LSI,
                    String.format(
                        "index %s has the same effect as primary key",
                        info.indexName
                    )
                );
                info.indexColumns = removePrimaryKeyPrefix(primaryColumns, info.indexColumns);
            }
        }
    }

    protected void inspectSharedIndexColumnLsi() {
        Map<String, List<String>> hashTableOnSharedIndexColumns = new TreeMap<>(String::compareToIgnoreCase);
        for (Map.Entry<String, InspectIndexInfo> entry : localIndexInspectInfo.entrySet()) {
            InspectIndexInfo info = entry.getValue();
            if (info.problem.containsKey(InspectIndexInfo.BadIndexKind.DUPLICATE_LSI)) {
                continue;
            }
            if (isAutoPartitionLsi(info.indexName)) {
                continue;
            }
            String firstIndexColumn = entry.getValue().indexColumns.get(0);
            if (!hashTableOnSharedIndexColumns.containsKey(firstIndexColumn)) {
                hashTableOnSharedIndexColumns.put(firstIndexColumn, new ArrayList<>());
            }
            hashTableOnSharedIndexColumns.get(firstIndexColumn).add(entry.getKey());
        }

        for (Map.Entry<String, List<String>> entry : hashTableOnSharedIndexColumns.entrySet()) {
            String sharedIndexColumn = entry.getKey();
            List<String> indexes = entry.getValue();
            indexes.sort((idx1, idx2) -> {
                return localIndexInspectInfo.get(idx1).indexColumns.size() - localIndexInspectInfo.get(
                    idx2).indexColumns.size();
            });

            /**
             * (a) unique -> (a,b) unique
             * (a,b) unique cannot-> (a) unique
             * so, if (a) is unique, it shouldn't be dropped
             * */
            for (int j = 0; j < indexes.size(); j++) {
                for (int k = j + 1; k < indexes.size(); k++) {
                    InspectIndexInfo prefixInfo = localIndexInspectInfo.get(indexes.get(j));
                    InspectIndexInfo largeInfo = localIndexInspectInfo.get(indexes.get(k));
                    if (containInPrefix(prefixInfo.indexColumns, largeInfo.indexColumns)) {
                        if (!prefixInfo.unique && !isAutoPartitionLsi(prefixInfo.indexName)) {
                            prefixInfo.problem.put(
                                InspectIndexInfo.BadIndexKind.DUPLICATE_LSI,
                                String.format(
                                    "found duplicate local indexes: %s, %s",
                                    prefixInfo.indexName, largeInfo.indexName
                                )
                            );
                        } else if (prefixInfo.unique && !isAutoPartitionLsi(prefixInfo.indexName)) {
                            //前向找一个更小的unique lsi，替代当前待drop的lsi的unique作用
                            for (int m = 0; m < j; m++) {
                                InspectIndexInfo subPrefixInfo = localIndexInspectInfo.get(indexes.get(m));
                                if (subPrefixInfo.problem.isEmpty()
                                    && subPrefixInfo.unique
                                    && containInPrefix(subPrefixInfo.indexColumns, prefixInfo.indexColumns)) {
                                    prefixInfo.problem.put(
                                        InspectIndexInfo.BadIndexKind.DUPLICATE_LSI,
                                        String.format(
                                            "found duplicate local indexes: %s, %s",
                                            prefixInfo.indexName, largeInfo.indexName
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean isAutoGeneratedLsi(String lsiName) {
        if (lsiName == null) {
            return false;
        }
        for (String prefix : autoGenLsiPrefix) {
            if (lsiName.toLowerCase().startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean isAutoPartitionLsi(String lsiName) {
        if (lsiName == null) {
            return false;
        }
        for (String prefix : autoPartitionLsiPrefix) {
            if (lsiName.toLowerCase().startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public TableIndexInspector inspectGsi() {
        inspectLowEfficiencyGsiColumn();
        inspectDuplicateGsiPartitionKeyWithPrimaryTable();
        inspectDuplicatedGsi();
        inspectSharedIndexColumnOnGsi();
        checkNeedAddLsi();
        return this;
    }

    public TableIndexInspector inspectLsi() {
        inspectPrefixWithPrimaryKeyLsi();
        removePrimaryKeySuffixInLsi();
        inspectDuplicateLsi();
        inspectSharedIndexColumnLsi();
        return this;
    }

    public TableIndexInspector inspectIndex() {
        inspectLsi();
        inspectGsi();
        return this;
    }

    private void checkNeedAddLsi() {
        for (Map.Entry<String, InspectIndexInfo> entry : globalIndexInspectInfo.entrySet()) {
            String gsiName = entry.getKey();
            InspectIndexInfo gsiInfo = entry.getValue();
            boolean gsiUnique = gsiInfo.unique;
            if (gsiInfo.problem.containsKey(InspectIndexInfo.BadIndexKind.DUP_GSI_INDEX_COLUMN)
                || gsiInfo.problem.containsKey(INEFFECTIVE_GSI)) {
                /**
                 * 理应drop掉GSI，并建立对应的lsi，但还是先扫一遍现有的lsi
                 * 如果有，就不需要添加新的lsi
                 * */
                List<String> gsiColumn = removePrimaryKeyPrefix(primaryColumns, gsiInfo.indexColumns);
                gsiColumn = removePrimaryKeySuffix(primaryColumns, gsiColumn);
                for (Map.Entry<String, InspectIndexInfo> lsiEntry : localIndexInspectInfo.entrySet()) {
                    String lsiName = lsiEntry.getKey();
                    InspectIndexInfo lsiInfo = lsiEntry.getValue();
                    List<String> lsiColumn = removePrimaryKeyPrefix(primaryColumns, lsiInfo.indexColumns);
                    lsiColumn = removePrimaryKeySuffix(primaryColumns, lsiColumn);

                    if (containInPrefix(gsiColumn, lsiColumn) && (!gsiUnique || gsiUnique && lsiInfo.unique)) {
                        gsiInfo.needGenLsi = false;
                        break;
                    }
                }
            }
        }
    }

    public static Set<String> queryTableNamesFromSchema(String schema) {
        final String showTablesFromSql = "show tables from `" + schema + "`";
        List<Map<String, Object>> result =
            DdlHelper.getServerConfigManager().executeQuerySql(showTablesFromSql, schema, null);
        Set<String> tables = new TreeSet<>(String::compareToIgnoreCase);
        for (Map<String, Object> row : result) {
            for (String key : row.keySet()) {
                tables.add((String) row.get(key));
            }
        }
        return tables;
    }

    private static Map<String, Map<String, InspectIndexInfo>> queryLocalIndexInspectInfoRecordFromSchema(
        String schema) {
        final String querySql =
            "select * from information_schema.statistics where table_schema = '" + schema
                + "' order by SEQ_IN_INDEX ASC";

        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(querySql, schema, null);

        Map<String, Map<String, InspectIndexInfo>> infosByTableNameLsiName = new TreeMap<>(String::compareToIgnoreCase);

        boolean isNewPartition = DbInfoManager.getInstance().isNewPartitionDb(schema);

        for (Map<String, Object> row : result) {
            String tableName =
                row.get("TABLE_NAME") == null ? null : ((Slice) row.get("TABLE_NAME")).toStringUtf8();
            String indexName =
                row.get("INDEX_NAME") == null ? null : ((Slice) row.get("INDEX_NAME")).toStringUtf8();
            String idxCol =
                row.get("COLUMN_NAME") == null ? null : ((Slice) row.get("COLUMN_NAME")).toStringUtf8();
            Long nonUnique =
                row.get("NON_UNIQUE") == null ? 1L : (Long) row.get("NON_UNIQUE");

            Long indexLocation =
                row.get("INDEX_LOCATION") == null ? 0L : (Long) row.get("INDEX_LOCATION");
            if (indexLocation != 0) {
                continue;
            }
            InspectIndexInfo info = null;
            if (!infosByTableNameLsiName.containsKey(tableName)) {
                infosByTableNameLsiName.put(tableName, new TreeMap<>(String::compareToIgnoreCase));
            }
            if (!infosByTableNameLsiName.get(tableName).containsKey(indexName)) {
                info = new InspectIndexInfo();
                info.unique = nonUnique == 0L;
                infosByTableNameLsiName.get(tableName).put(indexName, info);
            } else {
                info = infosByTableNameLsiName.get(tableName).get(indexName);
            }
            info.tableName = tableName;
            info.indexName = indexName;
            info.indexColumns.add(idxCol);
            info.isPartitionTable = isNewPartition;
        }

        return infosByTableNameLsiName;
    }

    private static List<InspectIndexInfo> queryGsiInspectInfoRecordFromSchema(String schema) {
        final String querySql =
            "select * from information_schema.global_indexes where schema = '" + schema + "'";
        final String queryGsiPartitionRuleSql = "show rule from %s.%s";

        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(querySql, schema, null);

        boolean isNewPartition = DbInfoManager.getInstance().isNewPartitionDb(schema);

        List<InspectIndexInfo> infos = new ArrayList<>();
        for (Map<String, Object> row : result) {
            String tableName =
                row.get("TABLE") == null ? null : ((Slice) row.get("TABLE")).toStringUtf8();
            String indexName =
                row.get("KEY_NAME") == null ? null : ((Slice) row.get("KEY_NAME")).toStringUtf8();
            String idxColsStr =
                row.get("INDEX_NAMES") == null ? null : ((Slice) row.get("INDEX_NAMES")).toStringUtf8();
            String coveringColsStr =
                row.get("COVERING_NAMES") == null ? null : ((Slice) row.get("COVERING_NAMES")).toStringUtf8();
            List<String> indexColumns =
                (StringUtil.isNullOrEmpty(idxColsStr) || StringUtil.isNullOrEmpty(idxColsStr.replace(" ", ""))) ?
                    ImmutableList.of() : Arrays.asList(idxColsStr.replace(" ", "").split(","));
            List<String> coveringColumns = (StringUtil.isNullOrEmpty(coveringColsStr) || StringUtil.isNullOrEmpty(
                coveringColsStr.replace(" ", ""))) ?
                ImmutableList.of() : Arrays.asList(coveringColsStr.replace(" ", "").split(","));
            coveringColumns =
                coveringColumns.stream().filter(col -> !StringUtils.equalsIgnoreCase(col, "_drds_implicit_id_"))
                    .collect(
                        Collectors.toList());
            ;
            Integer nonUnique = (Integer) row.get("NON_UNIQUE");
            String useCount = row.get("USE_COUNT") == null ? null : ((Slice) row.get("USE_COUNT")).toStringUtf8();
            String accessTime =
                row.get("LAST_ACCESS_TIME") == null ? null : ((Slice) row.get("LAST_ACCESS_TIME")).toStringUtf8();
            String cardinality =
                row.get("CARDINALITY") == null ? null : ((Slice) row.get("CARDINALITY")).toStringUtf8();
            String rowCount = row.get("ROW_COUNT") == null ? null : ((Slice) row.get("ROW_COUNT")).toStringUtf8();
            InspectIndexInfo info = new InspectIndexInfo();
            info.tableName = tableName;
            info.indexName = indexName;
            info.indexColumns = indexColumns;
            info.coveringColumns = coveringColumns;
            info.isPartitionTable = isNewPartition;
            try {
                if (useCount != null) {
                    info.useCount = Long.valueOf(useCount);
                }
            } catch (Exception e) {
                info.useCount = 0L;
            }

            try {
                if (accessTime != null) {
                    info.accessTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(accessTime);
                }
            } catch (Exception e) {
                info.accessTime = null;
            }

            try {
                if (cardinality != null) {
                    info.rowCardinality = Long.valueOf(cardinality);
                } else {
                    info.rowCardinality = 0L;
                }
            } catch (Exception e) {
                info.rowCardinality = 0L;
            }

            try {
                if (rowCount != null) {
                    info.rowCount = Long.valueOf(rowCount);
                } else {
                    info.rowCount = -1L;
                }
            } catch (Exception e) {
                info.rowCount = -1L;
            }

            if (nonUnique == 0) {
                info.unique = true;
            } else {
                info.unique = false;
            }

            final String queryRuleSql =
                String.format(queryGsiPartitionRuleSql, quoteName(schema), quoteName(indexName));
            List<Map<String, Object>> rule =
                DdlHelper.getServerConfigManager().executeQuerySql(queryRuleSql, schema, null);
            for (Map<String, Object> rowResult : rule) {
                String dbPartitionColumnsStr = (String) rowResult.get("DB_PARTITION_KEY");
                if (!StringUtil.isNullOrEmpty(dbPartitionColumnsStr)) {
                    info.dbPartitionColumns = Arrays.asList(dbPartitionColumnsStr.split(","));
                } else {
                    info.dbPartitionColumns = new ArrayList<>();
                }

                info.dbPartitionPolicy = (String) rowResult.get("DB_PARTITION_POLICY");
                if (rowResult.get("DB_PARTITION_COUNT") instanceof Integer) {
                    info.dbPartitionCount = ((Integer) rowResult.get("DB_PARTITION_COUNT")).toString();
                } else {
                    info.dbPartitionCount = (String) rowResult.get("DB_PARTITION_COUNT");
                }

                String tbPartitionColumnsStr = (String) rowResult.get("TB_PARTITION_KEY");
                if (!StringUtil.isNullOrEmpty(tbPartitionColumnsStr)) {
                    info.tbPartitionColumns = Arrays.asList(tbPartitionColumnsStr.split(","));
                } else {
                    info.tbPartitionColumns = new ArrayList<>();
                }

                info.tbPartitionPolicy = (String) rowResult.get("TB_PARTITION_POLICY");
                if (rowResult.get("TB_PARTITION_COUNT") instanceof Integer) {
                    info.tbPartitionCount = ((Integer) rowResult.get("TB_PARTITION_COUNT")).toString();
                } else {
                    info.tbPartitionCount = (String) rowResult.get("TB_PARTITION_COUNT");
                }
            }

            info.partitionSql = queryPartitionByOnGsi(schema, indexName);

            if (info.rowCardinality != 0 && info.rowCount != 0) {
                info.rowDiscrimination = info.rowCardinality * 1.0 / info.rowCount;
            } else {
                info.rowDiscrimination = 0.0;
            }

            infos.add(info);

            info.primaryTableAllColumns.addAll(
                OptimizerContext.getContext(schema)
                    .getLatestSchemaManager()
                    .getTable(tableName)
                    .getAllColumns()
                    .stream()
                    .map(ColumnMeta::getName)
                    .collect(Collectors.toList())
            );
            if (info.primaryTableAllColumns.size() == info.coveringColumns.size() + info.indexColumns.size()) {
                info.clustered = true;
            } else {
                info.clustered = false;
            }
        }

        return infos;
    }

    private List<Pair<String, Long>> queryGsiPartitionRows(String schema, String table, String gsi) {
        final String querySql =
            "select * from information_schema.table_detail where table_schema='" + schema + "' and table_name='" + table
                + "' and index_name='" + gsi + "'";
        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(querySql, schema, null);

        List<Pair<String, Long>> partitionRows = new ArrayList<>();
        for (Map<String, Object> row : result) {
            String partitionName = ((Slice) row.get("PARTITION_NAME")).toStringUtf8();
            Long rows = ((UInt64) row.get("TABLE_ROWS")).longValue();
            partitionRows.add(Pair.of(partitionName, rows));
        }
        return partitionRows;
    }

    private static String queryPartitionByOnGsi(String schema, String gsiName) {
        final String querySql =
            "show create table " + quoteName(gsiName);
        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(querySql, schema, null);

        String createTableSql = null;
        for (Map<String, Object> row : result) {
            createTableSql = (String) row.get("CREATE TABLE");
        }

        boolean isNewPartition = DbInfoManager.getInstance().isNewPartitionDb(schema);
        final MySqlCreateTableStatement createTableStatement =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(createTableSql).get(0);
        if (!isNewPartition && !StringUtil.isNullOrEmpty(createTableSql)) {
            List<String> partitionDefs = new ArrayList<>();
            if (createTableStatement.getDbPartitionBy() != null) {
                partitionDefs.add("dbpartition by " + createTableStatement.getDbPartitionBy().toString());
            }
            if (createTableStatement.getDbPartitions() != null) {
                partitionDefs.add("dbpartitions " + createTableStatement.getDbPartitions().toString());
            }
            if (createTableStatement.getTablePartitionBy() != null) {
                partitionDefs.add("tbpartition by " + createTableStatement.getTablePartitionBy().toString());
            }
            if (createTableStatement.getTablePartitions() != null) {
                partitionDefs.add("tbpartitions " + createTableStatement.getTbpartitions().toString());
            }
            return String.join(" ", partitionDefs);
        } else if (isNewPartition && !StringUtil.isNullOrEmpty(createTableSql)) {
            return "PARTITION BY " + createTableStatement.getPartitioning().toString();
        } else {
            return null;
        }
    }

    private static String quoteName(String name) {
        if (!StringUtil.isNullOrEmpty(name) && !name.contains("`")) {
            return "`" + name + "`";
        } else {
            return name;
        }
    }

    private void queryPrimaryTableRule(String schema, String tableName) {
        final String queryGsiPartitionRuleSql = "show rule from %s.%s";
        final String queryRuleSql = String.format(queryGsiPartitionRuleSql, quoteName(schema), quoteName(tableName));
        List<Map<String, Object>> rule =
            DdlHelper.getServerConfigManager().executeQuerySql(queryRuleSql, schema, null);
        for (Map<String, Object> rowResult : rule) {
            String dbPartitionColumnsStr = (String) rowResult.get("DB_PARTITION_KEY");
            if (!StringUtil.isNullOrEmpty(dbPartitionColumnsStr)) {
                this.dbPartitionColumns = Arrays.asList(dbPartitionColumnsStr.split(","));
            } else {
                this.dbPartitionColumns = new ArrayList<>();
            }

            this.dbPartitionPolicy = (String) rowResult.get("DB_PARTITION_POLICY");
            if (rowResult.get("DB_PARTITION_COUNT") instanceof Integer) {
                this.dbPartitionCount = ((Integer) rowResult.get("DB_PARTITION_COUNT")).toString();
            } else {
                this.dbPartitionCount = (String) rowResult.get("DB_PARTITION_COUNT");
            }

            String tbPartitionColumnsStr = (String) rowResult.get("TB_PARTITION_KEY");
            if (!StringUtil.isNullOrEmpty(tbPartitionColumnsStr)) {
                this.tbPartitionColumns = Arrays.asList(tbPartitionColumnsStr.split(","));
            } else {
                this.tbPartitionColumns = new ArrayList<>();
            }

            this.tbPartitionPolicy = (String) rowResult.get("TB_PARTITION_POLICY");
            if (rowResult.get("TB_PARTITION_COUNT") instanceof Integer) {
                this.tbPartitionCount = ((Integer) rowResult.get("TB_PARTITION_COUNT")).toString();
            } else {
                this.tbPartitionCount = (String) rowResult.get("TB_PARTITION_COUNT");
            }
        }
    }

    private boolean queryIsAutoPartitionTable(String schema, String table) {
        OptimizerContext context = OptimizerContext.getContext(schema);
        if (context == null) {
            return false;
        }
        TableMeta meta = context.getLatestSchemaManager().getTable(table);
        if (meta == null) {
            return false;
        }
        return meta.isAutoPartition();
    }

    private boolean indexColumnContainPrimaryKeyInSuffix(List<String> primaryKey, List<String> indexColumn) {
        if (indexColumn.size() < primaryKey.size() || primaryKey.isEmpty()) {
            return false;
        }
        int primaryKeySize = primaryKey.size();
        int indexSize = indexColumn.size();
        List<String> suffixColumn = indexColumn.subList(indexSize - primaryKeySize, indexSize);
        return primaryKey.equals(suffixColumn);
    }

    private boolean containInPrefix(List<String> prefix, List<String> columns) {
        if (columns.size() < prefix.size() || prefix.isEmpty()) {
            return false;
        }
        int prefixSize = prefix.size();
        return prefix.equals(columns.subList(0, prefixSize));
    }

    private List<String> removePrimaryKeySuffix(List<String> primaryKey, List<String> indexColumn) {
        if (primaryKey.isEmpty() || !indexColumnContainPrimaryKeyInSuffix(primaryKey, indexColumn)) {
            return indexColumn;
        }
        int primaryKeySize = primaryKey.size();
        int indexSize = indexColumn.size();
        if (indexSize < primaryKeySize) {
            return indexColumn;
        }
        return indexColumn.subList(0, indexSize - primaryKeySize);
    }

    private List<String> removePrimaryKeyPrefix(List<String> primaryKey, List<String> indexColumn) {
        if (primaryKey.isEmpty() || !containInPrefix(primaryKey, indexColumn)) {
            return indexColumn;
        }
        int primaryKeySize = primaryKey.size();
        int indexSize = indexColumn.size();
        if (indexSize < primaryKeySize) {
            return indexColumn;
        }
        return indexColumn.subList(primaryKeySize, indexSize);
    }

    protected void removePrimaryKeySuffixInLsi() {
        for (Map.Entry<String, InspectIndexInfo> entry : localIndexInspectInfo.entrySet()) {
            InspectIndexInfo info = entry.getValue();
            info.indexColumns = removePrimaryKeySuffix(this.primaryColumns, info.indexColumns);
        }
    }

}

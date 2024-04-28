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

package com.alibaba.polardbx.executor.balancer.serial;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.balancer.splitpartition.SplitPoint;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataDistInfo {
    public static class PgDataDistInfo {
        public String getPgName() {
            return pgName;
        }

        public void setPgName(String pgName) {
            this.pgName = pgName;
        }

        public String getTgName() {
            return tgName;
        }

        public void setTgName(String tgName) {
            this.tgName = tgName;
        }

        public Long getTableRows() {
            return tableRows;
        }

        public void setTableRows(Long tableRows) {
            this.tableRows = tableRows;
        }

        public Long getDiskSize() {
            return diskSize;
        }

        public void setDiskSize(Long diskSize) {
            this.diskSize = diskSize;
        }

        public Integer getOriginalPlace() {
            return originalPlace;
        }

        public void setOriginalPlace(Integer originalPlace) {
            this.originalPlace = originalPlace;
        }

        public Integer getTargetPlace() {
            return targetPlace;
        }

        public void setTargetPlace(Integer targetPlace) {
            this.targetPlace = targetPlace;
        }

        String pgName;

        String tgName;

        Long tableRows;

        Long diskSize;

        Integer originalPlace;

        Integer targetPlace;

        public String getFromPgName() {
            return fromPgName;
        }

        public void setFromPgName(String fromPgName) {
            this.fromPgName = fromPgName;
        }

        String fromPgName;

        @JSONCreator
        public PgDataDistInfo(String tgName, String pgName, Long tableRows, Long diskSize, Integer originalPlace,
                              Integer targetPlace, String fromPgName) {
            this.tgName = tgName;
            this.pgName = pgName;
            this.tableRows = tableRows;
            this.diskSize = diskSize;
            this.originalPlace = originalPlace;
            this.targetPlace = targetPlace;
            this.fromPgName = fromPgName;
        }

    }

    public static class TgDataDistInfo {
        public String getTgName() {
            return tgName;
        }

        public void setTgName(String tgName) {
            this.tgName = tgName;
        }

        public List<PgDataDistInfo> getPgDataDistInfos() {
            return pgDataDistInfos;
        }

        public void setPgDataDistInfos(
            List<PgDataDistInfo> pgDataDistInfos) {
            this.pgDataDistInfos = pgDataDistInfos;
        }

        String tgName;

        List<PgDataDistInfo> pgDataDistInfos;

        public List<String> getTableNames() {
            return tableNames;
        }

        public void setTableNames(List<String> tableNames) {
            this.tableNames = tableNames;
        }

        List<String> tableNames;

        @JSONCreator
        public TgDataDistInfo(String tgName, List<PgDataDistInfo> pgDataDistInfos, List<String> tableNames) {
            this.tgName = tgName;
            this.pgDataDistInfos = pgDataDistInfos;
            this.tableNames = tableNames;
        }
    }

    public static class DnInfo {
        public String getDnName() {
            return dnName;
        }

        public void setDnName(String dnName) {
            this.dnName = dnName;
        }

        public String getGroupKey() {
            return groupKey;
        }

        public void setGroupKey(String groupKey) {
            this.groupKey = groupKey;
        }

        String groupKey;
        String dnName;

        @JSONCreator
        public DnInfo(String dnName, String groupKey) {
            this.dnName = dnName;
            this.groupKey = groupKey;
        }
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<DnInfo> getDnInfos() {
        return dnInfos;
    }

    public void setDnInfos(List<DnInfo> dnInfos) {
        this.dnInfos = dnInfos;
    }

    public List<TgDataDistInfo> getTgDataDistInfos() {
        return tgDataDistInfos;
    }

    public void setTgDataDistInfos(
        List<TgDataDistInfo> tgDataDistInfos) {
        this.tgDataDistInfos = tgDataDistInfos;
    }

    String schema;

    List<DnInfo> dnInfos;

    List<TgDataDistInfo> tgDataDistInfos;

    Map<String, Integer> groupKeyIndexMap;

    @JSONCreator
    public DataDistInfo(String schema, List<DnInfo> dnInfos, List<TgDataDistInfo> tgDataDistInfos) {
        this.schema = schema;
        this.dnInfos = dnInfos;
        this.tgDataDistInfos = tgDataDistInfos;
    }

    public static DataDistInfo fromSchemaAndInstMap(String schema, Map<Integer, String> storageInstMap,
                                                    Map<Integer, String> groupKeyMap) {
        return new DataDistInfo(schema, fromGroupDetailMap(storageInstMap, groupKeyMap), new ArrayList<>());
    }

    public void appendTgDataDist(String tgName, List<PartitionGroupStat> partitionGroupStats, int[] originalPlace,
                                 int[] targetPlace) {
        TgDataDistInfo tgDataDistInfo = fromPartitionGroupStat(tgName, partitionGroupStats, originalPlace, targetPlace);
        this.tgDataDistInfos.add(tgDataDistInfo);
    }

    public void appendTgDataDist(String tgName, List<PartitionGroupStat> partitionGroupStats, int[] originalPlace,
                                 Map<String, Pair<List<SplitPoint>, List<Double>>> splitPointList) {
        TgDataDistInfo tgDataDistInfo =
            fromPartitionGroupStat(tgName, partitionGroupStats, originalPlace, splitPointList);
        this.tgDataDistInfos.add(tgDataDistInfo);

    }

    public void applyStatistic(Map<String, Map<String, PartitionGroupStat>> tgStats) {
        for (int i = 0; i < tgDataDistInfos.size(); i++) {
            TgDataDistInfo tgDataDistInfo = tgDataDistInfos.get(i);
            Map<String, PartitionGroupStat> partitionGroupStats = tgStats.get(tgDataDistInfo.tgName);
            tgDataDistInfo.pgDataDistInfos.stream().filter(o -> o.getTargetPlace() != -1).forEach(o -> {
                o.diskSize = partitionGroupStats.get(o.pgName).getTotalDiskSize();
                o.tableRows = partitionGroupStats.get(o.pgName).getDataRows();
                o.targetPlace =
                    getGroupIndex(partitionGroupStats.get(o.pgName).getFirstPartition().getLocation().getGroupKey());
            });
        }
    }

    public Integer getGroupIndex(String groupKey) {
        if (groupKeyIndexMap == null || groupKeyIndexMap.isEmpty()) {
            groupKeyIndexMap = new HashMap<>();
            for (int i = 0; i < dnInfos.size(); i++) {
                groupKeyIndexMap.put(dnInfos.get(i).groupKey, i);
            }
        }
        return groupKeyIndexMap.get(groupKey);
    }

    public static List<DnInfo> fromGroupDetailMap(Map<Integer, String> groupDetailMap,
                                                  Map<Integer, String> groupKeyMap) {
        return groupDetailMap.keySet().stream().map(o -> new DnInfo(groupDetailMap.get(o), groupKeyMap.get(o)))
            .collect(Collectors.toList());
    }

    public TgDataDistInfo fromPartitionGroupStat(String tgName, List<PartitionGroupStat> partitionGroupStats,
                                                 int[] originalPlace, int[] targetPlace) {
        List<PgDataDistInfo> pgDataDistInfo = IntStream.range(0, partitionGroupStats.size()).boxed()
            .map(i -> new PgDataDistInfo(partitionGroupStats.get(i).getTgName(),
                partitionGroupStats.get(i).pg.partition_name,
                partitionGroupStats.get(i).getDataRows(),
                partitionGroupStats.get(i).getTotalDiskSize(),
                originalPlace[i],
                targetPlace[i],
                "")).collect(Collectors.toList());
        List<String> tableNames = new ArrayList<>();
        if (!partitionGroupStats.isEmpty() || !partitionGroupStats.get(0).partitions.isEmpty()) {
            List<String> collectTableNames =
                partitionGroupStats.get(0).getFirstPartition().getTableGroupConfig().getTables();
            tableNames.addAll(collectTableNames);
        }
        return new TgDataDistInfo(tgName, pgDataDistInfo, tableNames);
    }

    public TgDataDistInfo fromPartitionGroupStat(String tgName, List<PartitionGroupStat> partitionGroupStats,
                                                 int[] originalPlace,
                                                 Map<String, Pair<List<SplitPoint>, List<Double>>> splitPointMap) {
        List<PgDataDistInfo> pgDataDistInfo = new ArrayList<>();
        for (int i = 0; i < partitionGroupStats.size(); i++) {
            String partitionName = partitionGroupStats.get(i).pg.partition_name;
            if (!splitPointMap.containsKey(partitionName)) {
                pgDataDistInfo.add(new PgDataDistInfo(partitionGroupStats.get(i).getTgName(),
                    partitionGroupStats.get(i).pg.getPartition_name(),
                    partitionGroupStats.get(i).getDataRows(),
                    partitionGroupStats.get(i).getTotalDiskSize(),
                    originalPlace[i],
                    originalPlace[i],
                    ""));
            } else {
                List<SplitPoint> splitPoints = splitPointMap.get(partitionName).getKey();
                List<Double> splitSizes = splitPointMap.get(partitionName).getValue();
                double scale =
                    partitionGroupStats.get(i).getTotalDiskSize() / (double) partitionGroupStats.get(i).getDataRows();
                int j = 0;
                Double scaleDiskSize = splitSizes.get(j) * scale;
                pgDataDistInfo.add(new PgDataDistInfo(partitionGroupStats.get(i).getTgName(),
                    splitPoints.get(j).leftPartition,
                    splitSizes.get(j).longValue(),
                    scaleDiskSize.longValue(),
                    originalPlace[i],
                    (originalPlace[i] + j) % dnInfos.size(),
                    partitionName));
                for (j = 1; j < splitPoints.size(); j++) {
                    Double diskSize = (splitSizes.get(j) * scale);
                    pgDataDistInfo.add(new PgDataDistInfo(partitionGroupStats.get(i).getTgName(),
                        splitPoints.get(j - 1).rightPartition,
                        splitSizes.get(j).longValue() - splitSizes.get(j - 1).longValue(),
                        diskSize.longValue(),
                        originalPlace[i],
                        (originalPlace[i] + j) % dnInfos.size(),
                        partitionName));
                }
//                Double resRowNum = partitionGroupStats.get(i).getDataRows() - splitSizes.stream().mapToDouble(o->0).sum();
                pgDataDistInfo.add(new PgDataDistInfo(partitionGroupStats.get(i).getTgName(),
                    partitionGroupStats.get(i).pg.getPartition_name(),
                    partitionGroupStats.get(i).getDataRows(),
                    partitionGroupStats.get(i).getTotalDiskSize(),
                    originalPlace[i],
                    -1,
                    ""));
            }
        }
        List<String> tableNames =
            partitionGroupStats.get(0).getFirstPartition().getTableGroupConfig().getTables();
        return new TgDataDistInfo(tgName, pgDataDistInfo, tableNames);
    }

}

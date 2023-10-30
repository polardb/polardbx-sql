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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.partition.common.PartInfoSessionVars;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionByNormalizationParams;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.google.common.collect.Lists;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;
import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;

/**
 * The complete partition definition of one logical table
 *
 * @author chenghui.lch
 */
public class PartitionInfo {

    /**
     * the id of logical table in table_partitions of metadb
     * <pre>
     *     Notice: hash & equal operation is not allowed to use this properties
     * </pre>
     */
    protected Long tableId;

    /**
     * the schema of logical table
     */
    protected String tableSchema;

    /**
     * the name of logical table
     */
    protected String tableName;

    /**
     * the pattern of table name
     */
    protected String tableNamePattern;

    /**
     * if enable random pattern of table name
     */
    protected boolean randomTableNamePatternEnabled = true;

    /**
     * the status of logical partitioned table
     */
    protected Integer status;

    /**
     * the flag for the subpartition template
     */
    protected Integer spTemplateFlag;

    /**
     * the table group id of table
     */
    protected Long tableGroupId;

    /**
     * the meta version of partition Info
     */
    protected Long metaVersion;

    /**
     * Auto split/merge/balance partitions
     */
    protected Integer autoFlag;

    /**
     * default table group
     */
    protected Boolean nonDefaultSingleTableGroup;

    /**
     * The general partition flags
     */
    protected Long partFlags;

    /**
     * the locality Info
     */
    protected String locality;
    /**
     * the table type of partitioned table, may be primary table or gsi table
     * tableType=0: partition table
     * tableType=1: gsi table
     * tableType=2: single table
     * tableType=3: broadcast table
     * tableType=4: gsi single table
     * tableType=5: gsi broadcast table
     */
    protected PartitionTableType tableType;

    /**
     * the complete definition of partitions
     */
    protected PartitionByDefinition partitionBy;

    /**
     * The hashCode of partInfo
     */
    protected volatile Integer partInfoHashCode = null;

    /**
     * The session variables during creating partitioned table
     */
    protected PartInfoSessionVars sessionVars;

    /**
     * use the search the partSpec by phyDb and phyTbl
     */
    protected PartSpecSearcher partSpecSearcher;

    public PartitionInfo() {
    }

    public boolean containSubPartitions() {
        return getPartitionBy() != null && getPartitionBy().getSubPartitionBy() != null;
    }

    public boolean isSinglePartition() {
        int cnt = this.partitionBy.getPhysicalPartitions().size();
        if (cnt > 1) {
            return false;
        }
        return true;
    }

    public boolean isBroadcastTable() {
        return this.tableType == PartitionTableType.BROADCAST_TABLE;
    }

    public boolean isSingleTable() {
        return this.tableType == PartitionTableType.SINGLE_TABLE;
    }

    public boolean isPartitionedTable() {
        return this.tableType == PartitionTableType.PARTITION_TABLE;
    }

    public boolean isGsiSingleOrSingleTable() {
        return this.tableType == PartitionTableType.SINGLE_TABLE
            || this.tableType == PartitionTableType.GSI_SINGLE_TABLE;
    }

    public boolean isGsiBroadcastOrBroadcast() {
        return this.tableType == PartitionTableType.BROADCAST_TABLE
            || this.tableType == PartitionTableType.GSI_BROADCAST_TABLE;
    }

    public boolean isPartitionedGsiTable() {
        return this.tableType == PartitionTableType.GSI_TABLE;
    }

    public boolean isPartitionedTableOrGsiTable() {
        return this.tableType == PartitionTableType.PARTITION_TABLE || this.tableType == PartitionTableType.GSI_TABLE;
    }

    public String defaultDbIndex() {
        return this.getPartitionBy().getPartitions().get(0).getLocation().getGroupKey();
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrefixTableName() {
        return isRandomTableNamePatternEnabled() ? tableNamePattern : tableName;
    }

    public String getTableNamePattern() {
        return tableNamePattern;
    }

    public void setTableNamePattern(String tableNamePattern) {
        this.tableNamePattern = tableNamePattern;
        this.randomTableNamePatternEnabled = true;
    }

    public boolean isRandomTableNamePatternEnabled() {
        return this.randomTableNamePatternEnabled;
    }

    public void setRandomTableNamePatternEnabled(boolean randomTableNamePatternEnabled) {
        this.randomTableNamePatternEnabled = randomTableNamePatternEnabled;
    }

    public PartitionByDefinition getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(PartitionByDefinition partitionBy) {
        this.partitionBy = partitionBy;
    }

    public Integer getSpTemplateFlag() {
        return spTemplateFlag;
    }

    public void setSpTemplateFlag(Integer spTemplateFlag) {
        this.spTemplateFlag = spTemplateFlag;
    }

    public Long getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(Long tableGroupId) {
        this.tableGroupId = tableGroupId;
    }

    public Long getMetaVersion() {
        return metaVersion;
    }

    public void setMetaVersion(Long metaVersion) {
        this.metaVersion = metaVersion;
    }

    public Integer getStatus() {
        return status;
    }

    public PartitionTableType getTableType() {
        return tableType;
    }

    public void setTableType(PartitionTableType tableType) {
        this.tableType = tableType;
    }

//    public PartitionTableType getGsiTableType() {
//        return gsiTableType;
//    }
//
//    public void setGsiTableType(PartitionTableType gsiTableType) {
//        this.gsiTableType = gsiTableType;
//    }

    public void setBuildNoneDefaultSingleGroup(Boolean flag) {
        this.nonDefaultSingleTableGroup = flag;
    }

    public Boolean getBuildNoneDefaultSingleGroup() {
        return (this.nonDefaultSingleTableGroup == null) ? false : this.nonDefaultSingleTableGroup;
    }

    public boolean enableAutoSplit() {
        return autoFlag.equals(TablePartitionRecord.PARTITION_AUTO_BALANCE_ENABLE_ALL);
    }

    public Integer getAutoFlag() {
        return autoFlag;
    }

    public void setAutoFlag(Integer autoFlag) {
        this.autoFlag = autoFlag;
    }

    public Long getPartFlags() {
        return partFlags;
    }

    public void setPartFlags(Long partFlags) {
        this.partFlags = partFlags;
    }

    public Map<String, List<PhysicalPartitionInfo>> getPhysicalPartitionTopology(List<String> partitionNames,
                                                                                 boolean throwException) {
        return getPhysicalPartitionTopology(partitionNames, throwException, false);
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    /**
     * <pre>
     *  key:    groupKey
     *  value:  the list of physical partition info
     *      if partitionNames is empty,
     *          return all the partitions topology
     *      else
     *          return the dedicated partitions(given by the input parameter) topology
     * </pre>
     */
    public Map<String, List<PhysicalPartitionInfo>> getPhysicalPartitionTopology(List<String> partitionNames,
                                                                                 boolean throwException,
                                                                                 boolean ignoreInvalid) {

        /**
         * Key: phy group
         * val: physical partition list
         */
        Map<String, List<PhysicalPartitionInfo>> topology = new HashMap<>();

        boolean needFilterParts = !GeneralUtil.isEmpty(partitionNames);
        Set<String> targetPartNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        if (partitionNames != null) {
            targetPartNameSet.addAll(partitionNames);
        }

        for (PartitionSpec partitionSpec : partitionBy.getPartitions()) {
            final String name = partitionSpec.getName();
            boolean isLogicalPart = partitionSpec.isLogical();
            if (isLogicalPart) {
                List<PartitionSpec> subParts = partitionSpec.getSubPartitions();
                for (int i = 0; i < subParts.size(); i++) {
                    PartitionSpec subpartSpec = subParts.get(i);
                    final String subPartName = subpartSpec.getName();
                    boolean findTargetPart = true;
                    if (needFilterParts) {
                        if (!targetPartNameSet.contains(subPartName)) {
                            findTargetPart = false;
                        }
                    }

                    if (!findTargetPart) {
                        continue;
                    }

                    final PartitionLocation location = subpartSpec.getLocation();
                    if (location != null && (!throwException || location.isValidLocation())) {
                        PhysicalPartitionInfo phyPartInfo = new PhysicalPartitionInfo();
                        phyPartInfo.setPartId(subpartSpec.getId());
                        phyPartInfo.setGroupKey(subpartSpec.getLocation().getGroupKey());
                        phyPartInfo.setPhyTable(subpartSpec.getLocation().getPhyTableName());
                        phyPartInfo.setPartName(subpartSpec.getName());
                        phyPartInfo.setParentPartName(name);
                        phyPartInfo.setPartLevel(subpartSpec.getPartLevel());
                        phyPartInfo.setPartBitSetIdx(subpartSpec.getPosition().intValue());

                        if (topology.containsKey(location.getGroupKey())) {
                            topology.get(location.getGroupKey()).add(phyPartInfo);
                        } else {
                            List<PhysicalPartitionInfo> phyPartInfos = new ArrayList<>();
                            phyPartInfos.add(phyPartInfo);
                            topology.put(location.getGroupKey(), phyPartInfos);
                        }
                    } else {
                        if (ignoreInvalid && location != null && !location.isValidLocation()) {
                            continue;
                        }
                        throw GeneralUtil
                            .nestedException(new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                "Found invalid location info of " + this.getTableName()));
                    }

                }
            } else {
                boolean findTargetPart = true;
                if (needFilterParts) {
                    if (!targetPartNameSet.contains(name)) {
                        findTargetPart = false;
                    }
                }

                if (!findTargetPart) {
                    continue;
                }

                final PartitionLocation location = partitionSpec.getLocation();
                if (location != null && (!throwException || location.isValidLocation())) {
                    PhysicalPartitionInfo phyPartInfo = new PhysicalPartitionInfo();
                    phyPartInfo.setPartId(partitionSpec.getId());
                    phyPartInfo.setGroupKey(partitionSpec.getLocation().getGroupKey());
                    phyPartInfo.setPhyTable(partitionSpec.getLocation().getPhyTableName());
                    phyPartInfo.setPartName(partitionSpec.getName());
                    phyPartInfo.setParentPartName(null);
                    phyPartInfo.setPartLevel(partitionSpec.getPartLevel());
                    phyPartInfo.setPartBitSetIdx(partitionSpec.getPosition().intValue());

                    if (topology.containsKey(location.getGroupKey())) {
                        topology.get(location.getGroupKey()).add(phyPartInfo);
                    } else {
                        List<PhysicalPartitionInfo> phyPartInfos = new ArrayList<>();
                        phyPartInfos.add(phyPartInfo);
                        topology.put(location.getGroupKey(), phyPartInfos);
                    }
                } else {
                    if (ignoreInvalid && location != null && !location.isValidLocation()) {
                        continue;
                    }
                    throw GeneralUtil
                        .nestedException(new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            "Found invalid location info of " + this.getTableName()));
                }
            }
        }
        return topology;
    }

    public Map<String, List<PhysicalPartitionInfo>> getPhysicalPartitionTopologyIgnore(List<String> partitionNames) {
        return getPhysicalPartitionTopology(partitionNames, true, true);
    }

    public Map<String, List<PhysicalPartitionInfo>> getPhysicalPartitionTopology(List<String> partitionNames) {
        return getPhysicalPartitionTopology(partitionNames, true, false);
    }

    public Map<String, Set<String>> getTopology() {
        return getTopology(false);
    }

    public Map<String, Set<String>> getTopology(boolean ignoreInvalid) {
        Map<String, List<PhysicalPartitionInfo>> physicalPartitionTopology =
            ignoreInvalid ? getPhysicalPartitionTopologyIgnore(null)
                : getPhysicalPartitionTopology(null);
        Map<String, Set<String>> topology = new HashMap<>();
        for (Map.Entry<String, List<PhysicalPartitionInfo>> entry : physicalPartitionTopology.entrySet()) {
            topology.put(entry.getKey(),
                entry.getValue().stream().map(PhysicalPartitionInfo::getPhyTable).collect(Collectors.toSet()));
        }
        return topology;
    }

    public boolean isGsi() {
        return tableType == PartitionTableType.GSI_TABLE;
    }

    public boolean isGsiOrPartitionedTable() {
        return tableType == PartitionTableType.GSI_TABLE || tableType == PartitionTableType.PARTITION_TABLE;
    }

    public String showCreateTablePartitionDefInfo(boolean showHashByRange) {
        return showCreateTablePartitionDefInfo(showHashByRange, "");
    }

    public String showCreateTablePartitionDefInfo(boolean showHashByRange, String textIntentBase) {
        String partByDef = "";
        if (tableType == PartitionTableType.SINGLE_TABLE) {
            partByDef += "SINGLE";
        } else if (tableType == PartitionTableType.BROADCAST_TABLE) {
            partByDef += "BROADCAST";
        } else {
            PartitionByNormalizationParams params = new PartitionByNormalizationParams();
            params.setShowHashByRange(showHashByRange);
            params.setTextIntentBase(textIntentBase);
            partByDef = partitionBy.normalizePartByDefForShowCreateTable(params);
            if (this.getAutoFlag() != 0) {
                partByDef += "\nAUTO_SPLIT=ON";
            }
        }
        return partByDef;
    }

    public Integer getAllPartLevelCount() {
        if (this.tableType != PartitionTableType.PARTITION_TABLE
            && this.tableType != PartitionTableType.GSI_TABLE) {
            return 0;
        } else {
            return this.partitionBy.getAllPartLevelCount();
        }
    }

    public List<String> getPartitionColumns() {
        return getPartitionColumnsNotReorder();
    }

    public List<String> getActualPartitionColumnsNotReorder() {
        final Set<String> actShardSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final List<String> actShardCols = new ArrayList<>();
        if (partitionBy != null) {
            List<List<String>> allLevelActualPartCols = getAllLevelActualPartCols();
            List<String> firstLevelActPartCol = allLevelActualPartCols.get(0);
            for (String colName : firstLevelActPartCol) {
                if (!actShardSet.contains(colName)) {
                    actShardSet.add(colName);
                    actShardCols.add(colName);
                }
            }
            if (partitionBy.getSubPartitionBy() != null) {
                List<String> secondLevelActPartCol = allLevelActualPartCols.get(0);
                for (String colName : secondLevelActPartCol) {
                    if (!actShardSet.contains(colName)) {
                        actShardSet.add(colName);
                        actShardCols.add(colName);
                    }
                }
            }
        }
        return actShardCols;
    }

    public List<String> getPartitionColumnsNotReorder() {
        final Set<String> shardSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final List<String> shardCols = new ArrayList<>();
        if (partitionBy != null) {
            for (String colName : partitionBy.getPartitionColumnNameList()) {
                if (!shardSet.contains(colName)) {
                    shardSet.add(colName);
                    shardCols.add(colName);
                }
            }

            if (partitionBy.getSubPartitionBy() != null) {
                for (String colName : partitionBy.getSubPartitionBy().getPartitionColumnNameList()) {
                    if (!shardSet.contains(colName)) {
                        shardSet.add(colName);
                        shardCols.add(colName);
                    }
                }
            }
        }
        return shardCols;
    }

    public Map<Integer, List<String>> getPartLevelToPartColsMapping() {
        final Set<String> shardSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Map<Integer, List<String>> shardColsByLevel = new HashMap<>();

        if (partitionBy != null) {
            List<String> shardCols = new ArrayList<>();
            for (String colName : partitionBy.getPartitionColumnNameList()) {
                if (!shardSet.contains(colName)) {
                    shardSet.add(colName);
                    shardCols.add(colName);
                }
            }
            shardColsByLevel.put(PARTITION_LEVEL_PARTITION, shardCols);

            if (partitionBy.getSubPartitionBy() != null) {
                shardSet.clear();
                List<String> subShardCols = new ArrayList<>();
                for (String colName : partitionBy.getSubPartitionBy().getPartitionColumnNameList()) {
                    if (!shardSet.contains(colName)) {
                        shardSet.add(colName);
                        subShardCols.add(colName);
                    }
                }
                shardColsByLevel.put(PARTITION_LEVEL_SUBPARTITION, subShardCols);
            }
        }

        return shardColsByLevel;
    }

    public List<String> getPartitionColumnsNotDeduplication() {
        final List<String> shardCols = new ArrayList<>();
        if (partitionBy != null) {
            shardCols.addAll(partitionBy.getPartitionColumnNameList());

            if (partitionBy.getSubPartitionBy() != null) {
                shardCols.addAll(partitionBy.getSubPartitionBy().getPartitionColumnNameList());
            }
        }
        return shardCols;
    }

    public String getPartitionNameByPhyLocation(String phyGrp, String phyTable) {
        PartitionSpec partSpec = partSpecSearcher.getPartSpec(phyGrp, phyTable);
        if (partSpec == null) {
            return null;
        }
        return partSpec.getName();
//        String targetPartName = null;
//        List<PartitionSpec> allPartSpecs = this.partitionBy.getPartitions();
//        if (this.partitionBy.getSubPartitionBy() != null) {
//            List<PartitionSpec> allSubPartSpecs = new ArrayList<>();
//            for (int i = 0; i < allPartSpecs.size(); i++) {
//                allSubPartSpecs.addAll(allPartSpecs.get(i).getSubPartitions());
//            }
//            for (int i = 0; i < allSubPartSpecs.size(); i++) {
//                PartitionSpec p = allSubPartSpecs.get(i);
//                String name = p.getName();
//                String grpKey = p.getLocation().getGroupKey();
//                String phyTbl = p.getLocation().getPhyTableName();
//                if (grpKey.equalsIgnoreCase(phyGrp) && phyTbl.equalsIgnoreCase(phyTable)) {
//                    targetPartName = name;
//                    break;
//                }
//            }
//        } else {
//            for (int i = 0; i < allPartSpecs.size(); i++) {
//                PartitionSpec p = allPartSpecs.get(i);
//                String name = p.getName();
//                String grpKey = p.getLocation().getGroupKey();
//                String phyTbl = p.getLocation().getPhyTableName();
//                if (grpKey.equalsIgnoreCase(phyGrp) && phyTbl.equalsIgnoreCase(phyTable)) {
//                    targetPartName = name;
//                    break;
//                }
//            }
//        }
//        return targetPartName;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public PartitionInfo copy() {
        PartitionInfo newPartInfo = new PartitionInfo();
        newPartInfo.setTableSchema(this.tableSchema);
        newPartInfo.setTableName(this.tableName);
        newPartInfo.setTableNamePattern(this.tableNamePattern);
        newPartInfo.setRandomTableNamePatternEnabled(this.randomTableNamePatternEnabled);
        newPartInfo.setStatus(this.status);
        newPartInfo.setSpTemplateFlag(this.spTemplateFlag);
        newPartInfo.setTableGroupId(this.tableGroupId);
        newPartInfo.setMetaVersion(this.metaVersion);
        newPartInfo.setAutoFlag(this.autoFlag);
        newPartInfo.setPartFlags(this.partFlags);
        newPartInfo.setTableType(this.tableType);
        newPartInfo.setSessionVars(this.sessionVars.copy());
        newPartInfo.setBuildNoneDefaultSingleGroup(this.getBuildNoneDefaultSingleGroup());

        if (this.partitionBy != null) {
            newPartInfo.setPartitionBy(this.partitionBy.copy());
        }

        if (this.locality != null) {
            newPartInfo.setLocality(this.locality);
        }

        if (this.partSpecSearcher != null) {
            newPartInfo.setPartSpecSearcher(
                PartSpecSearcher.buildPartSpecSearcher(newPartInfo.getTableType(), newPartInfo.getPartitionBy()));
        }

        partitionBy.refreshPhysicalPartitionsCache();
        return newPartInfo;
    }

    public int getAllPhysicalPartitionCount() {
        List<PartitionSpec> psList = this.partitionBy.getPhysicalPartitions();
        return psList.size();
    }

//    public List<String> getActualPartitionColumns() {
//        PartitionTableType tableType = this.tableType;
//        List<String> actualPartCols = new ArrayList<>();
//        if (tableType == PartitionTableType.BROADCAST_TABLE || tableType == PartitionTableType.SINGLE_TABLE) {
//            return actualPartCols;
//        }
//        return this.getPartitionBy().getActualPartitionColumns();
//    }

    public List<List<ColumnMeta>> getAllLevelActualPartColMetas() {
        List<Integer> allLevelPartColCnts = getAllLevelActualPartColCounts();
        List<List<ColumnMeta>> result = new ArrayList<>();
        PartitionByDefinition partByDef = this.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();
        if (partByDef != null) {
            int actPartColCnt = allLevelPartColCnts.get(0);
            List<ColumnMeta> allPartColMetas = partByDef.getPartitionFieldList();
            List<ColumnMeta> actPartColMetas = new ArrayList<>();
            for (int i = 0; i < actPartColCnt; i++) {
                actPartColMetas.add(allPartColMetas.get(i));
            }
            result.add(actPartColMetas);
        }
        if (subPartByDef != null) {
            int actSubPartColCnt = allLevelPartColCnts.get(1);
            List<ColumnMeta> allSubPartColMetas = subPartByDef.getPartitionFieldList();
            List<ColumnMeta> actSubPartColMetas = new ArrayList<>();
            for (int i = 0; i < actSubPartColCnt; i++) {
                actSubPartColMetas.add(allSubPartColMetas.get(i));
            }
            result.add(actSubPartColMetas);
        } else {
            result.add(Lists.newArrayList());
        }
        return result;
    }

    public List<List<String>> getAllLevelActualPartCols() {
        PartitionTableType tableType = this.tableType;
        List<List<String>> result = new ArrayList<>();
        if (tableType != PartitionTableType.PARTITION_TABLE && tableType != PartitionTableType.GSI_TABLE) {
            result.add(Lists.newArrayList());
            result.add(Lists.newArrayList());
            return result;
        }
        result = this.partitionBy.getAllLevelActualPartCols();
        return result;
    }

    public List<List<String>> getAllLevelFullPartCols() {
        PartitionTableType tableType = this.tableType;
        List<List<String>> result = new ArrayList<>();
        if (tableType != PartitionTableType.PARTITION_TABLE && tableType != PartitionTableType.GSI_TABLE) {
            result.add(Lists.newArrayList());
            result.add(Lists.newArrayList());
            return result;
        }
        result = this.partitionBy.getAllLevelFullPartCols();
        return result;
    }

    /**
     * Get the actual partition columns of all part levels
     * and covert them into a list (allowed duplicated columns)
     * <p>
     * Import Notice:
     * the order of  partition columns of list to be return
     * MUST BE the same as the order of definition in create tbl/create global index ddl
     */
    public List<String> getAllLevelActualPartColsAsList() {
        List<List<String>> allLevelActualPartCols = getAllLevelActualPartCols();
        List<String> results = new ArrayList<>();
        for (int i = 0; i < allLevelActualPartCols.size(); i++) {
            if (!allLevelActualPartCols.get(i).isEmpty()) {
                results.addAll(allLevelActualPartCols.get(i));
            }
        }
        return results;
    }

    /**
     * Get the actual partition columns of all part levels and merge them into a list without duplicated columns
     */
    public List<String> getAllLevelActualPartColsAsNoDuplicatedList() {
        List<List<String>> allLevelActualPartCols = getAllLevelActualPartCols();
        TreeSet<String> colSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<String> allLevelActualPartColList = new ArrayList<>();
        for (int i = 0; i < allLevelActualPartCols.size(); i++) {
            for (int j = 0; j < allLevelActualPartCols.get(i).size(); j++) {
                String col = allLevelActualPartCols.get(i).get(j);
                if (!colSet.contains(col)) {
                    colSet.add(col);
                    allLevelActualPartColList.add(col);
                } else {
                    continue;
                }
            }
        }
        return allLevelActualPartColList;
    }

    public Map<Integer, List<String>> getAllLevelActualPartColsAsNoDuplicatedListByLevel() {
        Map<Integer, List<String>> allLevelActualPartColsByLevel = new HashMap<>();

        List<List<String>> allLevelActualPartColsList = getAllLevelActualPartCols();

        if (GeneralUtil.isNotEmpty(allLevelActualPartColsList)) {
            List<String> allLevelActualPartColsNoDup = removeDuplicates(allLevelActualPartColsList.get(0));
            allLevelActualPartColsByLevel.put(PARTITION_LEVEL_PARTITION, allLevelActualPartColsNoDup);

            if (allLevelActualPartColsList.size() > 1 && GeneralUtil.isNotEmpty(allLevelActualPartColsList.get(1))) {
                List<String> allLevelActualSubPartColsNoDup = removeDuplicates(allLevelActualPartColsList.get(1));
                allLevelActualPartColsByLevel.put(PARTITION_LEVEL_SUBPARTITION, allLevelActualSubPartColsNoDup);
            }
        }

        return allLevelActualPartColsByLevel;
    }

    private List<String> removeDuplicates(List<String> columnNames) {
        TreeSet<String> colNamesSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<String> columnNamesWithoutDuplicate = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i);
            if (!colNamesSet.contains(colName)) {
                colNamesSet.add(colName);
                columnNamesWithoutDuplicate.add(colName);
            }
        }
        return columnNamesWithoutDuplicate;
    }

    public List<Integer> getAllLevelActualPartColCounts() {
        List<List<String>> allLevelPartCols = getAllLevelActualPartCols();
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < allLevelPartCols.size(); i++) {
            result.add(allLevelPartCols.get(i).size());
        }
        return result;
    }

    public List<Integer> getAllLevelFullPartColCounts() {
        List<List<String>> allLevelPartCols = getAllLevelFullPartCols();
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < allLevelPartCols.size(); i++) {
            result.add(allLevelPartCols.get(i).size());
        }
        return result;
    }

    public List<PartitionStrategy> getAllLevelPartitionStrategies() {
        List<PartitionStrategy> result = new ArrayList<>();
        result.add(this.getPartitionBy().getStrategy());
        if (this.getPartitionBy().getSubPartitionBy() != null) {
            result.add(this.getPartitionBy().getSubPartitionBy().getStrategy());
        }
        return result;

    }

    public List<Integer> getAllLevelPartitionCount() {
        List<Integer> result = new ArrayList<>();
        result.add(this.partitionBy.getPartitions().size());
        if (this.partitionBy.getSubPartitionBy() != null) {
            result.add(this.getAllPhysicalPartitionCount());
        }
        return result;
    }

    @Override
    public int hashCode() {
        if (partInfoHashCode == null) {
            synchronized (this) {
                if (partInfoHashCode == null) {
                    partInfoHashCode = hashCodeInner();
                }
            }
        }
        return partInfoHashCode;
    }

    private int hashCodeInner() {
        int hashCodeVal = tableSchema.toLowerCase().hashCode();
        hashCodeVal ^= tableName.toLowerCase().hashCode();
        hashCodeVal ^= status.intValue();
        hashCodeVal ^= spTemplateFlag.intValue();
        hashCodeVal ^= tableGroupId.intValue();
        hashCodeVal ^= metaVersion.intValue();
        hashCodeVal ^= partitionBy.hashCode();
        return hashCodeVal;
    }

    @Override
    public boolean equals(Object obj) {
        //return equals(obj, -1);
        return equals(obj, PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);
    }

//    /**
//     * check equals by specifying prefix partition column prefixPartColCnt，
//     * if prefixPartColCnt <= 0, then use all partition columns
//     */
//    public boolean equals(Object obj, int prefixPartColCnt) {
//        if (this == obj) {
//            return true;
//        }
//
//        if (obj == null) {
//            return false;
//        }
//
//        if (obj.getClass() != this.getClass()) {
//            return false;
//        }
//
//        PartitionInfo objPartInfo = (PartitionInfo) obj;
//        if(!StringUtils.equals(locality, (objPartInfo.getLocality()))){
//            return false;
//        }
//        if (objPartInfo.getTableType() != this.tableType) {
//            if (objPartInfo.isGsiBroadcastOrBroadcast() && this.isGsiBroadcastOrBroadcast()) {
//                return true;
//            }
//            if (objPartInfo.isGsiSingleOrSingleTable() && this.isGsiSingleOrSingleTable()) {
//                return true;
//            }
//            if (!(objPartInfo.isGsiOrPartitionedTable() && this.isGsiOrPartitionedTable())) {
//                return false;
//            }
//        } else {
//            if (this.tableType == PartitionTableType.SINGLE_TABLE
//                || this.tableType == PartitionTableType.BROADCAST_TABLE) {
//                return true;
//            }
//        }
//        if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
//            return getPartitionBy().equals(objPartInfo.getPartitionBy());
//        } else {
//            return getPartitionBy().equals(objPartInfo.getPartitionBy(), prefixPartColCnt);
//        }
//    }

    /**
     * check equals by specifying all level prefix partition column allLevelPrefixPartColCnts，
     * if allLevelPrefixPartColCnts={-1，1} , then use all partition columns
     */
    public boolean equals(Object obj, List<Integer> allLevelPrefixPartColCnts) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        PartitionInfo objPartInfo = (PartitionInfo) obj;
        if (!StringUtils.equals(locality, (objPartInfo.getLocality()))) {
            return false;
        }
        if (objPartInfo.getTableType() != this.tableType) {
            if (objPartInfo.isGsiBroadcastOrBroadcast() && this.isGsiBroadcastOrBroadcast()) {
                return true;
            }
            if (objPartInfo.isGsiSingleOrSingleTable() && this.isGsiSingleOrSingleTable()) {
                return true;
            }
            if (!(objPartInfo.isGsiOrPartitionedTable() && this.isGsiOrPartitionedTable())) {
                return false;
            }
        } else {
            if (this.tableType == PartitionTableType.SINGLE_TABLE
                || this.tableType == PartitionTableType.BROADCAST_TABLE) {
                return true;
            }
        }

        if (this.spTemplateFlag.intValue() != objPartInfo.getSpTemplateFlag().intValue()) {
            return false;
        }

        if (allLevelPrefixPartColCnts.equals(PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST)) {
            return getPartitionBy().equals(objPartInfo.getPartitionBy());
        } else {
            return getPartitionBy().equals(objPartInfo.getPartitionBy(), allLevelPrefixPartColCnts);
        }
    }

    public PartInfoSessionVars getSessionVars() {
        return sessionVars;
    }

    public void setSessionVars(PartInfoSessionVars sessionVars) {
        this.sessionVars = sessionVars;
    }

    public String getDigest(Long tableVersion) {
        StringBuilder sb = new StringBuilder();
        sb.append("partitionInfo digest:[");
        sb.append("tableVersion:");
        sb.append(tableVersion);
        sb.append(",");
        sb.append(showCreateTablePartitionDefInfo(true));
        for (PartitionSpec partitionSpec : partitionBy.getPartitions()) {
            sb.append(partitionSpec.getDigest());
            sb.append("\n");
        }
        sb.append("]");

        return sb.toString();
    }

    public boolean canPerformPruning(List<String> actualUsedCols, PartKeyLevel level) {
        if (level == PartKeyLevel.PARTITION_KEY && (this.tableType == PartitionTableType.PARTITION_TABLE
            || this.tableType == PartitionTableType.GSI_TABLE)) {
            return this.partitionBy.canPerformPruning(actualUsedCols);
        } else {
            return false;
        }
    }

    public PartSpecSearcher getPartSpecSearcher() {
        return partSpecSearcher;
    }

    public void setPartSpecSearcher(PartSpecSearcher partSpecSearcher) {
        this.partSpecSearcher = partSpecSearcher;
    }

    public void initPartSpecSearcher() {

        /**
         * Sort and adjust the partitions by their bound value definitions
         */
        sortAndAdjustPartitionPositionsByBoundValue();

        /**
         * Prepare the mapping from phy_db.phy_tb to partSpec
         */
        this.partSpecSearcher = PartSpecSearcher.buildPartSpecSearcher(getTableType(), getPartitionBy());

        /**
         * Prepare the orderNum in one phyDb for each partition
         */
        PartitionInfoBuilder.prepareOrderNumForPartitions(getTableType(), getPartitionBy().getPhysicalPartitions());

    }

    /**
     * Sort and adjust the partitions by their bound value definitions
     */
    private void sortAndAdjustPartitionPositionsByBoundValue() {

        /**
         * Reset the position of each part and subpart for both partitions and subpartitions
         */
        PartitionByDefinition.adjustPartitionPositionsByBoundVal(this.tableType, this.partitionBy);

        /**
         * Reset the parentPartPosition for both partitions and subpartitions
         */
        PartitionInfo.adjustParentPartPosiForBothPartsAndSubParts(this.partitionBy);

        /**
         * Reset the phyPartPosition for both partitions and subpartitions
         */
        PartitionInfo.adjustPhyPartPosiForBothPartsAndSubParts(this.partitionBy);
    }

    private static void adjustPhyPartPosiForBothPartsAndSubParts(PartitionByDefinition partitionBy) {
        List<PartitionSpec> newSpecList = partitionBy.getPartitions();
        long allPhyPartPosiCounter = 0L;
        for (int i = 0; i < newSpecList.size(); i++) {
            PartitionSpec spec = newSpecList.get(i);
            if (spec.isLogical()) {
                for (int j = 0; j < spec.getSubPartitions().size(); j++) {
                    PartitionSpec spSpec = spec.getSubPartitions().get(j);
                    ++allPhyPartPosiCounter;
                    spSpec.setPhyPartPosition(allPhyPartPosiCounter);
                }
                spec.setPhyPartPosition(0L);
            } else {
                ++allPhyPartPosiCounter;
                spec.setPhyPartPosition(allPhyPartPosiCounter);
            }
        }
        PartitionByDefinition subPartByDef = partitionBy.getSubPartitionBy();
        if (subPartByDef != null) {
            List<PartitionSpec> newSubPartitions = subPartByDef.getPartitions();
            for (int i = 0; i < newSubPartitions.size(); i++) {
                PartitionSpec spec = newSubPartitions.get(i);
                spec.setPhyPartPosition(0L);
            }
        }
    }

    private static void adjustParentPartPosiForBothPartsAndSubParts(PartitionByDefinition partitionBy) {
        List<PartitionSpec> newSpecs = partitionBy.getPartitions();
        for (int i = 0; i < newSpecs.size(); i++) {
            PartitionSpec spec = newSpecs.get(i);
            spec.setParentPartPosi(0L);
            if (spec.isLogical()) {
                for (int j = 0; j < spec.getSubPartitions().size(); j++) {
                    PartitionSpec spSpec = spec.getSubPartitions().get(j);
                    spSpec.setParentPartPosi(spec.getPosition());
                }
            }
        }
        PartitionByDefinition subPartBy = partitionBy.getSubPartitionBy();
        if (subPartBy != null) {
            List<PartitionSpec> newSubPartitions = subPartBy.getPartitions();
            for (int i = 0; i < newSubPartitions.size(); i++) {
                PartitionSpec spec = newSubPartitions.get(i);
                spec.setParentPartPosi(0L);
            }
        }
    }

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }
}

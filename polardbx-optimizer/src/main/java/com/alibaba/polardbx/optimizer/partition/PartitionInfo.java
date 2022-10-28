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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.partition.pruning.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * The complete partition definition of one logical table
 *
 * @author chenghui.lch
 */
public class PartitionInfo {

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
     * the complete definition of subpartitions
     */
    protected SubPartitionByDefinition subPartitionBy;

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
        return subPartitionBy != null;
    }

    public boolean isSinglePartition() {
        int cnt = this.partitionBy.getPartitions().size();
        if (cnt > 1) {
            return false;
        }

        if (this.subPartitionBy != null) {
            int spCnt = this.getPartitionBy().getPartitions().get(0).getSubPartitions().size();
            if (spCnt > 1) {
                return false;
            }
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

    public SubPartitionByDefinition getSubPartitionTemplateDef() {
        return subPartitionBy;
    }

    public void setSubPartitionTemplateDef(SubPartitionByDefinition subPartitionTemplateDef) {
        this.subPartitionBy = subPartitionTemplateDef;
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

    public SubPartitionByDefinition getSubPartitionBy() {
        return subPartitionBy;
    }

    public void setSubPartitionBy(SubPartitionByDefinition subPartitionBy) {
        this.subPartitionBy = subPartitionBy;
    }

    public Integer getStatus() {
        return status;
    }

    public String normalizePartitionByInfo() {
        return partitionBy.normalizePartitionByDefForShowCreateTable(false);
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

        for (PartitionSpec partitionSpec : partitionBy.getPartitions()) {
            final String name = partitionSpec.getName();
            boolean containTargetPartition = GeneralUtil.isEmpty(partitionNames) ||
                partitionNames.stream().filter(r -> r.equalsIgnoreCase(name)).findAny().orElse(null) != null;
            if (containTargetPartition) {

                final PartitionLocation location = partitionSpec.getLocation();
                if (location != null && (!throwException || location.isValidLocation())) {

                    PhysicalPartitionInfo phyPartInfo = new PhysicalPartitionInfo();
                    phyPartInfo.setPartId(partitionSpec.getId());
                    phyPartInfo.setGroupKey(partitionSpec.getLocation().getGroupKey());
                    phyPartInfo.setPhyTable(partitionSpec.getLocation().getPhyTableName());
                    phyPartInfo.setPartName(partitionSpec.getName());
                    phyPartInfo.setPartLevel(PartKeyLevel.PARTITION_KEY);
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
                        .nestedException(new NotSupportException("Not support to get topology with subpartitions"));
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
        if (subPartitionBy == null) {

            String partByDef = "";
            if (tableType == PartitionTableType.SINGLE_TABLE) {
                partByDef += "SINGLE";
            } else if (tableType == PartitionTableType.BROADCAST_TABLE) {
                partByDef += "BROADCAST";
            } else {
                partByDef = partitionBy.normalizePartitionByDefForShowCreateTable(showHashByRange);
                if (this.getAutoFlag() != 0) {
                    partByDef += "\nAUTO_SPLIT=ON";
                }
            }

            return partByDef;

        } else {
            throw GeneralUtil
                .nestedException(new NotSupportException("Not support to show create for tables with subpartitions"));
        }
    }

    public List<String> getPartitionColumns() {
        return getPartitionColumnsNotReorder();
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
        }
        if (subPartitionBy != null) {
            for (String colName : subPartitionBy.getSubPartitionColumnNameList()) {
                if (!shardSet.contains(colName)) {
                    shardSet.add(colName);
                    shardCols.add(colName);
                }
            }
        }
        return shardCols;
    }

    public String getPartitionNameByPhyLocation(String phyGrp, String phyTable) {

        String targetPartName = null;
        List<PartitionSpec> allPartSpecs = this.partitionBy.getPartitions();

        if (subPartitionBy != null) {
            List<SubPartitionSpec> allSubPartSpecs = new ArrayList<>();
            for (int i = 0; i < allPartSpecs.size(); i++) {
                allSubPartSpecs.addAll(allPartSpecs.get(i).getSubPartitions());
            }
            for (int i = 0; i < allSubPartSpecs.size(); i++) {
                SubPartitionSpec p = allSubPartSpecs.get(i);
                String name = p.getName();
                String grpKey = p.getLocation().getGroupKey();
                String phyTbl = p.getLocation().getPhyTableName();
                if (grpKey.equalsIgnoreCase(phyGrp) && phyTbl.equalsIgnoreCase(phyTable)) {
                    targetPartName = name;
                    break;
                }
            }
        } else {
            for (int i = 0; i < allPartSpecs.size(); i++) {
                PartitionSpec p = allPartSpecs.get(i);
                String name = p.getName();
                String grpKey = p.getLocation().getGroupKey();
                String phyTbl = p.getLocation().getPhyTableName();
                if (grpKey.equalsIgnoreCase(phyGrp) && phyTbl.equalsIgnoreCase(phyTable)) {
                    targetPartName = name;
                    break;
                }
            }
        }

        return targetPartName;
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

        if (this.subPartitionBy != null) {
            newPartInfo.setSubPartitionBy(this.subPartitionBy.copy());
        }

        if (this.locality != null) {
            newPartInfo.setLocality(this.locality);
        }

        if (this.partSpecSearcher != null) {
            newPartInfo.setPartSpecSearcher(
                PartSpecSearcher.buildPartSpecSearcher(newPartInfo.getTableType(), newPartInfo.getPartitionBy()));
        }
        return newPartInfo;
    }

    public int getAllPhysicalPartitionCount() {
        if (this.subPartitionBy == null) {
            return this.partitionBy.getPartitions().size();
        }

        int allPhyPartCnt = 0;
        List<PartitionSpec> psList = this.partitionBy.getPartitions();
        for (int i = 0; i < psList.size(); i++) {
            allPhyPartCnt += psList.get(i).getSubPartitions().size();
        }
        return allPhyPartCnt;
    }

    public List<String> getActualPartitionColumns() {
        PartitionTableType tableType = this.tableType;
        List<String> actualPartCols = new ArrayList<>();
        if (tableType == PartitionTableType.BROADCAST_TABLE || tableType == PartitionTableType.SINGLE_TABLE) {
            return actualPartCols;
        }
        return this.getPartitionBy().getActualPartitionColumns();
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
        if (subPartitionBy != null) {
            hashCodeVal ^= subPartitionBy.hashCode();
        }

        return hashCodeVal;
    }

//    @Override
//    public boolean equals(Object obj) {
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
//
//        return getPartitionBy().equals(objPartInfo.getPartitionBy());
//    }

    @Override
    public boolean equals(Object obj) {
        return equals(obj, -1);
    }

    /**
     * check equals by specifying prefix partition column prefixPartColCnt，
     * if prefixPartColCnt <= 0, then use all partition columns
     */
    public boolean equals(Object obj, int prefixPartColCnt) {
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
        if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
            return getPartitionBy().equals(objPartInfo.getPartitionBy());
        } else {
            return getPartitionBy().equals(objPartInfo.getPartitionBy(), prefixPartColCnt);
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
        sb.append(this.toString());
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
         * Prepare the orderNum in one phyDb for each partition
         */
        PartitionInfoBuilder.prepareOrderNumForPartitions(getTableType(), getPartitionBy().getPartitions());

        /**
         * Prepare the mapping from phy_db.phy_tb to partSpec
         */
        this.partSpecSearcher = PartSpecSearcher.buildPartSpecSearcher(getTableType(), getPartitionBy());
    }
}

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

package com.alibaba.polardbx.gms.partition;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Objects;

/**
 * @author chenghui.lch
 */
public class TablePartitionRecord implements SystemTableRecord {

    public final static long NO_PARENT_ID = -1L;
    public final static long NO_PARTITION_POSITION = -1L;

    public final static int PARTITION_LEVEL_LOGICAL_TABLE = 0;
    public final static int PARTITION_LEVEL_PARTITION = 1;
    public final static int PARTITION_LEVEL_SUBPARTITION = 2;
    public final static int PARTITION_LEVEL_NO_NEXT_PARTITION = -1;

    public final static int PARTITION_STATUS_LOGICAL_TABLE_ABSENT = 0;
    public final static int PARTITION_STATUS_LOGICAL_TABLE_PUBLIC = 1;

    public final static int SUBPARTITION_TEMPLATE_USING = 1;
    public final static int SUBPARTITION_TEMPLATE_UNUSED = 0;
    public final static int SUBPARTITION_TEMPLATE_NOT_EXISTED = -1;

    public final static int PARTITION_STATUS_PARTITION_OFFLINE = -1;

    /**
     * Flags for auto-balance, use the variable of auto_flag
     */
    public final static int PARTITION_AUTO_BALANCE_DISABLE = 0;
    public final static int PARTITION_AUTO_BALANCE_ENABLE_ALL = 1;
    public final static int PARTITION_AUTO_BALANCE_ENABLE_SPLIT = 1 << 2;
    public final static int PARTITION_AUTO_BALANCE_ENABLE_MERGE = 1 << 3;
    public final static int PARTITION_AUTO_BALANCE_ENABLE_MOVE = 1 << 4;

    public final static int PARTITION_TABLE_TYPE_PARTITION_TABLE = 0;
    public final static int PARTITION_TABLE_TYPE_GSI_TABLE = 1;
    public final static int PARTITION_TABLE_TYPE_SINGLE_TABLE = 2;
    public final static int PARTITION_TABLE_TYPE_BROADCAST_TABLE = 3;
    public final static int PARTITION_TABLE_TYPE_GSI_SINGLE_TABLE = 4;
    public final static int PARTITION_TABLE_TYPE_GSI_BROADCAST_TABLE = 5;
    public final static int PARTITION_TABLE_TYPE_OSS_TABLE = 6;
    public final static int PARTITION_TABLE_TYPE_COLUMNAR_TABLE = 7;

    public final static String PARTITION_ENGINE_INNODB = "InnoDB";
    public final static String PARTITION_ENGINE_COLUMNAR = "Columnar";

    public Long id;
    public Long parentId;
    public Date createTime;
    public Date updateTime;
    public String tableSchema;
    public String tableName;
    /**
     * Flag that label if the subpartition is defined by using template
     * 1:sub part is defined by template, 0: sub part is NOT defined by template
     */
    public Integer spTempFlag;
    /**
     * The group id that the partition belong to
     * <pre>
     *  When part_level >= 0, it ref to the id of table group;
     *  When next_level = -1, it ref to the id of partition group
     * </pre>
     */
    public Long groupId;
    public Long metaVersion;

    /**
     * auto-balance
     */
    public Integer autoFlag;

    /**
     * The type of partition table
     * <pre>
     *     0: the partition table is a primary table,
     *     1: the partition table is a gsi table
     * </pre>
     */
    public Integer tblType;

    public String partName;

    /**
     * The partition name that is used in subpartition template
     * <pre>
     *     if the partlevel != PARTITION_LEVEL_SUBPARTITION,
     *     its value will be null.
     * <pre/>
     */
    public String partTempName;
    /**
     * The level that labels the level of curr partition
     * <pre>
     *   -1:no_next_level,
     *  0:logical,
     *  1:1st_level_part,
     *  2:2nd_level_part,
     *  3:3rd_level_part
     * </pre>
     */
    public Integer partLevel;
    public Integer nextLevel;
    public Integer partStatus;
    public Long partPosition;
    public String partMethod;
    public String partExpr;
    public String partDesc;
    public String partComment;
    public String partEngine;
    public ExtraFieldJSON partExtras;
    public Long partFlags;
    public String phyTable;

    /**
     * flags values for part_flags
     */
    public final static long FLAG_LOCK = 0x1;
    public final static long FLAG_AUTO_PARTITION = 0x2;// label if a part-table is auto-partitioned table
    public final static long FLAG_TTL_TEMPORARY_TABLE = 0x4; // label if a part-table is a ttl-tmp table

    @Override
    public TablePartitionRecord fill(ResultSet rs) throws SQLException {

        this.id = rs.getLong("id");
        this.parentId = rs.getLong("parent_id");
        this.createTime = rs.getTimestamp("create_time");
        this.updateTime = rs.getTimestamp("update_time");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.spTempFlag = rs.getInt("sp_temp_flag");
        this.groupId = rs.getLong("group_id");
        this.metaVersion = rs.getLong("meta_version");
        this.autoFlag = rs.getInt("auto_flag");
        this.tblType = rs.getInt("tbl_type");
        this.partName = rs.getString("part_name");
        this.partTempName = rs.getString("part_temp_name");
        this.partLevel = rs.getInt("part_level");
        this.nextLevel = rs.getInt("next_level");
        this.partStatus = rs.getInt("part_status");
        this.partPosition = rs.getLong("part_position");
        this.partMethod = rs.getString("part_method");
        this.partExpr = rs.getString("part_expr");
        this.partDesc = rs.getString("part_desc");
        this.partComment = rs.getString("part_comment");
        this.partEngine = rs.getString("part_engine");
        this.partExtras = ExtraFieldJSON.fromJson(rs.getString("part_extras"));
        this.partFlags = rs.getLong("part_flags");
        this.phyTable = rs.getString("phy_table");
        return this;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
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

    public Integer getSpTempFlag() {
        return spTempFlag;
    }

    public void setSpTempFlag(Integer spTempFlag) {
        this.spTempFlag = spTempFlag;
    }

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public Long getMetaVersion() {
        return metaVersion;
    }

    public void setMetaVersion(Long metaVersion) {
        this.metaVersion = metaVersion;
    }

    public Integer getAutoFlag() {
        return autoFlag;
    }

    public void setAutoFlag(Integer autoFlag) {
        this.autoFlag = autoFlag;
    }

    public Integer getTblType() {
        return tblType;
    }

    public void setTblType(Integer tblType) {
        this.tblType = tblType;
    }

    public String getPartName() {
        return partName;
    }

    public void setPartName(String partName) {
        this.partName = partName;
    }

    public String getPartTempName() {
        return partTempName;
    }

    public void setPartTempName(String partTempName) {
        this.partTempName = partTempName;
    }

    public Integer getPartLevel() {
        return partLevel;
    }

    public void setPartLevel(Integer partLevel) {
        this.partLevel = partLevel;
    }

    public Integer getNextLevel() {
        return nextLevel;
    }

    public void setNextLevel(Integer nextLevel) {
        this.nextLevel = nextLevel;
    }

    public Integer getPartStatus() {
        return partStatus;
    }

    public void setPartStatus(Integer partStatus) {
        this.partStatus = partStatus;
    }

    public Long getPartPosition() {
        return partPosition;
    }

    public void setPartPosition(Long partPosition) {
        this.partPosition = partPosition;
    }

    public String getPartMethod() {
        return partMethod;
    }

    public void setPartMethod(String partMethod) {
        this.partMethod = partMethod;
    }

    public String getPartExpr() {
        return partExpr;
    }

    public void setPartExpr(String partExpr) {
        this.partExpr = partExpr;
    }

    public String getPartDesc() {
        return partDesc;
    }

    public void setPartDesc(String partDesc) {
        this.partDesc = partDesc;
    }

    public String getPartComment() {
        return partComment;
    }

    public void setPartComment(String partComment) {
        this.partComment = partComment;
    }

    public String getPartEngine() {
        return partEngine;
    }

    public void setPartEngine(String partEngine) {
        this.partEngine = partEngine;
    }

    public ExtraFieldJSON getPartExtras() {
        return partExtras;
    }

    public void setPartExtras(ExtraFieldJSON partExtras) {
        this.partExtras = partExtras;
    }

    public Long getPartFlags() {
        return partFlags;
    }

    public void setPartFlags(Long partFlags) {
        this.partFlags = partFlags;
    }

    public boolean isAutoPartition() {
        return (this.partFlags & FLAG_AUTO_PARTITION) != 0L;
    }

    public void setAutoPartition() {
        this.partFlags |= FLAG_AUTO_PARTITION;
    }

    public void clearAutoPartition() {
        this.partFlags &= ~FLAG_AUTO_PARTITION;
    }

    public boolean isTtlTemporary() {
        return (this.partFlags & FLAG_TTL_TEMPORARY_TABLE) != 0L;
    }

    public void setTtlTemporary() {
        this.partFlags |= FLAG_TTL_TEMPORARY_TABLE;
    }

    public void clearTtlTemporary() {
        this.partFlags &= ~FLAG_TTL_TEMPORARY_TABLE;
    }

    public String getPhyTable() {
        return phyTable;
    }

    public void setPhyTable(String phyTable) {
        this.phyTable = phyTable;
    }

    public TablePartitionRecord copy() {
        TablePartitionRecord rec = new TablePartitionRecord();
        rec.id = this.id;
        rec.parentId = this.parentId;
        rec.createTime = this.createTime;
        rec.updateTime = this.updateTime;
        rec.tableSchema = this.tableSchema;
        rec.tableName = this.tableName;
        rec.spTempFlag = this.spTempFlag;
        rec.groupId = this.groupId;
        rec.metaVersion = this.metaVersion;
        rec.autoFlag = this.autoFlag;
        rec.tblType = this.tblType;
        rec.partName = this.partName;
        rec.partTempName = this.partTempName;
        rec.partLevel = this.partLevel;
        rec.nextLevel = this.nextLevel;
        rec.partStatus = this.partStatus;
        rec.partPosition = this.partPosition;
        rec.partMethod = this.partMethod;
        rec.partExpr = this.partExpr;
        rec.partDesc = this.partDesc;
        rec.partComment = this.partComment;
        rec.partEngine = this.partEngine;
        rec.partExtras = this.partExtras;
        rec.partFlags = this.partFlags;
        rec.phyTable = this.phyTable;
        return rec;
    }

    @Override
    public String toString() {
        return "TablePartitionRecord{" +
            "id=" + id +
            ", parentId=" + parentId +
            ", createTime=" + createTime +
            ", updateTime=" + updateTime +
            ", tableSchema='" + tableSchema + '\'' +
            ", tableName='" + tableName + '\'' +
            ", spTempFlag=" + spTempFlag +
            ", groupId=" + groupId +
            ", metaVersion=" + metaVersion +
            ", autoFlag=" + autoFlag +
            ", tblType=" + tblType +
            ", partName='" + partName + '\'' +
            ", partTempName='" + partTempName + '\'' +
            ", partLevel=" + partLevel +
            ", nextLevel=" + nextLevel +
            ", partStatus=" + partStatus +
            ", partPosition=" + partPosition +
            ", partMethod='" + partMethod + '\'' +
            ", partExpr='" + partExpr + '\'' +
            ", partDesc='" + partDesc + '\'' +
            ", partComment='" + partComment + '\'' +
            ", partEngine='" + partEngine + '\'' +
            ", partExtras=" + partExtras +
            ", partFlags=" + partFlags +
            ", phyTable='" + phyTable + '\'' +
            '}';
    }

    public static boolean isPartitionRecordEqual(TablePartitionRecord record1, TablePartitionRecord record2) {
        return Objects.equals(record1.parentId, record2.parentId) &&
            Objects.equals(record1.tableSchema, record2.tableSchema) &&
            Objects.equals(record1.tableName, record2.tableName) &&
            Objects.equals(record1.spTempFlag, record2.spTempFlag) &&
            Objects.equals(record1.groupId, record2.groupId) &&
            Objects.equals(record1.metaVersion, record2.metaVersion) &&
            Objects.equals(record1.autoFlag, record2.autoFlag) &&
            Objects.equals(record1.tblType, record2.tblType) &&
            Objects.equals(record1.partName, record2.partName) &&
            Objects.equals(record1.partTempName, record2.partTempName) &&
            Objects.equals(record1.partLevel, record2.partLevel) &&
            Objects.equals(record1.nextLevel, record2.nextLevel) &&
            Objects.equals(record1.partStatus, record2.partStatus) &&
            Objects.equals(record1.partPosition, record2.partPosition) &&
            Objects.equals(record1.partMethod, record2.partMethod) &&
            Objects.equals(record1.partExpr, record2.partExpr) &&
            Objects.equals(record1.partDesc, record2.partDesc) &&
            Objects.equals(record1.partComment, record2.partComment) &&
            Objects.equals(record1.partEngine, record2.partEngine) &&
            Objects.equals(record1.partFlags, record2.partFlags) &&
            Objects.equals(record1.phyTable, record2.phyTable);
    }
}

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

package com.alibaba.polardbx.gms.tablegroup;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@ToString
public class TableGroupRecord implements SystemTableRecord {

    public static final long INVALID_TABLE_GROUP_ID = -1L;
    public static final long NO_TABLE_GROUP_ID = 0L;

    /**
     * 0: tg for partition_tbl
     * 1: default tg for single_tbl
     * 2: non-default tg for single_tbl
     * 3: broadcast tg for broadcast_tbl
     * 4: tg for oss table
     * 5: tg for columnar table
     */
    public static final int TG_TYPE_PARTITION_TBL_TG = 0;
    public static final int TG_TYPE_DEFAULT_SINGLE_TBL_TG = 1;
    public static final int TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG = 2;
    public static final int TG_TYPE_BROADCAST_TBL_TG = 3;
    public static final int TG_TYPE_OSS_TBL_TG = 4;
    public static final int TG_TYPE_COLUMNAR_TBL_TG = 5;

    public Long id;
    public Date gmt_create;
    public Date gmt_modified;
    public String schema;
    public String tg_name;
    public String locality;
    public String primary_zone;
    public Long meta_version;
    public int manual_create;
    public int tg_type = TG_TYPE_PARTITION_TBL_TG;
    public int auto_split_policy;
    public String partition_definition;

    /*
    use as the partition postfix when auto-genetate the partition_name or physical
    table_name, we use the select...for update to exclusively access/update this filed
    */
    private int inited;

    public TableGroupRecord() {

    }

    @Override
    public TableGroupRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmt_create = rs.getTimestamp("gmt_create");
        this.gmt_modified = rs.getTimestamp("gmt_modified");
        this.schema = rs.getString("schema_name");
        this.tg_name = rs.getString("tg_name");
        this.meta_version = rs.getLong("meta_version");
        this.manual_create = rs.getInt("manual_create");
        this.tg_type = rs.getInt("tg_type");
        this.auto_split_policy = rs.getInt("auto_split_policy");
        this.locality = rs.getString("locality");
        this.inited = rs.getInt("inited");
        this.partition_definition = rs.getString("partition_definition");
        return this;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getGmt_create() {
        return gmt_create;
    }

    public void setGmt_create(Date gmt_create) {
        this.gmt_create = gmt_create;
    }

    public Date getGmt_modified() {
        return gmt_modified;
    }

    public void setGmt_modified(Date gmt_modified) {
        this.gmt_modified = gmt_modified;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTg_name() {
        return tg_name;
    }

    public void setTg_name(String tg_name) {
        this.tg_name = tg_name;
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public String getPrimary_zone() {
        return primary_zone;
    }

    public void setPrimary_zone(String primary_zone) {
        this.primary_zone = primary_zone;
    }

    public int getInited() {
        return inited;
    }

    public void setInited(int inited) {
        this.inited = inited;
    }

    public Long getMeta_version() {
        return meta_version;
    }

    public void setMeta_version(Long meta_version) {
        this.meta_version = meta_version;
    }

    public int getManual_create() {
        return manual_create;
    }

    public void setManual_create(int manual_create) {
        this.manual_create = manual_create;
    }

    public int getTg_type() {
        return tg_type;
    }

    public boolean isBroadCastTableGroup() {
        return tg_type == TG_TYPE_BROADCAST_TBL_TG;
    }

    public boolean isSingleTableGroup() {
        return tg_type == TG_TYPE_DEFAULT_SINGLE_TBL_TG || tg_type == TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG;
    }

    public boolean isColumnarTableGroup() {
        return tg_type == TG_TYPE_COLUMNAR_TBL_TG || TableGroupNameUtil.isColumnarTg(tg_name);
    }

    public boolean withBalanceSingleTableLocality() {
        return LocalityDesc.parse(locality).getBalanceSingleTable();
    }

    public boolean isLocalitySpecified() {
        return StringUtils.isEmpty(locality);
    }

    public void setTg_type(int tg_type) {
        this.tg_type = tg_type;
    }

    public int getAuto_split_policy() {
        return auto_split_policy;
    }

    public void setAuto_split_policy(int auto_split_policy) {
        this.auto_split_policy = auto_split_policy;
    }

    public String getPartition_definition() {
        return partition_definition;
    }

    public void setPartition_definition(String partition_definition) {
        this.partition_definition = partition_definition;
    }
}

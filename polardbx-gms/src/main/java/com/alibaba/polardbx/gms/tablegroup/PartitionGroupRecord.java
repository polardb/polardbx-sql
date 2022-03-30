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

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PartitionGroupRecord implements SystemTableRecord {
    public Long id;
    public String partition_name;
    public Long tg_id;
    public Date gmt_create;
    public Date gmt_modified;
    public String phy_db;
    public String locality;
    public String primary_zone;
    public Long pax_group_id = 0L;
    public Long meta_version = 1L;
    public int visible = 1;

    @Override
    public PartitionGroupRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.partition_name = rs.getString("partition_name");
        this.tg_id = rs.getLong("tg_id");
        this.gmt_create = rs.getTimestamp("gmt_create");
        this.gmt_modified = rs.getTimestamp("gmt_modified");
        this.phy_db = rs.getString("phy_db");
        this.locality = rs.getString("locality");
        this.primary_zone = rs.getString("primary_zone");
        this.meta_version = rs.getLong("meta_version");
        this.pax_group_id = rs.getLong("pax_group_id");
        this.visible = rs.getInt("visible");
        return this;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getPartition_name() {
        return partition_name;
    }

    public void setPartition_name(String partition_name) {
        this.partition_name = partition_name;
    }

    public Long getTg_id() {
        return tg_id;
    }

    public void setTg_id(Long tg_id) {
        this.tg_id = tg_id;
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

    public String getPhy_db() {
        return phy_db;
    }

    public void setPhy_db(String phy_db) {
        this.phy_db = phy_db;
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

    public Long getPax_group_id() {
        return pax_group_id;
    }

    public void setPax_group_id(Long pax_group_id) {
        this.pax_group_id = pax_group_id;
    }

    public Long getMeta_version() {
        return meta_version;
    }

    public void setMeta_version(Long meta_version) {
        this.meta_version = meta_version;
    }

    public int getVisible() {
        return visible;
    }

    public void setVisible(int visible) {
        this.visible = visible;
    }

    public String digest() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("pgId:");
        sb.append(id);
        sb.append(" name:");
        sb.append(partition_name);
        sb.append(" tgId:");
        sb.append(tg_id);
        sb.append(" phyDb:");
        sb.append(phy_db);
        return sb.toString();
    }
}

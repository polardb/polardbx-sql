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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.utils.TStringUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author chenghui.lch
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class GroupDetailInfoExRecord extends GroupDetailInfoRecord implements Comparable<GroupDetailInfoExRecord> {

    public String phyDbName;

    public GroupDetailInfoExRecord() {
    }

    public GroupDetailInfoExRecord(String groupName, String storageInst) {
        this.groupName = groupName;
        this.storageInstId = storageInst;
    }

    @Override
    public GroupDetailInfoExRecord fill(ResultSet rs) throws SQLException {
        this.storageInstId = rs.getString("storage_inst_id");
        this.dbName = rs.getString("db_name");
        this.groupName = rs.getString("group_name");
        this.phyDbName = rs.getString("phy_db_name");
        return this;
    }

    @Override
    public int compareTo(GroupDetailInfoExRecord o) {
        int res = TStringUtil.compareTo(this.dbName, o.dbName);
        if (res != 0) {
            return res;
        }
        res = TStringUtil.compareTo(this.groupName, o.groupName);
        return res;
    }
}

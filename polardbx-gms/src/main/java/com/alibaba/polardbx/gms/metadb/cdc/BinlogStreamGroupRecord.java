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

package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BinlogStreamGroupRecord implements SystemTableRecord {
    private Long id;
    private String groupName;
    private String groupDesc;

    @SuppressWarnings("unchecked")
    @Override
    public BinlogStreamGroupRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.groupName = rs.getString("group_name");
        this.groupDesc = rs.getString("group_desc");
        return this;
    }

    public String getGroupName() {
        return groupName;
    }

    public Long getId() {
        return id;
    }

    public String getGroupDesc() {
        return groupDesc;
    }
}

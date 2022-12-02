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
import java.sql.Timestamp;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class JoinGroupInfoRecord implements SystemTableRecord {

    public long id;
    public Timestamp gmtCreate;
    public Timestamp gmtModified;
    public String tableSchema;
    public String joinGroupName;
    public String locality;

    @Override
    public JoinGroupInfoRecord fill(ResultSet result) throws SQLException {
        this.id = result.getLong("id");
        this.gmtCreate = result.getTimestamp("gmt_create");
        this.gmtModified = result.getTimestamp("gmt_modified");
        this.tableSchema = result.getString("table_schema");
        this.joinGroupName = result.getString("join_group_name");
        this.locality = result.getString("locality");
        return this;
    }

}

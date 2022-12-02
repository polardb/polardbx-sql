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

package com.alibaba.polardbx.gms.metadb.seq;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SequencesRecord implements SystemTableRecord {

    public String id;
    public String schemaName;
    public String name;
    public String value;
    public String unitCount;
    public String unitIndex;
    public String innerStep;
    public String incrementBy;
    public String startWith;
    public String maxValue;
    public String cycle;
    public String type;
    public String status;
    public String gmtCreated;
    public String gmtModified;

    @Override
    public SequencesRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getString("id");
        this.schemaName = rs.getString("schema_name");
        this.name = rs.getString("name");
        this.value = rs.getString("value");
        this.unitCount = rs.getString("unit_count");
        this.unitIndex = rs.getString("unit_index");
        this.innerStep = rs.getString("inner_step");
        this.incrementBy = rs.getString("increment_by");
        this.startWith = rs.getString("start_with");
        this.maxValue = rs.getString("max_value");
        this.cycle = rs.getString("cycle");
        this.type = rs.getString("type");
        this.status = rs.getString("status");
        this.gmtCreated = rs.getString("gmt_created");
        this.gmtModified = rs.getString("gmt_modified");
        return this;
    }

}

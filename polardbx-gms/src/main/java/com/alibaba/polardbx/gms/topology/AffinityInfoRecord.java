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

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AffinityInfoRecord implements SystemTableRecord {

    public String storageInstId;
    /**
     * CN ip
     */
    public String ip;
    /**
     * CN port
     */
    public int port;

    public String address;

    @Override
    public AffinityInfoRecord fill(ResultSet rs) throws SQLException {
        this.storageInstId = rs.getString("storage_inst_id");
        this.ip = rs.getString("ip");
        this.port = rs.getInt("port");
        this.address = rs.getString("address");
        return this;
    }

    public AffinityInfoRecord copy() {
        AffinityInfoRecord newRecord = new AffinityInfoRecord();

        newRecord.storageInstId = this.storageInstId;
        newRecord.ip = this.ip;
        newRecord.port = this.port;
        newRecord.address = this.address;

        return newRecord;
    }
}

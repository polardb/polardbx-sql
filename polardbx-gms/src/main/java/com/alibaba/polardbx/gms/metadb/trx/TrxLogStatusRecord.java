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

package com.alibaba.polardbx.gms.metadb.trx;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author yaozhili
 */
public class TrxLogStatusRecord implements SystemTableRecord {

    public int status;
    public String currentTableName;
    public Timestamp gmtModified;
    public int flag;
    public Timestamp now;

    @Override
    public TrxLogStatusRecord fill(ResultSet rs) throws SQLException {
        this.status = rs.getInt("status");
        this.currentTableName = rs.getString("current_table_name");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.flag = rs.getInt("flag");
        this.now = rs.getTimestamp("now");
        return this;
    }
}

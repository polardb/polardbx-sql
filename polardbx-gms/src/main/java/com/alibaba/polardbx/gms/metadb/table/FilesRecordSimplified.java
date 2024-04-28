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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class FilesRecordSimplified implements SystemTableRecord {

    public String fileName;
    public String partitionName;
    public Long commitTs;
    public Long removeTs;
    public Long schemaTs;

    @Override
    public FilesRecordSimplified fill(ResultSet rs) throws SQLException {
        this.fileName = rs.getString("file_name");
        this.commitTs = rs.getLong("commit_ts");
        if (rs.wasNull()) {
            this.commitTs = null;
        }
        this.removeTs = rs.getLong("remove_ts");
        if (rs.wasNull()) {
            this.removeTs = null;
        }
        this.partitionName = rs.getString("partition_name");
        this.schemaTs = rs.getLong("schema_ts");
        if (rs.wasNull()) {
            this.schemaTs = null;
        }

        return this;
    }
}

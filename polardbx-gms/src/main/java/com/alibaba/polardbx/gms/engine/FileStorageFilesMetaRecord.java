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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class FileStorageFilesMetaRecord implements SystemTableRecord {
    public long id;
    public String fileName;
    public String engine;
    public Long commitTs;
    public Long removeTs;

    @Override
    public FileStorageFilesMetaRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.fileName = rs.getString("file_name");
        this.engine = rs.getString("engine");
        this.commitTs = rs.getLong("commit_ts");
        if (rs.wasNull()) {
            this.commitTs = null;
        }
        this.removeTs = rs.getLong("remove_ts");
        if (rs.wasNull()) {
            this.removeTs = null;
        }
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.fileName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.engine);
        if (this.commitTs == null) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.commitTs);
        }

        if (this.removeTs == null) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.removeTs);
        }
        return params;
    }
}


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

package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EncdbKey implements SystemTableRecord {

    private long id;

    private String key;

    private String type;

    @Override
    public EncdbKey fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.key = rs.getString("key");
        this.type = KeyType.valueOf(rs.getString("type")).name();
        return this;
    }

    public String getKey() {
        return key;
    }

    public String getType() {
        return type;
    }

    public long getId() {
        return id;
    }

    public static enum KeyType {
        MEK_HASH, MEK_ENC;
    }
}

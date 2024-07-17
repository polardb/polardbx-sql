/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.sql.Timestamp;

public class ColumnarDataConsistencyLockRecord implements SystemTableRecord {
    public long id;
    public String entityId;
    public int ownerId;
    public long state;
    public int lastOwner;
    public Timestamp lastOwnerChange;
    public Timestamp createTime;
    public Timestamp updateTime;

    @Override
    public ColumnarDataConsistencyLockRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.entityId = rs.getString("entity_id");
        this.ownerId = rs.getInt("owner_id");
        this.state = rs.getLong("state");
        this.lastOwner = rs.getInt("last_owner");
        this.lastOwnerChange = rs.getTimestamp("last_owner_change");
        this.createTime = rs.getTimestamp("create_time");
        this.updateTime = rs.getTimestamp("update_time");

        return this;
    }

    @Override
    public String toString() {
        return "ColumnarDataConsistencyLockRecord{" +
            "id=" + id +
            ", entityId='" + entityId + '\'' +
            ", ownerId=" + ownerId +
            ", state=" + state +
            ", lastOwner=" + lastOwner +
            ", lastOwnerChange=" + lastOwnerChange +
            ", createTime=" + createTime +
            ", updateTime=" + updateTime +
            '}';
    }

    public enum OWNER_TYPE {
        /**
         * incremental data transfer
         */
        STREAM(Val.STREAM_ID),
        /**
         * full data transfer
         */
        SNAPSHOT(Val.STREAM_ID),
        /**
         * compaction
         */
        COMPACTION(Val.COMPACTION_ID);

        private final int ownerId;

        OWNER_TYPE(int ownerId) {
            this.ownerId = ownerId;
        }

        public int getOwnerId() {
            return ownerId;
        }

        public static OWNER_TYPE of(int ownerId) {
            switch (ownerId) {
            case Val.STREAM_ID:
                return STREAM;
            case Val.SNAPSHOT_ID:
                return SNAPSHOT;
            case Val.COMPACTION_ID:
                return COMPACTION;
            default:
                throw new IllegalArgumentException("Unknown owner id: " + ownerId);
            }
        }

        private final class Val {
            private static final int STREAM_ID = 0;
            private static final int SNAPSHOT_ID = 1;
            private static final int COMPACTION_ID = 2;
        }
    }
}

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

package com.alibaba.polardbx.gms.metadb.lease;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class LeaseRecord implements SystemTableRecord {

    private long id;

    private String schemaName;
    private String leaseHolder;
    private String leaseKey;

    private long startAt;
    private long lastModified;
    private long expireAt;
    private long ttlMillis;

    public static final long DRIFT = 500;

    public static LeaseRecord create(String schemaName,
                                     String leaseKey,
                                     long ttlMillis){
        LeaseRecord record = new LeaseRecord();
        record.schemaName = schemaName;
        record.leaseKey = leaseKey;
        record.ttlMillis = ttlMillis;
        record.leaseHolder = getLeaseHolder();
        return record;
    }

    public static String getLeaseHolder(){
        return TddlNode.getHost() + ":" + TddlNode.getPort();
    }

    public boolean valid(){
        return (System.currentTimeMillis() + DRIFT) < expireAt;
    }

    public boolean inValid(){
        return !valid();
    }

    @Override
    public LeaseRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.schemaName = rs.getString("schema_name");
        this.leaseHolder = rs.getString("lease_holder");
        this.leaseKey = rs.getString("lease_key");
        this.startAt = rs.getLong("start_at");
        this.lastModified = rs.getLong("last_modified");
        this.expireAt = rs.getLong("expire_at");
        this.ttlMillis = rs.getLong("ttl_millis");
        return this;
    }

    public Map<Integer, ParameterContext> buildParamsForInsert() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.leaseHolder);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.leaseKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ttlMillis);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ttlMillis);
        return params;
    }

    public long getId() {
        return this.id;
    }

    public void setId(final long id) {
        this.id = id;
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    public void setLeaseHolder(final String leaseHolder) {
        this.leaseHolder = leaseHolder;
    }

    public String getLeaseKey() {
        return this.leaseKey;
    }

    public void setLeaseKey(final String leaseKey) {
        this.leaseKey = leaseKey;
    }

    public long getStartAt() {
        return this.startAt;
    }

    public void setStartAt(final long startAt) {
        this.startAt = startAt;
    }

    public long getLastModified() {
        return this.lastModified;
    }

    public void setLastModified(final long lastModified) {
        this.lastModified = lastModified;
    }

    public long getExpireAt() {
        return this.expireAt;
    }

    public void setExpireAt(final long expireAt) {
        this.expireAt = expireAt;
    }

    public long getTtlMillis() {
        return this.ttlMillis;
    }

    public void setTtlMillis(final long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    public String info(){
        return toString();
    }

    @Override
    public String toString() {
        return "LeaseRecord{" +
            "id=" + id +
            ", schemaName='" + schemaName + '\'' +
            ", leaseHolder='" + leaseHolder + '\'' +
            ", leaseKey='" + leaseKey + '\'' +
            ", startAt=" + startAt +
            ", lastModified=" + lastModified +
            ", expireAt=" + expireAt +
            ", ttlMillis=" + ttlMillis +
            '}';
    }
}

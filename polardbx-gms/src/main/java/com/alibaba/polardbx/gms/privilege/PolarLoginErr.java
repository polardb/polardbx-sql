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

package com.alibaba.polardbx.gms.privilege;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * +----+---------------------+---------------------+---------------+-----------------+-------------+---------------------+
 * | id | gmt_created         | gmt_modified        | user_name     | max_error_limit | error_count | expire_date         |
 * +----+---------------------+---------------------+---------------+-----------------+-------------+---------------------+
 * |  9 | 2020-12-21 15:51:30 | 2020-12-21 15:51:30 | POLARDBX_ROOT | 5               | 1           | 2020-12-21 15:51:30 |
 * +----+---------------------+---------------------+---------------+-----------------+-------------+---------------------+
 */
public class PolarLoginErr implements SystemTableRecord {

    private long id;
    private Timestamp gmtCreated;
    private Timestamp gmtModified;
    private String limitKey;
    private Timestamp expireDate;
    private int maxErrorLimit;
    private int errorCount;
    private AtomicBoolean enableUpdate;

    public PolarLoginErr() {
        enableUpdate = new AtomicBoolean(true);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getLimitKey() {
        return limitKey;
    }

    public void setLimitKey(String limitKey) {
        this.limitKey = limitKey;
    }

    public Timestamp getGmtCreated() {
        return gmtCreated;
    }

    public void setGmtCreated(Timestamp gmtCreated) {
        this.gmtCreated = gmtCreated;
    }

    public Timestamp getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Timestamp gmtModified) {
        this.gmtModified = gmtModified;
    }

    public Timestamp getExpireDate() {
        return expireDate;
    }

    public void setExpireDate(Timestamp expireDate) {
        this.expireDate = expireDate;
    }

    public int getMaxErrorLimit() {
        return maxErrorLimit;
    }

    public void setMaxErrorLimit(int maxErrorLimit) {
        this.maxErrorLimit = maxErrorLimit;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(int errorCount) {
        this.errorCount = errorCount;
    }

    public AtomicBoolean getEnableUpdate() {
        return enableUpdate;
    }

    @Override
    public PolarLoginErr fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong(1);
        this.gmtCreated = rs.getTimestamp(2);
        this.gmtModified = rs.getTimestamp(3);
        this.limitKey = rs.getString(4);
        this.maxErrorLimit = rs.getInt(5);
        this.errorCount = rs.getInt(6);
        this.expireDate = rs.getTimestamp(7);
        return this;
    }
}

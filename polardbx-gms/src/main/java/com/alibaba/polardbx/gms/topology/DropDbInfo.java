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

/**
 * @author chenghui.lch
 */
public class DropDbInfo {

    protected String dbName;

    boolean isDropIfExists;

    protected long socketTimeout;

    private boolean allowDropForce;

    private long ts;

    /**
     * 用于import database场景
     * 为true表示仅删除逻辑层元信息，保留物理库
     * 默认为false
     */
    private boolean reservePhyDb = false;

    private long versionId;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public long getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(long socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public boolean isDropIfExists() {
        return isDropIfExists;
    }

    public void setDropIfExists(boolean dropIfExists) {
        isDropIfExists = dropIfExists;
    }

    public boolean isAllowDropForce() {
        return allowDropForce;
    }

    public void setAllowDropForce(boolean allowDropForce) {
        this.allowDropForce = allowDropForce;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public boolean isReservePhyDb() {
        return reservePhyDb;
    }

    public void setReservePhyDb(boolean reservePhyDb) {
        this.reservePhyDb = reservePhyDb;
    }

    public long getVersionId() {
        return versionId;
    }

    public void setVersionId(long versionId) {
        this.versionId = versionId;
    }
}

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

package com.alibaba.polardbx.common.model;

public class RepoInst {

    public static final int REPO_INST_TYPE_MASTER = 1;

    public static final int REPO_INST_TYPE_SLAVE  = 2;

    public static final int REPO_INST_ALL         = 3;

    protected DBType        dbType                = DBType.MYSQL;

    protected String        address;

    /**
     *
     */
    protected String        dnId;

    /**
     * repoInstId = dnId@addr
     */
    protected String        repoInstId;

    /**
     * 实例的读权重
     */
    protected int           readWeight;

    protected int           writeWeight;

    public DBType getDbType() {
        return dbType;
    }

    public void setDbType(DBType dbType) {
        this.dbType = dbType;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getReadWeight() {
        return readWeight;
    }

    public void setReadWeight(int readWeight) {
        this.readWeight = readWeight;
    }

    public int getWriteWeight() {
        return writeWeight;
    }

    public void setWriteWeight(int writeWeight) {
        this.writeWeight = writeWeight;
    }

    public String getDnId() {
        return dnId;
    }

    public void setDnId(String dnId) {
        this.dnId = dnId;
    }

    public String getRepoInstId() {
        return repoInstId;
    }

    public void setRepoInstId(String repoInstId) {
        this.repoInstId = repoInstId;
    }
}

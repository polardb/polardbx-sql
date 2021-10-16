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

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class CreateDbInfo {
    protected int dbType = DbInfoRecord.DB_TYPE_PART_DB;
    protected boolean isCreateIfNotExists = false;
    protected String charset = "";
    protected String collation = "";
    protected String dbName;
    protected String locality = "";
    protected long shardDbCountEachInst = -1;
    protected long socketTimeout = -1;

    /**
     * The groupKey of singleGroup that use to storage all single tables
     * <pre>
     *      the singleGroup is the defaultGroup of tddlRule
     * </pre>
     */
    protected String singleGroup;

    /**
     * The default phy group for log db
     */
    protected String defaultDbIndex;

    /**
     * The map contains all the groups and their phyDbName,
     * include singleDbGroup and its phyDbName
     * <pre>
     *  key: groupKey
     *  val: phyDbName
     * </pre>
     */
    protected Map<String, String> groupPhyDbMap;

    /**
     * All the storage inst list
     */
    protected List<String> storageInstList;

    /**
     * The GroupLocator decide each group to locate on which storage inst
     */
    protected GroupLocator groupLocator;

    public int getDbType() {
        return dbType;
    }

    public void setDbType(int dbType) {
        this.dbType = dbType;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSingleGroup() {
        return singleGroup;
    }

    public void setSingleGroup(String singleGroup) {
        this.singleGroup = singleGroup;
    }

    public Map<String, String> getGroupPhyDbMap() {
        return groupPhyDbMap;
    }

    public void setGroupPhyDbMap(Map<String, String> groupPhyDbMap) {
        this.groupPhyDbMap = groupPhyDbMap;
    }

    public List<String> getStorageInstList() {
        return storageInstList;
    }

    public void setStorageInstList(List<String> storageInstList) {
        this.storageInstList = storageInstList;
    }

    public GroupLocator getGroupLocator() {
        return groupLocator;
    }

    public void setGroupLocator(GroupLocator groupLocator) {
        this.groupLocator = groupLocator;
    }

    public boolean isCreateIfNotExists() {
        return isCreateIfNotExists;
    }

    public void setCreateIfNotExists(boolean createIfNotExists) {
        isCreateIfNotExists = createIfNotExists;
    }

    public long getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(long socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public long getShardDbCountEachInst() {
        return shardDbCountEachInst;
    }

    public void setShardDbCountEachInst(long shardDbCountEachInst) {
        this.shardDbCountEachInst = shardDbCountEachInst;
    }

    public String getDefaultDbIndex() {
        return defaultDbIndex;
    }

    public void setDefaultDbIndex(String defaultDbIndex) {
        this.defaultDbIndex = defaultDbIndex;
    }
}

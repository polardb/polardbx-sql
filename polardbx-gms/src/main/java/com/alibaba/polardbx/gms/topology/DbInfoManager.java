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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
public class DbInfoManager extends AbstractLifecycle {

    protected final static Set<Integer> normalPartDbTypeSet = new HashSet<>();

    static {
        normalPartDbTypeSet.add(DbInfoRecord.DB_TYPE_PART_DB);
        normalPartDbTypeSet.add(DbInfoRecord.DB_TYPE_NEW_PART_DB);
    }

    public static final String PARTITION_MODE_SHARDING = "sharding";
    public static final String PARTITION_MODE_PARTITIONING = "partitioning";

    protected volatile Map<String, DbInfoRecord> allDbInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    protected volatile Map<Long, DbInfoRecord> dbIdMap = new HashMap<>();
    protected static DbInfoManager instance = new DbInfoManager();

    public static DbInfoManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    public void loadDbInfoFromMetaDb(Map<String, DbInfoRecord> allRunningDbInfoMap,
                                     Map<String, DbInfoRecord> allRemovedDbInfoMap) {
        Map<String, DbInfoRecord> newAllDbInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<Long, DbInfoRecord> dbIdMap = new HashMap<>();
        try (Connection metaConn = MetaDbDataSource.getInstance().getConnection()) {
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaConn);
            List<DbInfoRecord> dbInfoRecords = dbInfoAccessor.getAllDbInfos();
            for (DbInfoRecord dbInfoRecord : dbInfoRecords) {
                if (dbInfoRecord.dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                    newAllDbInfoMap.put(dbInfoRecord.dbName, dbInfoRecord);
                    allRunningDbInfoMap.put(dbInfoRecord.dbName, dbInfoRecord);
                    dbIdMap.put(dbInfoRecord.id, dbInfoRecord);
                } else if (dbInfoRecord.dbStatus == DbInfoRecord.DB_STATUS_DROPPING) {
                    allRemovedDbInfoMap.put(dbInfoRecord.dbName, dbInfoRecord);
                }
            }
            this.allDbInfoMap = newAllDbInfoMap;
            this.dbIdMap = dbIdMap;
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static boolean isNormalDbType(int dbType) {
        return normalPartDbTypeSet.contains(dbType);
    }

    public static String getPartitionModeByDbType(int dbType) {
        if (dbType == DbInfoRecord.DB_TYPE_PART_DB) {
            return PARTITION_MODE_SHARDING;
        } else if (dbType == DbInfoRecord.DB_TYPE_NEW_PART_DB) {
            return PARTITION_MODE_PARTITIONING;
        } else {
            return null;
        }
    }

    public String getDbChartSet(String dbName) {
        DbInfoRecord dbInfoRecord = allDbInfoMap.get(dbName);
        if (dbInfoRecord == null) {
            return null;
        }
        return dbInfoRecord.charset;
    }

    public List<String> getDbList() {
        return new ArrayList<>(allDbInfoMap.keySet());
    }

    public List<DbInfoRecord> getDbInfoList() {
        return new ArrayList<>(allDbInfoMap.values());
    }

    public DbInfoRecord getDbInfo(String dbName) {
        return allDbInfoMap.get(dbName);
    }

    public DbInfoRecord getDbInfo(long dbId) {
        return this.dbIdMap.get(dbId);
    }

    public boolean isNewPartitionDb(String dbName) {
        DbInfoRecord dbInfoRecord = allDbInfoMap.get(dbName);
        if (dbInfoRecord == null) {
            return false;
        }
        return dbInfoRecord.dbType == DbInfoRecord.DB_TYPE_NEW_PART_DB;
    }

    /**
     * Only allowed used for Junit Test!!!!
     */
    public void addNewMockPartitionDb(String mockDbName) {
        DbInfoRecord mockNewPartDb = new DbInfoRecord();
        mockNewPartDb.dbType = DbInfoRecord.DB_TYPE_NEW_PART_DB;
        mockNewPartDb.dbStatus = DbInfoRecord.DB_STATUS_RUNNING;
        mockNewPartDb.dbName = mockDbName;
        mockNewPartDb.charset = "utf8mb4";
        mockNewPartDb.appName = mockDbName;
        allDbInfoMap.put(mockDbName, mockNewPartDb);
    }

    public void removeMockPartitionDb(String mockDbName) {
        allDbInfoMap.remove(mockDbName);
    }

    @Override
    protected void doInit() {
        super.doInit();

        if (ConfigDataMode.isMock()) {
            return;
        }

        // load all db and its charsetEncoding
        loadDbInfoFromMetaDb(new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER),
            new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }
}

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

package com.alibaba.polardbx.stats;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AppStatistics {

    private Map<String, GroupStatistics> groupMap = new ConcurrentHashMap<>();

    private boolean isPrecise = true;

    private final AtomicLong readCount  = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();
    private       String dbName;
    private final String appName;

    public AppStatistics(String dbName, String appName){
        this.dbName = (dbName == null) ? "" : dbName;
        this.appName = appName;
    }

    private GroupStatistics getGroupStatistics(String groupName) {
        GroupStatistics stat = groupMap.get(groupName);
        if (stat == null) {
            GroupStatistics newStat = new GroupStatistics(groupName);
            stat = groupMap.putIfAbsent(groupName, newStat);
            return (stat == null) ? newStat : stat;
        }
        return stat;
    }

    public GroupStatistics addGroup(String groupName) {
        return getGroupStatistics(groupName);
    }

    public String getAppName() {
        return appName;
    }

    public GroupStatistics getGroup(String groupName) {
        return groupMap.get(groupName);
    }

    public long addRead(long count) {
        return readCount.addAndGet(count);
    }

    public long addWrite(long count) {
        return writeCount.addAndGet(count);
    }

    public void addPhysicalReadRequestPerAtom(String groupName, String dbKey, long count, long sqlLength) {
        if (groupName == null || dbKey == null || count <= 0) {
            return;
        }

        GroupStatistics gs = getGroupStatistics(groupName);
        gs.addPhysicalReadRequestPerAtom(dbKey, count, sqlLength);
    }

    public void addPhysicalWriteRequestPerAtom(String groupName, String dbKey, long count, long sqlLength) {
        if (groupName == null || dbKey == null || count <= 0) {
            return;
        }

        GroupStatistics gs = getGroupStatistics(groupName);
        gs.addPhysicalWriteRequestPerAtom(dbKey, count, sqlLength);
    }

    public List<List<Object>> toObjects(List<List<Object>> obsList) {
        for (GroupStatistics gs : groupMap.values()) {
            gs.toObjects(obsList, dbName, appName);
        }
        return obsList;
    }

    public void addSqlErrorCount(String groupName, String dbKey, long count) {
        if (groupName == null || dbKey == null || count <= 0) {
            return;
        }

        GroupStatistics gs = getGroupStatistics(groupName);
        gs.addSqlErrorCount(dbKey, count);
    }

    public void addConnErrorCount(String groupName, String dbKey, long count) {
        if (groupName == null || dbKey == null || count <= 0) {
            return;
        }

        GroupStatistics gs = getGroupStatistics(groupName);
        gs.addConnErrorCount(dbKey, count);
    }

    public void addReadTimeCost(String groupName, String dbKey, long cost) {
        if (groupName == null || dbKey == null || cost <= 0) {
            return;
        }

        GroupStatistics gs = getGroupStatistics(groupName);
        gs.addReadTimeCost(dbKey, cost);
    }

    public void addWriteTimeCost(String groupName, String dbKey, long cost) {
        if (groupName == null || dbKey == null || cost <= 0) {
            return;
        }

        GroupStatistics gs = getGroupStatistics(groupName);
        gs.addWriteTimeCost(dbKey, cost);
    }

    public void addRows(String groupName, String dbKey, long row) {
        if (groupName == null || dbKey == null || row < 0) {
            return;
        }

        GroupStatistics gs = getGroupStatistics(groupName);
        gs.addRows(dbKey, row);
    }

    public void clear() {
        for (GroupStatistics gs : groupMap.values()) {
            gs.clear();
        }
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}

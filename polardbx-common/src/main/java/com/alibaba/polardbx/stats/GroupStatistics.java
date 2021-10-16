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

public class GroupStatistics {

    private final Map<String, AtomStatistics> atomStatisticsMap = new ConcurrentHashMap<>();
    private final String                      groupName;

    public GroupStatistics(String groupName){
        this.groupName = groupName;
    }

    private AtomStatistics getAtomStatistics(String dbKey, String ipPort) {
        AtomStatistics stat = atomStatisticsMap.get(dbKey);
        if (stat == null) {
            AtomStatistics newStat = new AtomStatistics(dbKey, ipPort);
            stat = atomStatisticsMap.putIfAbsent(dbKey, newStat);
            return (stat == null) ? newStat : stat;
        }
        return stat;
    }

    public AtomStatistics addAtom(String dbKey, String ipPort) {
        if (dbKey == null) {
            return null;
        }
        return getAtomStatistics(dbKey, ipPort);
    }

    public void addPhysicalReadRequestPerAtom(String dbKey, long count, long sqlLength) {
        if (dbKey == null || count <= 0) {
            return;
        }

        AtomStatistics as = getAtomStatistics(dbKey, null);
        as.addPhysicalReadRequestPerAtom(count, sqlLength);
    }

    public void addPhysicalWriteRequestPerAtom(String dbKey, long count, long sqlLength) {
        if (dbKey == null || count <= 0) {
            return;
        }

        AtomStatistics as = getAtomStatistics(dbKey, null);
        as.addPhysicalWriteRequestPerAtom(count, sqlLength);
    }

    public List<List<Object>> toObjects(List<List<Object>> obsList, String dbName, String appName) {
        for (AtomStatistics as : atomStatisticsMap.values()) {
            obsList.add(as.toObjects(dbName, appName, groupName));
        }
        return obsList;
    }

    public AtomStatistics getAtom(String atomName) {
        return atomStatisticsMap.get(atomName);
    }

    public void addSqlErrorCount(String dbKey, long count) {
        if (dbKey == null || count <= 0) {
            return;
        }

        AtomStatistics as = getAtomStatistics(dbKey, null);
        as.addSqlError(count);
    }

    public void addConnErrorCount(String dbKey, long count) {
        if (dbKey == null || count <= 0) {
            return;
        }

        AtomStatistics as = getAtomStatistics(dbKey, null);
        as.addConnError(count);
    }

    public void addReadTimeCost(String dbKey, long cost) {
        if (dbKey == null || cost <= 0) {
            return;
        }

        AtomStatistics as = getAtomStatistics(dbKey, null);
        as.addReadTimeCost(cost);
    }

    public void addWriteTimeCost(String dbKey, long cost) {
        if (dbKey == null || cost <= 0) {
            return;
        }

        AtomStatistics as = getAtomStatistics(dbKey, null);
        as.addWriteTimeCost(cost);
    }

    public void addRows(String dbKey, long row) {
        if (dbKey == null || row < 0) {
            return;
        }

        AtomStatistics as = getAtomStatistics(dbKey, null);
        as.addRows(row);
    }

    public void clear() {
        for (AtomStatistics as : atomStatisticsMap.values()) {
            as.clear();
        }
    }

    public void removeAtom(String atomName) {
        atomStatisticsMap.remove(atomName);
    }
}

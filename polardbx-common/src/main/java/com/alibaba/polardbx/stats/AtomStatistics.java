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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class AtomStatistics {

    private final AtomicLong connErrorCount              = new AtomicLong();
    private final AtomicLong sqlErrorCount               = new AtomicLong();
    private final AtomicLong physicalReadRequestPerAtom  = new AtomicLong();
    private final AtomicLong physicalWriteRequestPerAtom = new AtomicLong();
    private final AtomicLong physicalReadTime            = new AtomicLong();
    private final AtomicLong physicalWriteTime           = new AtomicLong();
    private final AtomicLong sqlLength                   = new AtomicLong();
    private final AtomicLong rows                        = new AtomicLong();

    private final String dbkey;
    private String ipPort;

    public AtomStatistics(String dbkey, String ipPort){
        this.dbkey = dbkey;
        this.ipPort = (ipPort == null) ? "" : ipPort;
    }

    public void clean() {
        connErrorCount.set(0L);
        sqlErrorCount.set(0L);
        physicalReadRequestPerAtom.set(0L);
        physicalWriteRequestPerAtom.set(0L);
    }

    public String getDbkey() {
        return dbkey;
    }

    public Long addConnError() {
        return connErrorCount.incrementAndGet();
    }

    public Long addConnError(long count) {
        return connErrorCount.addAndGet(count);
    }

    public Long getConnError() {
        return connErrorCount.get();
    }

    public Long addSqlError() {
        return sqlErrorCount.incrementAndGet();
    }

    public Long addReadTimeCost(long cost) {
        return physicalReadTime.addAndGet(cost);
    }

    public Long addWriteTimeCost(long cost) {
        return physicalWriteTime.addAndGet(cost);
    }

    public Long addSqlError(long count) {
        return sqlErrorCount.addAndGet(count);
    }

    public Long getSqlError() {
        return sqlErrorCount.get();
    }

    public Long addPhysicalReadRequestPerAtom(long count, long sqlLength) {
        this.sqlLength.addAndGet(sqlLength);
        return physicalReadRequestPerAtom.addAndGet(count);
    }

    public Long getPhysicalReadRequestPerAtom() {
        return physicalReadRequestPerAtom.get();
    }

    public Long addPhysicalWriteRequestPerAtom(long count, long sqlLength) {
        this.sqlLength.addAndGet(sqlLength);
        return physicalWriteRequestPerAtom.addAndGet(count);
    }

    public Long getPhysicalWriteRequestPerAtom() {
        return physicalWriteRequestPerAtom.get();
    }

    public void addRows(long row) {
        rows.addAndGet(row);
    }

    public List<Object> toObjects(String dbName, String appName, String groupName) {
        List<Object> obslist = new ArrayList<Object>();
        obslist.add(dbName);
        obslist.add(ipPort);
        obslist.add(appName);
        obslist.add(groupName);
        obslist.add(dbkey);
        obslist.add(physicalReadRequestPerAtom.get());
        obslist.add(physicalWriteRequestPerAtom.get());
        obslist.add(physicalReadRequestPerAtom.get() + physicalWriteRequestPerAtom.get());
        obslist.add(physicalReadTime.get());
        obslist.add(physicalWriteTime.get());
        obslist.add(physicalReadTime.get() + physicalWriteTime.get());
        obslist.add(connErrorCount);
        obslist.add(sqlErrorCount);
        obslist.add(sqlLength);
        obslist.add(rows);
        return obslist;
    }

    public void clear() {
        connErrorCount.set(0L);
        physicalReadRequestPerAtom.set(0L);
        physicalWriteRequestPerAtom.set(0L);
        physicalReadTime.set(0L);
        physicalWriteTime.set(0L);
        connErrorCount.set(0L);
        sqlErrorCount.set(0L);
        sqlLength.set(0L);
        rows.set(0L);
    }

    public String getIpPort() {
        return ipPort;
    }

    public void setIpPort(String ipPort) {
        this.ipPort = ipPort;
    }
}

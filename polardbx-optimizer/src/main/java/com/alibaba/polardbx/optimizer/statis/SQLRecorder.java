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

package com.alibaba.polardbx.optimizer.statis;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * SQL统计排序记录器
 * 
 * @author xianmao.hexm 2010-9-30 上午10:48:28
 */
public final class SQLRecorder {

    private static final Logger logger          = LoggerFactory.getLogger(SQLRecorder.class);

    private int                 index;
    // SQL执行时间的最小值
    private long                minValue;
    private int                 count;
    private SQLRecord[]         records;
    private int                 lastIndex;
    // 记录的SQL总数
    private final ReentrantLock lock;
    private volatile long       maxSizeThresold = 4 * 1024;
    private volatile long       slowSqlTime     = 1000;

    public SQLRecorder(int count){
        this(count, 16384, 1000);
    }

    public SQLRecorder(int count, int maxSizeThresold, long slowSqlTime){
        this.count = count;
        this.lastIndex = count - 1;
        this.maxSizeThresold = maxSizeThresold;
        this.slowSqlTime = slowSqlTime;
        this.records = new SQLRecord[count];
        this.lock = new ReentrantLock();
    }

    public SQLRecord[] getRecords() {
        return records;
    }

    /**
     * 检查当前的值能否进入排名
     */
    public boolean check(long value) {
        return (index < count) || (value > minValue);
    }

    public void add(SQLRecord record) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (index < count) {
                records[index++] = record;
                if (index == count) {
                    Arrays.sort(records);
                    minValue = records[0].executeTime;
                }
            } else {
                swap(record);
            }
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            for (int i = 0; i < count; i++) {
                records[i] = null;
            }
            index = 0;
            minValue = 0;
        } finally {
            lock.unlock();
        }
    }

    public void sort() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Arrays.sort(records);
            minValue = records[0].executeTime;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 交换元素位置并重新定义最小值
     */
    private void swap(SQLRecord record) {
        int x = find(record.executeTime, 0, lastIndex);
        switch (x) {
            case 0:
                break;
            case 1:
                minValue = record.executeTime;
                records[0] = record;
                break;
            default:
                --x;// 向左移动一格
                final SQLRecord[] records = this.records;
                for (int i = 0; i < x; i++) {
                    records[i] = records[i + 1];
                }
                records[x] = record;
                minValue = records[0].executeTime;
        }
    }

    /**
     * 定位v在当前范围内的排名
     */
    private int find(long v, int from, int to) {
        int x = from + ((to - from + 1) >> 1);
        if (v <= records[x].executeTime) {
            --x;// 向左移动一格
            if (from >= x) {
                return v <= records[from].executeTime ? from : from + 1;
            } else {
                return find(v, from, x);
            }
        } else {
            ++x;// 向右移动一格
            if (x >= to) {
                return v <= records[to].executeTime ? to : to + 1;
            } else {
                return find(v, x, to);
            }
        }
    }

    /**
     * 记录逻辑sql执行信息
     * 
     * @param endTime
     */
    public void recordSql(String sql, long startTime, String user, String host, String port, String schema, long affectRow,
                          long endTime, String uuid) {
        try {
            long time = endTime - startTime;
            if (this.check(time)) {

                SQLRecord record = new SQLRecord();
                record.statement = sql;
                record.startTime = System.currentTimeMillis();
                record.executeTime = time;
                record.schema = schema;
                record.user = user;
                record.host = host;
                record.port = port;
                record.affectRow = affectRow;
                record.traceId = uuid;
                this.add(record);
            }
        } catch (Throwable e) {
            logger.error("error when record sql", e);
        }
    }

    /**
     * 记录物理sql执行信息
     */
    public void recordSql(SQLRecord record) {
        try {
            if (this.check(record.executeTime)) {
                if (record.statement.length() > maxSizeThresold) {
                    StringBuilder newSql = new StringBuilder((int) maxSizeThresold + 3);
                    newSql.append(record.statement.substring(0, (int) maxSizeThresold));
                    newSql.append("...");
                    record.statement = newSql.toString();
                }

                this.add(record);
            }
        } catch (Throwable e) {
            logger.error("error when record sql", e);
        }
    }

    public long getMaxSizeThresold() {
        return maxSizeThresold;
    }

    public void setMaxSizeThresold(long maxSizeThresold) {
        this.maxSizeThresold = maxSizeThresold;
    }

    public void setCount(int count) {
        if (this.count != count) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                SQLRecord[] records = new SQLRecord[count];
                int start = (this.count > count ? this.count - count : 0);
                System.arraycopy(this.records, start, records, 0, this.count > count ? count : this.count);
                this.count = count;
                this.lastIndex = count - 1;
                this.records = records;
            } finally {
                lock.unlock();
            }
        }
    }

    public long getSlowSqlTime() {
        return slowSqlTime;
    }

    public void setSlowSqlTime(long slowSqlTime) {
        this.slowSqlTime = slowSqlTime;
    }

}

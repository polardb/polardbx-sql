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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class SqlRecorderTest {

    private Random random;

    @Before
    public void setUp() {
        this.random = new Random(System.currentTimeMillis());
    }

    @Test
    public void testSortedRecords() {
        for (int round = 0; round < 10; round++) {
            final int count = 10 + round;
            SQLRecorder sqlRecorder = new SQLRecorder(count);
            fillRandomRecorder(sqlRecorder, count * 2);
            SQLRecord[] records = sqlRecorder.getSortedRecords();
            Assert.assertEquals(sqlRecorder.size(), count);
            checkRecordsSorted(records, count);

            sqlRecorder.clear();
            final int countNotFull = count - 2;
            fillRandomRecorder(sqlRecorder, countNotFull);
            records = sqlRecorder.getSortedRecords();
            checkRecordsSorted(records, countNotFull);

            for (int i = countNotFull; i < count; i++) {
                Assert.assertNull("Expect null record here", records[i]);
            }
        }
    }

    @Test
    public void testExpireRecords() {
        for (int round = 0; round < 10; round++) {
            final int count = 10 + round;
            final long slowSqlExpireTime = 6000L;
            final long startInterval = 1000L;

            SQLRecorder sqlRecorder = new SQLRecorder(count);
            sqlRecorder.setSlowSqlExpireTime(slowSqlExpireTime);
            long curTime = 0;

            for (int i = 0; i < count; i++) {
                SQLRecord sqlRecord = new SQLRecord();
                sqlRecord.startTime = curTime + i * startInterval;
                sqlRecord.executeTime = (random.nextInt(10) + 1) * 2000L;
                sqlRecord.statement = "select " + i;
                sqlRecorder.recordSql(sqlRecord);
            }
            Assert.assertEquals(count, sqlRecorder.size());
            SQLRecord[] records = sqlRecorder.getSortedRecords();
            checkRecordsSorted(records, count);
            // 插入最新时间点的记录
            SQLRecord newestSqlRecord = new SQLRecord();
            newestSqlRecord.startTime = curTime + count * startInterval;
            newestSqlRecord.executeTime = (random.nextInt(10) + 1) * 2000L;
            newestSqlRecord.statement = "select " + count;
            sqlRecorder.recordSql(newestSqlRecord);

            int remainRecordCount = (int) (slowSqlExpireTime / startInterval + 1);
            Assert.assertEquals(String.format("Expected remain %d records in %d round ", remainRecordCount, round),
                remainRecordCount, sqlRecorder.size());
        }
    }

    private void fillRandomRecorder(SQLRecorder sqlRecorder, int count) {
        long curTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            SQLRecord sqlRecord = new SQLRecord();
            sqlRecord.startTime = curTime + i * 1000L;
            sqlRecord.executeTime = (random.nextInt(10) + 1) * 2000L;
            sqlRecord.statement = "select " + i;
            sqlRecorder.recordSql(sqlRecord);
        }
    }

    private void checkRecordsSorted(SQLRecord[] records, int count) {
        long executeTime = 0L;
        for (int i = 0; i < count; i++) {
            if (i != 0) {
                Assert.assertTrue("Records are not sorted", records[i].executeTime >= executeTime);
            }
            executeTime = records[i].executeTime;
        }
    }
}

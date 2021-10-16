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

package com.alibaba.polardbx.ccl;

import com.google.common.collect.Sets;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.service.impl.CclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclTriggerService;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author busu
 * date: 2020/11/2 11:19 上午
 */
public class CclConfigTest {

    ICclConfigService cclConfigService;

    @Before
    public void before() {
        cclConfigService = new CclConfigService() {
            @Override
            public void init(ICclService cclService, ICclTriggerService cclTriggerService) {
                this.cclService = cclService;
            }
        };

        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord = new CclRuleRecord();
        cclRuleRecord.id = "busu1";
        cclRuleRecord.queueSize = 1;
        cclRuleRecord.keywords = "[\"SELECT\",\"busu\"]";
        cclRuleRecord.parallelism = 1;
        cclRuleRecord.userName = "user";
        cclRuleRecord.clientIp = "%";
        cclRuleRecord.sqlType = "SELECT";
        cclRuleRecord.dbName = "*";
        cclRuleRecord.tableName = "*";
        sets.add(cclRuleRecord);

        CclRuleRecord cclRuleRecord1 = new CclRuleRecord();
        cclRuleRecord1.id = "busuReject";
        cclRuleRecord1.queueSize = 1;
        cclRuleRecord1.keywords = "[\"SELECT\",\"busu1\"]";
        cclRuleRecord1.parallelism = 1;
        cclRuleRecord1.userName = "user";
        cclRuleRecord1.clientIp = "%";
        cclRuleRecord1.sqlType = "SELECT";
        cclRuleRecord1.dbName = "*";
        cclRuleRecord1.tableName = "*";
        sets.add(cclRuleRecord1);

        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "busu3";
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.keywords = "[\"SELECT\",\"busu2\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = "*";
        cclRuleRecord2.tableName = "*";
        sets.add(cclRuleRecord2);

        ICclService cclService = new CclManagerTest.TestCclService(cclConfigService);
        cclConfigService.init(cclService, null);
        cclConfigService.refreshWithRules(sets);

    }

    @Test
    public void test1() {

        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "busutest1";
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.keywords = "[\"SELECT\",\"busu2\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = "*";
        cclRuleRecord2.tableName = "*";
        cclConfigService.refreshWithRules(Sets.newHashSet(cclRuleRecord2));
        List<CclRuleInfo> cclRuleInfos = cclConfigService.getCclRuleInfos();
        Assert.assertTrue(cclRuleInfos.size() == 1);
        Assert.assertTrue(cclRuleInfos.get(0).getCclRuleRecord().id.equals("busutest1"));

    }

    @Test
    public void test2() throws InterruptedException {
        int threadCnt = 10;
        int oneThreadTasks = 50;
        Thread[] threads = new Thread[threadCnt];
        AtomicLong version = new AtomicLong(0);
        for (int i = 0; i < threadCnt; ++i) {
            threads[i] = new Thread() {
                public void run() {
                    for (int j = 0; j < oneThreadTasks; ++j) {
                        CclRuleRecord cclRuleRecord1 = new CclRuleRecord();
                        cclRuleRecord1.id = RandomStringUtils.random(10);
                        cclRuleRecord1.queueSize = 1;
                        cclRuleRecord1.keywords = "[\"SELECT\",\"busu2\"]";
                        cclRuleRecord1.parallelism = 1;
                        cclRuleRecord1.userName = "user";
                        cclRuleRecord1.clientIp = "%";
                        cclRuleRecord1.sqlType = "SELECT";
                        cclRuleRecord1.dbName = "*";
                        cclRuleRecord1.tableName = "*";

                        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();

                        cclRuleRecord2.id = RandomStringUtils.random(10);
                        cclRuleRecord2.queueSize = 1;
                        cclRuleRecord2.keywords = "[\"SELECT\",\"busu2\"]";
                        cclRuleRecord2.parallelism = 1;
                        cclRuleRecord2.userName = "user";
                        cclRuleRecord2.clientIp = "%";
                        cclRuleRecord2.sqlType = "SELECT";
                        cclRuleRecord2.dbName = "*";
                        cclRuleRecord2.tableName = "*";
                        cclConfigService.refreshWithRules(Sets.newHashSet(cclRuleRecord1, cclRuleRecord2));
                        version.incrementAndGet();
                    }
                }
            };
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertTrue(cclConfigService.getCclRuleInfos().size() == 2);

        Assert.assertTrue(version.get() == (long) (threadCnt * oneThreadTasks));

    }

}

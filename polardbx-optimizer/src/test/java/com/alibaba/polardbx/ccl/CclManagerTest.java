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

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.optimizer.ccl.common.CclContext;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.service.impl.CclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.impl.CclService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclTriggerService;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author busu
 * date: 2020/10/30 2:13 下午
 */
public class CclManagerTest {

    ExecutionContext executionContext;
    String user = "user";
    String dbName = "dbName";
    String tableName = "tableName";
    String host = "192.168.0.10";
    String sqlType = "SELECT";

    final static String RUN = "RUN";
    final static String WAIT = "WAIT";
    final static String KILL = "KILL";
    final static String NONE = "NONE";

    TestCclService service;

    ICclConfigService cclConfigService;

    @Before
    public void before() {
        executionContext = new ExecutionContext();
        executionContext.setPrivilegeMode(true);

        MockExecutionPlan executionPlan = new MockExecutionPlan();
        String originSql = "SELECT a from busu where id = ?";
        PlanCache.CacheKey cacheKey = newCacheKey(originSql);
        executionPlan.setCacheKey(cacheKey);

        executionContext.setFinalPlan(executionPlan);

        Map<Integer, ParameterContext> paramMap =
            ImmutableMap.of(1, new ParameterContext(ParameterMethod.setInt, new Object[] {1, 1}));
        Parameters params = new Parameters(Lists.newArrayList(paramMap));
        executionContext.setParams(params);

        executionContext.setOriginSql(originSql);

        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setUser(user);
        privilegeContext.setSchema(dbName);
        privilegeContext.setHost(host);
        PrivilegeVerifyItem privilegeVerifyItem = new PrivilegeVerifyItem();
        privilegeVerifyItem.setDb(dbName);
        privilegeVerifyItem.setTable(tableName);
        privilegeVerifyItem.setPrivilegePoint(PrivilegePoint.valueOf("SELECT"));

        List<PrivilegeVerifyItem> privilegeVerifyItemList = Lists.newArrayList(privilegeVerifyItem);
        executionPlan.setPrivilegeVerifyItems(privilegeVerifyItemList);

        executionContext.setPrivilegeContext(privilegeContext);

        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord = new CclRuleRecord();

        cclRuleRecord.id = "1";
        cclRuleRecord.queueSize = 2;
        cclRuleRecord.keywords = "[\"SELECT\",\"busu\"]";
        cclRuleRecord.parallelism = 2;
        cclRuleRecord.userName = "user";
        cclRuleRecord.clientIp = "%";
        cclRuleRecord.sqlType = "SELECT";
        cclRuleRecord.dbName = "*";
        cclRuleRecord.tableName = "*";
        sets.add(cclRuleRecord);

        CclRuleRecord cclRuleRecord1 = new CclRuleRecord();
        cclRuleRecord1.id = "2";
        cclRuleRecord1.queueSize = 2;
        cclRuleRecord1.keywords = "[\"SELECT\",\"busu1\"]";
        cclRuleRecord1.parallelism = 2;
        cclRuleRecord1.userName = "user";
        cclRuleRecord1.clientIp = "%";
        cclRuleRecord1.sqlType = "SELECT";
        cclRuleRecord1.dbName = "*";
        cclRuleRecord1.tableName = "*";
        sets.add(cclRuleRecord1);

        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "3";
        cclRuleRecord2.queueSize = 2;
        cclRuleRecord2.keywords = "[\"SELECT\",\"busu2\"]";
        cclRuleRecord2.parallelism = 2;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = "*";
        cclRuleRecord2.tableName = "*";
        sets.add(cclRuleRecord2);

        CclRuleRecord cclRuleRecord3 = new CclRuleRecord();
        cclRuleRecord3.id = "4";
        cclRuleRecord3.queueSize = 2;
        cclRuleRecord3.keywords = "[\"SELECT\",\"busu3\"]";
        cclRuleRecord3.parallelism = 2;
        cclRuleRecord3.userName = "user";
        cclRuleRecord3.clientIp = "%";
        cclRuleRecord3.sqlType = "SELECT";
        cclRuleRecord3.dbName = "*";
        cclRuleRecord3.tableName = "*";
        sets.add(cclRuleRecord3);

        cclConfigService = new CclConfigService() {
            @Override
            public void init(ICclService cclService, ICclTriggerService cclTriggerService) {
                this.cclService = cclService;
            }
        };

        service = new TestCclService(cclConfigService);
        cclConfigService.init(service, null);
        cclConfigService.refreshWithRules(sets);

    }

    @Test
    public void test1() {
        long connId = 1;
        ExecutionContext executionContext1 = executionContext.copy();
        executionContext1.setConnId(connId);
        service.begin(executionContext1);
        Assert.assertEquals(RUN, service.getActionMap().get(connId));
        service.end(executionContext1);
        CclContext cclContext = executionContext1.getCclContext();
        CclRuleInfo cclRuleInfo = cclContext.getCclRule();
        Assert.assertTrue(cclRuleInfo.getRunningCount().get() == 0);
        Assert.assertTrue(cclRuleInfo.getStayCount().get() == 0);
        Assert.assertTrue(cclRuleInfo.getWaitQueue().size() == 0);
    }

    @Test
    public void test1a() {
        long connId = 1;
        ExecutionContext executionContext1 = executionContext.copy();
        executionContext1.setConnId(connId);
        executionContext1.setOriginSql("SELECT busu from busu1, busu2");
        service.begin(executionContext1);
        Assert.assertEquals(RUN, service.getActionMap().get(connId));
        service.end(executionContext1);
        CclContext cclContext = executionContext1.getCclContext();
        CclRuleInfo cclRuleInfo = cclContext.getCclRule();
        Assert.assertTrue(cclRuleInfo.getRunningCount().get() == 0);
        Assert.assertTrue(cclRuleInfo.getStayCount().get() == 0);
        Assert.assertTrue(cclRuleInfo.getWaitQueue().size() == 0);
    }

    @Test
    public void test2() {
        long connId = 1;
        ExecutionContext executionContext1 = executionContext.copy();
        executionContext1.setConnId(connId);
        service.begin(executionContext1);
        Assert.assertEquals(RUN, service.getActionMap().get(connId));
        //   service.end(executionContext1);
        CclContext cclContext = executionContext1.getCclContext();
        CclRuleInfo cclRuleInfo = cclContext.getCclRule();
        Assert.assertTrue(cclRuleInfo.getRunningCount().get() == 1);
        Assert.assertTrue(cclRuleInfo.getStayCount().get() == 1);
        Assert.assertTrue(cclRuleInfo.getWaitQueue().size() == 0);
    }

    @Test
    public void test3() {

        long connId1 = 1;
        ExecutionContext executionContext1 = executionContext.copy();
        executionContext1.setConnId(connId1);
        service.begin(executionContext1);

        long connId2 = 2;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu1 from dingfeng");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);

        Assert.assertEquals(RUN, service.getActionMap().get(connId1));
        Assert.assertEquals(RUN, service.getActionMap().get(connId2));

    }

    @Test
    public void test4() throws InterruptedException {
        long connId1 = 1;
        ExecutionContext executionContext1 = executionContext.copy();
        executionContext1.setConnId(connId1);
        service.begin(executionContext1);
        long connId2 = 2;
        Thread thread = new Thread() {
            public void run() {
                ExecutionContext executionContext2 = executionContext.copy();
                executionContext2.setConnId(connId2);
                service.begin(executionContext2);
            }
        };
        thread.start();
        for (int i = 0; i < 100; ++i) {
            Thread.sleep(1);
            if (WAIT.equals(service.getActionMap().get(connId2))) {
                break;
            }
        }
        service.end(executionContext1);
        thread.join();
        Assert.assertEquals(RUN, service.getActionMap().get(connId1));
        Assert.assertEquals(RUN, service.getActionMap().get(connId2));

    }

    @Test
    public void test5() {
        long connId2 = 2;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busuxxxx from dingfeng");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);

        Assert.assertEquals(NONE, service.getActionMap().get(connId2));
    }

    @Test
    public void test6() {

        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busuxxxx from dingfeng");
        executionContext2.setConnId(1);
        service.begin(executionContext2);

        ExecutionContext executionContext3 = executionContext.copy();
        executionContext3.setOriginSql("SELECT busuxxxx from dingfeng");
        executionContext3.setConnId(2);
        service.begin(executionContext3);

        ExecutionContext executionContext4 = executionContext.copy();
        executionContext4.setOriginSql("SELECT busuxxxx from dingfeng");
        executionContext4.setConnId(3);
        service.begin(executionContext4);

        Assert.assertEquals(NONE, service.getActionMap().get(3L));

    }

    @Test
    public void test7() throws InterruptedException {

        int threadCnt = 10;
        int oneThreadTasks = 100;
        Thread[] threads = new Thread[threadCnt];
        AtomicInteger atomicInteger = new AtomicInteger(0);
        String[] sqls = new String[] {
            "SELECT busu from dingfeng",
            "UPDATE table set value = 1",
            "INSERT INTO TABLE (a, b, c) values (1,2,3)",
            "DELETE FROM TABLE WHERE busu = 'dingfeng' ",
            "SELECT busu from busu2",
            "SELECT busu from busu3,busu2"
        };
        for (int i = 0; i < threadCnt; ++i) {
            threads[i] = new Thread() {
                private int connId = atomicInteger.incrementAndGet();

                public void run() {
                    for (int i = 0; i < 1000; ++i) {
                        for (String sql : sqls) {
                            ExecutionContext executionContext2 = executionContext.copy();
                            executionContext2.setOriginSql(sql);
                            executionContext2.setConnId(connId);

                            try {
                                service.begin(executionContext2);
                            } catch (Exception e) {

                            } finally {
                                service.end(executionContext2);
                            }

                        }
                    }
                }
            };
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (CclRuleInfo cclRuleInfo : cclConfigService.getCclRuleInfos()) {
            Assert.assertTrue(cclRuleInfo.getStayCount().get() == 0);
            Assert.assertTrue(cclRuleInfo.getRunningCount().get() == 0);
        }
    }

    @Test
    public void test8() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 2;
        cclRuleRecord2.keywords = "[\"SELECT\",\"busu2\"]";
        cclRuleRecord2.parallelism = 2;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3 from " + tableName);
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        Assert.assertEquals(NONE, service.getActionMap().get(connId2));
    }

    @Test
    public void test9() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 2;
        cclRuleRecord2.keywords = "[\"SELECT\",\"busu2\"]";
        cclRuleRecord2.parallelism = 2;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = "wrong dbName";
        cclRuleRecord2.tableName = tableName;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu2 from " + tableName);
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        Assert.assertEquals(NONE, service.getActionMap().get(connId2));
    }

    @Test
    public void test10() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 2;
        cclRuleRecord2.keywords = "[\"SELECT\",\"busu2\"]";
        cclRuleRecord2.parallelism = 2;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu2 from " + tableName);
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        Assert.assertEquals(RUN, service.getActionMap().get(connId2));
    }

    @Test(expected = TddlRuntimeException.class)
    public void test11() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"1\"]";
        cclRuleRecord2.parallelism = 0;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3 from where id in (?,?) ");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        Assert.assertEquals(KILL, service.getActionMap().get(connId2));
    }

    @Test(expected = TddlRuntimeException.class)
    public void test111() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"1\"]";
        cclRuleRecord2.parallelism = 0;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3 from where id in (?,?) ");
        executionContext2.setConnId(connId2);
        Exception ex = null;
        try {
            service.begin(executionContext2);
        } catch (Exception e) {
            ex = e;
        }
        Assert.assertTrue(ex != null);
        Assert.assertEquals(KILL, service.getActionMap().get(connId2));
        service.begin(executionContext2);
    }

    @Test(expected = TddlRuntimeException.class)
    public void test12() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"1\"]";
        cclRuleRecord2.parallelism = 0;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.waitTimeout = 1;
        cclRuleRecord2.queueSize = 1;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3 from where id in (?,?) ");
        executionContext2.setConnId(connId2);
        long startTime = System.nanoTime();
        try {
            service.begin(executionContext2);
        } catch (TddlRuntimeException e) {
            long duration = (System.nanoTime() - startTime) / 1000000;
            Assert.assertTrue(duration >= 1000);
            throw e;
        }
        Assert.assertTrue(false);
    }

    @Test
    public void test13() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"1\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.fastMatch = 1;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3 from where id in (?,?) ");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        service.end(executionContext2);
        service.begin(executionContext2);
        service.end(executionContext2);
        Assert.assertEquals(service.getCacheStats().get(3).intValue(), 0);
    }

    @Test
    public void test14() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"busu3\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.fastMatch = 1;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3, 1 as a from where id in (?,?) ");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        service.end(executionContext2);
        service.begin(executionContext2);
        service.end(executionContext2);
        Assert.assertEquals(service.getCacheStats().get(3).intValue(), 1);
    }

    @Test
    public void test15() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"kkk\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.fastMatch = 1;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3, 1 as a from where id in (?) ");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        service.end(executionContext2);
        service.begin(executionContext2);
        service.end(executionContext2);
        Assert.assertEquals(service.getCacheStats().get(1).intValue(), 1);
    }

    @Test
    public void test16() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"kkk\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.fastMatch = 1;
        cclRuleRecord2.clientIp = "127.0.0.1";
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3, 1 as a from where id in (?) ");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        service.end(executionContext2);
        service.begin(executionContext2);
        service.end(executionContext2);
        Assert.assertEquals(service.getCacheStats().get(0).intValue(), 1);
    }

    @Test
    public void test17() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"kkk\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.fastMatch = 1;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3, 1 as a from where id in (1)");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        service.end(executionContext2);
        service.begin(executionContext2);
        service.end(executionContext2);
        Assert.assertEquals(service.getCacheStats().get(1).intValue(), 1);
    }

    @Test
    public void test18() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"kkk\",\"1\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.fastMatch = 1;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.setOriginSql("SELECT busu3, 1 as a from where id in (?)");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        service.end(executionContext2);
        service.begin(executionContext2);
        service.end(executionContext2);
        Assert.assertEquals(service.getCacheStats().get(2).intValue(), 1);
    }

    @Test
    public void test19() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.keywords = "[\"kkk\",\"1\"]";
        cclRuleRecord2.parallelism = 1;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "127%";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.queueSize = 1;
        cclRuleRecord2.fastMatch = 1;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.getPrivilegeContext().setHost("127.0.0.1");
        executionContext2.setOriginSql("SELECT busu3, 1 as a from where id in (?)");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
        service.end(executionContext2);
        service.begin(executionContext2);
        service.end(executionContext2);
        Assert.assertEquals(service.getCacheStats().get(2).intValue(), 1);
    }

    @Test(expected = TddlRuntimeException.class)
    public void test20() {
        Set<CclRuleRecord> sets = Sets.newHashSet();
        CclRuleRecord cclRuleRecord2 = new CclRuleRecord();
        cclRuleRecord2.id = "test8";
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.parallelism = 0;
        cclRuleRecord2.userName = "user";
        cclRuleRecord2.clientIp = "%0.1";
        cclRuleRecord2.sqlType = "SELECT";
        cclRuleRecord2.dbName = dbName;
        cclRuleRecord2.tableName = tableName;
        cclRuleRecord2.queueSize = 0;
        cclRuleRecord2.fastMatch = 1;
        sets.add(cclRuleRecord2);
        cclConfigService.refreshWithRules(sets);
        long connId2 = 1;
        ExecutionContext executionContext2 = executionContext.copy();
        executionContext2.getPrivilegeContext().setHost("127.0.0.1");
        executionContext2.setOriginSql("SELECT busu3, 1 as a from where id in (?)");
        executionContext2.setConnId(connId2);
        service.begin(executionContext2);
    }

    @Test
    public void test21() {
        String sql = "select ?";
        PlanCache.CacheKey cacheKey = new PlanCache.CacheKey(dbName, sql, null, null, true, true);
        Assert.assertTrue(sql.hashCode() == cacheKey.getTemplateHash());
    }

    static class TestCclService extends CclService {

        Map<Long, String> actionMap = new ConcurrentHashMap<>();

        public TestCclService(ICclConfigService cclConfigService) {
            super(cclConfigService);
        }

        public Map<Long, String> getActionMap() {
            return actionMap;
        }

        @Override
        protected void doWait(ExecutionContext executionContext) {
//            System.out.println("doWait " + executionContext.getOriginSql());
            actionMap.put(executionContext.getConnId(), WAIT);
            super.doWait(executionContext);
        }

        @Override
        protected void doRun(ExecutionContext executionContext) {
//            System.out.println("doRun " + executionContext.getOriginSql());
            actionMap.put(executionContext.getConnId(), RUN);
            super.doRun(executionContext);
        }

        @Override
        protected void doKill(ExecutionContext executionContext) {
//            System.out.println("doKill " + executionContext.getOriginSql());
            actionMap.put(executionContext.getConnId(), KILL);
            super.doKill(executionContext);
        }

        @Override
        protected void doNone(ExecutionContext executionContext) {
//            System.out.println("doNone " + executionContext.getOriginSql());
            actionMap.put(executionContext.getConnId(), NONE);
        }

        @Override
        public void invalidateCclRule(CclRuleInfo cclRuleInfo) {

        }

        @Override
        public void clearCache() {

        }

    }

    static class MockExecutionPlan extends ExecutionPlan {

        PlanCache.CacheKey cacheKey;

        public MockExecutionPlan() {
            super(null, null, null);
        }

        public void setCacheKey(PlanCache.CacheKey cacheKey) {
            this.cacheKey = cacheKey;
        }

        public PlanCache.CacheKey getCacheKey() {
            return this.cacheKey;
        }

    }

    static PlanCache.CacheKey newCacheKey(String originSql) {
        try {
            PlanCache.CacheKey cacheKey = new PlanCache.CacheKey("", originSql, "", Lists.newArrayList(), true, true);
            return cacheKey;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

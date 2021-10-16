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

import com.google.common.collect.Lists;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import org.junit.Before;
import org.junit.Test;

/**
 * @author busu
 * date: 2020/10/30 11:43 上午
 */
public class CclRuleInfoTest {

    @Before
    public void before() {

    }

    @Test
    public void test1() {
        CclRuleRecord cclRuleRecord = new CclRuleRecord();
        cclRuleRecord.id = "ccl1";
        cclRuleRecord.sqlType = "SELECT";
        cclRuleRecord.dbName = "busu";
        cclRuleRecord.tableName = "busu";
        cclRuleRecord.userName = "busu";
        cclRuleRecord.clientIp = "%";
        cclRuleRecord.parallelism = 1;
        cclRuleRecord.keywords = "[\"keyword1\",\"keyword2\",\"keyword3\"]";
        cclRuleRecord.templateId = TStringUtil.int2FixedLenHexStr(12);
        cclRuleRecord.queueSize = 1;
        CclRuleInfo cclRuleInfo = CclRuleInfo.create(cclRuleRecord);
        Assert.assertTrue(cclRuleInfo.getRunningCount().get() == 0);
        Assert.assertTrue(cclRuleInfo.getStayCount().get() == 0);
        Assert.assertTrue(cclRuleInfo.getWaitQueue().size() == 0);
        Assert.assertTrue(cclRuleInfo.getCclRuntimeStat().killedCount.get() == 0);
        Assert.assertTrue(cclRuleInfo.getHost().getValue().equalsIgnoreCase("%"));
        Assert.assertTrue(cclRuleInfo.getKeywords().equals(Lists.newArrayList("keyword1", "keyword2", "keyword3")));
        Assert.assertTrue(cclRuleInfo.getTemplateId().equalsIgnoreCase(cclRuleRecord.templateId));
        Assert.assertTrue(cclRuleInfo.getSqlType() == PrivilegePoint.SELECT);
        Assert.assertTrue(cclRuleInfo.isNeedMatchDb() == true);
        Assert.assertTrue(cclRuleInfo.isNeedMatchTable() == true);
        Assert.assertTrue(cclRuleInfo.isEnabled() == false);
    }

    @Test
    public void test2() {
        CclRuleRecord cclRuleRecord = new CclRuleRecord();
        cclRuleRecord.id = "ccl1";
        cclRuleRecord.sqlType = "SELECT";
        cclRuleRecord.dbName = "busu";
        cclRuleRecord.tableName = "busu";
        cclRuleRecord.userName = "busu";
        cclRuleRecord.clientIp = "%";
        cclRuleRecord.parallelism = 1;
        cclRuleRecord.keywords = "[\"keyword1\",\"keyword2\",\"keyword3\"]";
        cclRuleRecord.queueSize = 1;
        CclRuleInfo cclRuleInfo = CclRuleInfo.create(cclRuleRecord);
        Assert.assertTrue(cclRuleInfo.getRunningCount().get() == 0);
        Assert.assertTrue(cclRuleInfo.getStayCount().get() == 0);
        Assert.assertTrue(cclRuleInfo.getWaitQueue().size() == 0);
        Assert.assertTrue(cclRuleInfo.getCclRuntimeStat().killedCount.get() == 0);
        Assert.assertTrue(cclRuleInfo.getHost().getValue().equalsIgnoreCase("%"));
        Assert.assertTrue(cclRuleInfo.getKeywords().equals(Lists.newArrayList("keyword1", "keyword2", "keyword3")));
        Assert.assertTrue(cclRuleInfo.getTemplateId() == null);
        Assert.assertTrue(cclRuleInfo.getSqlType() == PrivilegePoint.SELECT);
        Assert.assertTrue(cclRuleInfo.isNeedMatchDb() == true);
        Assert.assertTrue(cclRuleInfo.isNeedMatchTable() == true);
        Assert.assertTrue(cclRuleInfo.isEnabled() == false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test3() {
        CclRuleRecord cclRuleRecord = new CclRuleRecord();
        cclRuleRecord.id = "ccl1";
        cclRuleRecord.sqlType = "N/A";
        cclRuleRecord.dbName = "busu";
        cclRuleRecord.tableName = "busu";
        cclRuleRecord.userName = "busu";
        cclRuleRecord.clientIp = "%";
        cclRuleRecord.parallelism = 1;
        cclRuleRecord.keywords = "[\"keyword1\",\"keyword2\",\"keyword3\"]";
        cclRuleRecord.queueSize = 1;
        CclRuleInfo.create(cclRuleRecord);
    }

    @Test
    public void test4() {
        CclRuleRecord cclRuleRecord = new CclRuleRecord();
        cclRuleRecord.id = "ccl1";
        cclRuleRecord.sqlType = "SELECT";
        cclRuleRecord.dbName = "busu";
        cclRuleRecord.tableName = "busu";
        cclRuleRecord.userName = "busu";
        cclRuleRecord.clientIp = "127.0.1%";
        cclRuleRecord.parallelism = 1;
        cclRuleRecord.keywords = "[\"keyword1\",\"keyword2\",\"keyword3\"]";
        cclRuleRecord.queueSize = 1;
        CclRuleInfo cclRuleInfo = CclRuleInfo.create(cclRuleRecord);
        Assert.assertTrue(cclRuleInfo.getHostCommPrefixLen() == 7);
    }

    @Test
    public void test5() {
        CclRuleRecord cclRuleRecord = new CclRuleRecord();
        cclRuleRecord.id = "ccl1";
        cclRuleRecord.sqlType = "SELECT";
        cclRuleRecord.dbName = "busu";
        cclRuleRecord.tableName = "busu";
        cclRuleRecord.userName = "busu";
        cclRuleRecord.clientIp = "%";
        cclRuleRecord.parallelism = 1;
        cclRuleRecord.keywords = "[\"keyword1\",\"keyword2\",\"keyword3\"]";
        cclRuleRecord.queueSize = 1;
        CclRuleInfo cclRuleInfo = CclRuleInfo.create(cclRuleRecord);
        Assert.assertTrue(cclRuleInfo.getHostCommPrefixLen() == 0);
    }

    @Test
    public void test6() {
        CclRuleRecord cclRuleRecord = new CclRuleRecord();
        cclRuleRecord.id = "ccl1";
        cclRuleRecord.sqlType = "SELECT";
        cclRuleRecord.dbName = "busu";
        cclRuleRecord.tableName = "busu";
        cclRuleRecord.userName = "busu";
        cclRuleRecord.clientIp = "%.1";
        cclRuleRecord.parallelism = 1;
        cclRuleRecord.keywords = "[\"keyword1\",\"keyword2\",\"keyword3\"]";
        cclRuleRecord.queueSize = 1;
        CclRuleInfo cclRuleInfo = CclRuleInfo.create(cclRuleRecord);
        Assert.assertTrue(cclRuleInfo.getHostCommPrefixLen() == -1);
    }

}

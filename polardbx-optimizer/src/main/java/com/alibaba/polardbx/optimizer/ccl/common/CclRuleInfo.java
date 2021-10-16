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

package com.alibaba.polardbx.optimizer.ccl.common;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.privilege.Host;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author busu
 * date: 2020/10/29 1:38 下午
 */
@Data
public class CclRuleInfo<T> {

    public final static int RUNNING_INDEX = 0;
    public final static int WAITING_INDEX = 1;
    private final static char WILDCARD = '%';

    private final CclRuleRecord cclRuleRecord;

    private final Deque<T> waitQueue;

    private volatile int maxStayCount;

    /**
     * stayCount = waitingCount + runningCount
     */
    private final AtomicInteger stayCount;

    private final AtomicInteger runningCount;

    private final Host host;

    private final boolean needMatchDb;

    private final boolean needMatchTable;

    private final boolean needMatchUser;

    private final boolean needMatchHost;

    private final boolean normalHost;

    private final int hostCommPrefixLen;

    private final int hostCommSuffixLen;

    private volatile boolean enabled;

    private final List<String> keywords;
    private final int totalKeywordLen;

    private final String templateId;

    private final Set<Integer> templateIdSet;

    private final Map<Integer, Object> posParamMap;

    private final PrivilegePoint sqlType;

    private final CclRuntimeStat cclRuntimeStat;

    private final boolean reschedule;

    private final boolean queryMatch;

    public CclRuleInfo(CclRuleRecord cclRuleRecord, Host host, int maxStayCount, boolean needMatchDb,
                       boolean needMatchTable, boolean needMatchUser, boolean needMatchHost, boolean normalHost,
                       List<String> keywords, String templateId, Set<Integer> templateIdSet, PrivilegePoint sqlType,
                       Map<Integer, Object> posParamMap, boolean queryMatch) {
        this.cclRuleRecord = cclRuleRecord;
        this.waitQueue = new ConcurrentLinkedDeque<>();
        this.maxStayCount = maxStayCount;
        this.stayCount = new AtomicInteger(0);
        this.runningCount = new AtomicInteger(0);
        this.host = host;
        this.needMatchDb = needMatchDb;
        this.needMatchTable = needMatchTable;
        this.keywords = keywords;

        if (this.keywords != null) {
            this.totalKeywordLen = this.keywords.stream().mapToInt(String::length).sum();
        } else {
            totalKeywordLen = 0;
        }

        this.needMatchUser = needMatchUser;
        this.needMatchHost = needMatchHost;
        this.normalHost = normalHost;
        int hostCommPrefixLenTmp = -1;
        int hostCommSuffixLenTmp = -1;
        if (!normalHost && cclRuleRecord != null) {
            hostCommPrefixLenTmp = cclRuleRecord.clientIp.indexOf(WILDCARD);
            if (hostCommPrefixLenTmp != cclRuleRecord.clientIp.length() - 1) {
                hostCommPrefixLenTmp = -1;
            }
            hostCommSuffixLenTmp = StringUtils.reverse(cclRuleRecord.clientIp).indexOf(WILDCARD);
            if (hostCommSuffixLenTmp != cclRuleRecord.clientIp.length() - 1) {
                hostCommSuffixLenTmp = -1;
            }
        }
        this.hostCommPrefixLen = hostCommPrefixLenTmp;
        this.hostCommSuffixLen = hostCommSuffixLenTmp;

        this.templateId = templateId;
        this.sqlType = sqlType;
        this.templateIdSet = templateIdSet;
        this.cclRuntimeStat = new CclRuntimeStat();
        boolean reschedule = false;
        if (cclRuleRecord != null) {
            reschedule = cclRuleRecord.lightWait > 0;
        }
        this.reschedule = reschedule;
        this.posParamMap = posParamMap;
        this.queryMatch = queryMatch;
    }

    public static CclRuleInfo create(CclRuleRecord cclRuleRecord) {
        Host host = new Host(cclRuleRecord.clientIp);
        boolean needMatchDb = !StringUtils.equals(cclRuleRecord.dbName, "*");
        boolean needMatchTable = !StringUtils.equals(cclRuleRecord.tableName, "*");
        boolean needMatchHost = !StringUtils.equals(cclRuleRecord.clientIp, "%");
        boolean needMatchUser = cclRuleRecord.userName != null;
        boolean normalHost = !StringUtils.contains(cclRuleRecord.clientIp, "%");

        List<String> keywords = Lists.newArrayList();
        if (!StringUtils.isEmpty(cclRuleRecord.keywords)) {
            keywords = JSON.parseArray(cclRuleRecord.keywords, String.class);
        }
        String templateId = null;
        Set<Integer> templateIdSet = Sets.newHashSet();
        if (StringUtils.isNotBlank(cclRuleRecord.templateId)) {
            templateId = cclRuleRecord.templateId;
            String[] templateIdArray = StringUtils.split(templateId, ",");
            for (String tempalteIdStr : templateIdArray) {
                int templateIdInt = TStringUtil.hex2Int(tempalteIdStr);
                templateIdSet.add(templateIdInt);
            }
        }
        boolean queryMatch = false;
        if (StringUtils.isNotBlank(cclRuleRecord.queryTemplateId)) {
            int queryTemplateIdInt = TStringUtil.hex2Int(cclRuleRecord.queryTemplateId);
            templateIdSet.add(queryTemplateIdInt);
            queryMatch = true;
        }
        PrivilegePoint sqlType = PrivilegePoint.valueOf(cclRuleRecord.sqlType);
        if (sqlType == null) {
            throw new IllegalArgumentException("invalid sql type " + cclRuleRecord.sqlType);
        }
        int maxStayCount = cclRuleRecord.queueSize + cclRuleRecord.parallelism;

        Map<Integer, Object> posParamMap = null;
        if (StringUtils.isNotBlank(cclRuleRecord.params)) {
            posParamMap = JSON.parseObject(cclRuleRecord.params, Map.class);
        }

        return new CclRuleInfo(cclRuleRecord, host, maxStayCount, needMatchDb,
            needMatchTable, needMatchUser, needMatchHost, normalHost, keywords, templateId, templateIdSet,
            sqlType, posParamMap, queryMatch);

    }

    @Override
    public int hashCode() {
        if (cclRuleRecord != null) {
            return Objects.hashCode(cclRuleRecord.id);
        }

        //when cclRuleRecord is null. return the same hashCode of zero
        return 0;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (obj == null || !(obj instanceof CclRuleInfo)) {
            return false;
        }

        CclRuleInfo that = (CclRuleInfo) obj;
        if (this.cclRuleRecord == that.cclRuleRecord) {
            return true;
        }

        if (this.cclRuleRecord != null && that.cclRuleRecord != null
            && Objects.equals(this.cclRuleRecord.id, that.cclRuleRecord.id)) {
            return true;
        }

        return false;
    }

    public long getOrderValue() {
        return -cclRuleRecord.priority;
    }

    public static class CclRuntimeStat {
        public final AtomicLong matchCclRuleHitCount = new AtomicLong(0);
        public final AtomicLong totalMatchCclRuleCount = new AtomicLong(0);
        public final AtomicLong killedCount = new AtomicLong(0);
    }

}

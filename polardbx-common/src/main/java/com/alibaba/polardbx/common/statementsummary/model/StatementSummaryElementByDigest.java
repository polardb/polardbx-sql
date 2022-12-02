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

package com.alibaba.polardbx.common.statementsummary.model;

import lombok.Data;

/**
 * @author busu
 * date: 2021/11/4 11:27 上午
 */
@Data
public class StatementSummaryElementByDigest {
    private long beginTime;
    private long count;
    private long errorCount;
    private long maxAffectedRows;
    private long sumAffectedRows;
    private long maxTransTime;
    private long sumTransTime;
    private long maxResponseTime;
    private long sumResponseTime;
    private long sumPhysicalTime;
    private long maxPhysicalTime;
    private long sumPhysicalExecCount;
    private long maxPhysicalExecCount;
    private long maxParseCpuTime;
    private long sumParseCpuTime;
    private long maxExecPlanCpuTime;
    private long sumExecPlanCpuTime;
    private long maxPhyFetchRows;
    private long sumPhyFetchRows;
    private long firstSeen;
    private long lastSeen;

    public void clear() {
        beginTime = 0;
        count = 0;
        errorCount = 0;
        maxAffectedRows = 0;
        sumAffectedRows = 0;
        maxTransTime = 0;
        sumTransTime = 0;
        maxResponseTime = 0;
        sumResponseTime = 0;
        maxPhysicalTime = 0;
        sumPhysicalTime = 0;
        maxPhysicalExecCount = 0;
        sumPhysicalExecCount = 0;
        firstSeen = 0;
        lastSeen = 0;
        maxParseCpuTime = 0;
        sumParseCpuTime = 0;
        maxExecPlanCpuTime = 0;
        sumExecPlanCpuTime = 0;
        maxPhyFetchRows = 0;
        sumPhyFetchRows = 0;
    }

    public void merge(ExecInfo stat) {
        count++;
        //errorCount
        if (stat.getErrorCount() > 0) {
            errorCount += stat.getErrorCount();
        }
        //affectedRows
        if (stat.getAffectedRows() > maxAffectedRows) {
            maxAffectedRows = stat.getAffectedRows();
        }
        sumAffectedRows += stat.getAffectedRows();
        //transTime
        if (stat.getTransTime() > maxTransTime) {
            maxTransTime = stat.getTransTime();
        }
        sumTransTime += stat.getTransTime();
        //response time
        if (stat.getResponseTime() > maxResponseTime) {
            maxResponseTime = stat.getResponseTime();
        }
        sumResponseTime += stat.getResponseTime();
        //physical time
        long onePhysicalTime =
            stat.getPhysicalExecCount() == 0 ? 0 : stat.getPhysicalTime() / stat.getPhysicalExecCount();
        if (onePhysicalTime > maxPhysicalTime) {
            maxPhysicalTime = onePhysicalTime;
        }
        sumPhysicalTime += stat.getPhysicalTime();
        //physical sql count
        if (stat.getPhysicalExecCount() > maxPhysicalExecCount) {
            maxPhysicalExecCount = stat.getPhysicalExecCount();
        }
        sumPhysicalExecCount += stat.getPhysicalExecCount();
        //build plan cpu time
        if (stat.getParseTime() > maxParseCpuTime) {
            maxParseCpuTime = stat.getParseTime();
        }
        sumParseCpuTime += stat.getParseTime();
        //execute plan cpu time
        if (stat.getExecPlanCpuTime() > maxExecPlanCpuTime) {
            maxExecPlanCpuTime = stat.getExecPlanCpuTime();
        }
        sumExecPlanCpuTime += stat.getExecPlanCpuTime();
        //physical fetch rows
        if (stat.getPhyFetchRows() > maxPhyFetchRows) {
            maxPhyFetchRows = stat.getPhyFetchRows();
        }
        sumPhyFetchRows += stat.getPhyFetchRows();

        if (firstSeen == 0) {
            firstSeen = stat.getTimestamp();
        }
        lastSeen = stat.getTimestamp();
    }

    public void merge(StatementSummaryElementByDigest statementSummaryElementByDigest) {
        count += statementSummaryElementByDigest.getCount();
        errorCount += statementSummaryElementByDigest.getErrorCount();
        //affectedRows
        if (statementSummaryElementByDigest.getMaxAffectedRows() > maxAffectedRows) {
            maxAffectedRows = statementSummaryElementByDigest.getMaxAffectedRows();
        }
        sumAffectedRows += statementSummaryElementByDigest.getSumAffectedRows();
        //transTime
        if (statementSummaryElementByDigest.getMaxTransTime() > maxTransTime) {
            maxTransTime = statementSummaryElementByDigest.getMaxTransTime();
        }
        sumTransTime += statementSummaryElementByDigest.getSumTransTime();
        //response time
        if (statementSummaryElementByDigest.getMaxResponseTime() > maxResponseTime) {
            maxResponseTime = statementSummaryElementByDigest.getMaxResponseTime();
        }
        sumResponseTime += statementSummaryElementByDigest.getSumResponseTime();
        //physical time
        if (statementSummaryElementByDigest.getMaxPhysicalTime() > maxPhysicalTime) {
            maxPhysicalTime = statementSummaryElementByDigest.getMaxPhysicalTime();
        }
        sumPhysicalTime += statementSummaryElementByDigest.getSumPhysicalTime();
        //physical sql count
        if (statementSummaryElementByDigest.getMaxPhysicalExecCount() > maxPhysicalExecCount) {
            maxPhysicalExecCount = statementSummaryElementByDigest.getMaxPhysicalExecCount();
        }
        sumPhysicalExecCount += statementSummaryElementByDigest.getSumPhysicalExecCount();

        //build plan cpu time
        if (statementSummaryElementByDigest.getMaxParseCpuTime() > maxParseCpuTime) {
            maxParseCpuTime = statementSummaryElementByDigest.getMaxParseCpuTime();
        }
        sumParseCpuTime += statementSummaryElementByDigest.getSumParseCpuTime();
        //execute plan cpu time
        if (statementSummaryElementByDigest.getMaxExecPlanCpuTime() > maxExecPlanCpuTime) {
            maxExecPlanCpuTime = statementSummaryElementByDigest.getMaxExecPlanCpuTime();
        }
        sumExecPlanCpuTime += statementSummaryElementByDigest.getSumExecPlanCpuTime();
        //physical fetch rows
        if (statementSummaryElementByDigest.getMaxPhyFetchRows() > maxPhyFetchRows) {
            maxPhyFetchRows = statementSummaryElementByDigest.getMaxPhyFetchRows();
        }
        sumPhyFetchRows += statementSummaryElementByDigest.getSumPhyFetchRows();

        //first seen
        if (statementSummaryElementByDigest.getFirstSeen() < firstSeen) {
            firstSeen = statementSummaryElementByDigest.getFirstSeen();
        }
        //last seen
        if (statementSummaryElementByDigest.getLastSeen() > lastSeen) {
            lastSeen = statementSummaryElementByDigest.getLastSeen();
        }
    }

}
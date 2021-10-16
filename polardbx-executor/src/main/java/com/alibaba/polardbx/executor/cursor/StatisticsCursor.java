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

package com.alibaba.polardbx.executor.cursor;

import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;

public abstract class StatisticsCursor implements Cursor {

    protected OperatorStatistics statistics = new OperatorStatistics();

    protected boolean enableOperatorMetric;
    protected long startStatTimeNano = -1;

    protected long perRowTimeNano = 0;

    protected StatisticsCursor(boolean enableOperatorMetric) {
        this.enableOperatorMetric = enableOperatorMetric;
    }

    /**
     * the stat group of the plan which executor is current cursor.
     * <p>
     * <pre>
     *  Note:
     *      When the cursor is in apply subQuery or in scalar subQuery,
     *      the subQuery operator cpu stat will be ignore and
     *      the value of targetPlanStatGroup will be null.
     * So, must check null before use the value of targetPlanStatGroup
     *
     * <pre/>
     */
    protected RuntimeStatistics.OperatorStatisticsGroup targetPlanStatGroup = null;

    public OperatorStatistics getStatistics() {
        return statistics;
    }

    public RuntimeStatistics.OperatorStatisticsGroup getTargetPlanStatGroup() {
        return targetPlanStatGroup;
    }

    public void setTargetPlanStatGroup(RuntimeStatistics.OperatorStatisticsGroup targetPlanStatGroup) {
        this.targetPlanStatGroup = targetPlanStatGroup;
    }

    protected void startProcessStat() {
        if (perRowTimeNano <= 0) {
            startStatTimeNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }
    }

    protected void endProcessStat(long newRowCnt) {
        if (perRowTimeNano > 0) {
            //FIXME here don't much more cpu for cursor mode.
            statistics.addProcessDuration(perRowTimeNano);
        } else {
            long perRowTimeNano = ThreadCpuStatUtil.getThreadCpuTimeNano() - this.startStatTimeNano;
            if (perRowTimeNano > 0 && newRowCnt > 0) {
                this.perRowTimeNano = perRowTimeNano;
            }
            statistics.addProcessDuration(perRowTimeNano);
        }
        statistics.addRowCount(newRowCnt);
    }

    protected void startInitStat() {
        this.startStatTimeNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
    }

    protected void endInitStat() {
        statistics.addStartupDuration(ThreadCpuStatUtil.getThreadCpuTimeNano() - this.startStatTimeNano);
    }

    protected void startCloseStat() {
        this.startStatTimeNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
    }

    protected void endCloseStat() {
        statistics.addCloseDuration(ThreadCpuStatUtil.getThreadCpuTimeNano() - this.startStatTimeNano);
        if (this.targetPlanStatGroup != null && enableOperatorMetric) {
            RuntimeStatHelper.addAsyncTaskCpuTimeToParent(this.targetPlanStatGroup);
        }
    }

}

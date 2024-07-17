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

package com.alibaba.polardbx.optimizer.core.profiler;

import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.utils.thread.CpuCollector;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.core.profiler.memory.MemoryEstimation;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import org.apache.calcite.rel.RelNode;

/**
 * The abstract class for RuntimeStatistics , its implement,the
 * RuntimeStatistics is in tddl-executor
 *
 * @author chenghui.lch
 */
public abstract class RuntimeStat implements CpuCollector {

    protected static int metricLevel = MetricLevel.OPERATOR.metricLevel;

    public static void setEnableCpuProfile(boolean enableSqlProfileVal) {
        if (enableSqlProfileVal) {
            metricLevel = MetricLevel.OPERATOR.metricLevel;
        } else {
            metricLevel = MetricLevel.DEFAULT.metricLevel;
        }
    }

    public static int getMetricLevel() {
        return metricLevel;
    }

    public abstract MemoryEstimation getMemoryEstimation();

    public abstract CpuStat getCpuStat();

    public abstract MemoryPool getMemoryPool();

    public abstract RelNode getPlanTree();

    public abstract void setPlanTree(RelNode rel);

    public abstract void addPhySqlCount(long phySqlCount);

    public abstract void addPhySqlTimecost(long phySqlTc);

    public abstract void addPhyFetchRows(long phyRsRows);

    public abstract void addPhyAffectedRows(long phyAffectiveRow);

    public abstract void addPhyConnTimecost(long phyConnTimecost);

    public abstract void addColumnarSnapshotTimecost(long columnarSnapshotTimecost);

    public abstract void setSqlType(SqlType sqlType);

    public abstract void setRunningWithCpuProfile(boolean runningWithCpuProfile);

    public abstract boolean isRunningWithCpuProfile();

    public abstract void holdMemoryPool();
}

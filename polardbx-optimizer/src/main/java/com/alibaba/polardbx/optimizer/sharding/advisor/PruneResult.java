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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;

/**
 * Prune the search space in sacrifice of result quality
 *
 * @author shengyu
 */
public class PruneResult {
    long pruned;
    long unPruned;
    long step;
    double ratio;
    ParamManager paramManager;

    public PruneResult(ParamManager paramManager) {
        pruned = 0L;
        unPruned = 0L;
        step = 0L;
        ratio = 1D;
        this.paramManager = paramManager;
    }

    void incP() {
        pruned++;
    }

    void incU() {
        step++;
        unPruned++;
    }

    double getRatio() {
        while (step >= paramManager.getInt(ConnectionParams.SHARDING_ADVISOR_APPRO_THRESHOLD)) {
            ratio += Math.ceil((ratio - 0.95) * 2) * 0.1;
            step -= paramManager.getInt(ConnectionParams.SHARDING_ADVISOR_APPRO_THRESHOLD);
        }
        return ratio;
    }

    public String printResult(int level, ShardResult result) {
        if (AdvisorUtil.PRINT_MODE == AdvisorUtil.Mode.LAST && level != 0) {
            return printResult();
        }
        if (AdvisorUtil.PRINT_MODE == AdvisorUtil.Mode.EMPTY) {
            return printResult();
        }
        return result.printResult() + printResult();
    }

    String printResult() {
        StringBuilder sb = new StringBuilder();
        sb.append("unPruned: ").append(unPruned);
        sb.append(" pruned: ").append(pruned);
        sb.append(" ratio: ").append(ratio).append("\n");
        return sb.toString();
    }

}
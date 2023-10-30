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

package com.alibaba.polardbx.optimizer.partition.util;

import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.In;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StepExplainItem {
    public PartPrunedResult prunedResult;
    public String stepDesc = null;
    public PartKeyLevel partLevel;

    public boolean isSubPartStepOr = false;
    public boolean useSubPartByTemp = false;
    public List<PartitionPruneStep> targetSubSteps = new ArrayList<>();

    public List<Integer> prunePartSpecPosiList = new ArrayList<>();
    public List<Integer> parentSpecPosiList = new ArrayList<>();
    public Map<Integer, PartPrunedResult> partSpecPosiToPruneResultMap = new HashMap<>();

    public StepExplainItem() {
    }

}

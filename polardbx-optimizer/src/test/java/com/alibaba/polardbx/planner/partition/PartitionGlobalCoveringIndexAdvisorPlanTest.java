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

package com.alibaba.polardbx.planner.partition;

import com.alibaba.polardbx.optimizer.index.IndexAdvisor;
import com.alibaba.polardbx.planner.common.AdvisorTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author dylan
 */
public class PartitionGlobalCoveringIndexAdvisorPlanTest extends AdvisorTestCommon {

    public PartitionGlobalCoveringIndexAdvisorPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                                       String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        adviseType = IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX;
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PartitionGlobalCoveringIndexAdvisorPlanTest.class);
    }

}



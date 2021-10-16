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

package com.alibaba.polardbx.planner.construct;

import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.hint.HintPlanner4Test;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.sql.SqlNodeList;

/**
 * @author Eric Fu
 */
public abstract class BaseHintPlannerTest extends BasePlannerTest {

    public BaseHintPlannerTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        SqlNodeList astList = new FastsqlParser().parse(testSql);
        ExecutionPlan executionPlan = HintPlanner4Test.getInstance(appName).getPlan(astList.get(0));
        return RelUtils.toString(executionPlan.getPlan());
    }
}

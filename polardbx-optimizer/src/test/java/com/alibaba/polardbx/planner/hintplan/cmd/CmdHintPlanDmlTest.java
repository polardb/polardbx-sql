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

package com.alibaba.polardbx.planner.hintplan.cmd;

import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.alibaba.polardbx.planner.hintplan.index.ParameterizedHintTestCommon;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

/**
 * @author chenmo.cm
 */
@RunWith(EclipseParameterized.class)
public class CmdHintPlanDmlTest extends ParameterizedHintTestCommon {

    public CmdHintPlanDmlTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(CmdHintPlanDmlTest.class);
    }
}

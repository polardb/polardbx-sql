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

package com.alibaba.polardbx.planner.subqueryplan;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

/**
 * @author chenghui.lch 2018年1月4日 下午1:44:54
 * @since 5.0.0
 */
public class DerivedSubQueryPlanTest extends PlanTestCommon {

    public DerivedSubQueryPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(DerivedSubQueryPlanTest.class);
    }

}

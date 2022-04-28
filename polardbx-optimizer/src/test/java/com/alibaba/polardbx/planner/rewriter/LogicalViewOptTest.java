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

package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * optimize logicalView's relNode tree, simplify the sql sent to DN.
 * unwrap subQueries by making join operations adjacent in the tree
 *
 * @author shengyu
 */
public class LogicalViewOptTest extends PlanTestCommon {
    public LogicalViewOptTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(LogicalViewOptTest.class);
    }
}
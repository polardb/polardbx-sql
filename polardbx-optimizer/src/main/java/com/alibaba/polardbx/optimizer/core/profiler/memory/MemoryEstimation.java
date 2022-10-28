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

package com.alibaba.polardbx.optimizer.core.profiler.memory;

import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.parse.visitor.AstStructStats;

/**
 * @author chenghui.lch
 */
public class MemoryEstimation {

    public long sqlLength = 0;
    public long sqlCommaCount = 0;
    public long parameterizedSqlLen = 0;
    public long logicalParamsCount = 0;
    public long logicalParamsSize = 0;
    public long estimatedFastAstSize = 0;
    public long estimatedSqlNodeSize = 0;
    public long estimatedRelNodeSize = 0;
    public SqlType sqlType = null;
    public AstStructStats astStructStats = new AstStructStats();
    public PlanMemEstimation cachedPlanMemEstimation = null;

    public MemoryEstimation() {
    }

    public void reset() {
        sqlLength = 0;
        sqlCommaCount = 0;
        parameterizedSqlLen = 0;
        logicalParamsCount = 0;
        logicalParamsSize = 0;
        estimatedFastAstSize = 0;
        estimatedSqlNodeSize = 0;
        estimatedRelNodeSize = 0;
        astStructStats.reset();
    }

}

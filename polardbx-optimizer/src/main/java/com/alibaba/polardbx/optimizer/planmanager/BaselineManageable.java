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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

import javax.sql.DataSource;

public interface BaselineManageable {
    void forceClear(Integer baselineId);

    void forceClearAll();

    void forceLoad(Integer baselineId);

    void forceLoadAll();

    void forceValidateAll();

    void forceValidate(Integer baselineId);

    BaselineInfo createBaselineInfo(String parameterizedSql, SqlNode ast, ExecutionContext ec);

    PlanInfo createPlanInfo(String planJsonString, RelNode plan, int baselinInfoId, String traceId,
                            String origin, SqlNode ast,
                            ExecutionContext executionContext);

    void updateBaseline(BaselineInfo otherBaselineInfo);

    void deleteBaseline(int baselineInfoId, String parameterSql);

    void deleteBaseline(int baselineInfoId, String parameterSql, int planInfoId);

    void updatePlanInfo(PlanInfo planInfo, String parameterSql, int originPlanId);

    boolean checkBaselineHashCodeValid(BaselineInfo baselineInfo, PlanInfo planInfo);

    void forcePersist(Integer baselineId);

    void forcePersistAll();
}

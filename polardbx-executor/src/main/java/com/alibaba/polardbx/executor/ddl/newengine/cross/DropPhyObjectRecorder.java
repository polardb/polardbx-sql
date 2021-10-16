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

package com.alibaba.polardbx.executor.ddl.newengine.cross;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

public class DropPhyObjectRecorder extends GenericPhyObjectRecorder {

    public DropPhyObjectRecorder(RelNode physicalPlan, ExecutionContext executionContext) {
        super(physicalPlan, executionContext);
    }

    @Override
    protected boolean checkIfPhyObjectDone() {
        boolean phyObjectDone = super.checkIfPhyObjectDone();
        return ddlContext.getState() == DdlState.ROLLBACK_RUNNING ? !phyObjectDone : phyObjectDone;
    }

}

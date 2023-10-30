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

import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import org.apache.calcite.rel.RelNode;

/**
 * The new and old DDL engines will coexist until all DDL related
 * components/functions migrate to new DDL engine.
 * <p>
 * A DDL request may be executed via new or old DDL engine during
 * coexistence, but note that:
 * - it can't run on both of DDL engines at the same time.
 * - there is only one job executed via one of DDL engines against
 * a database object at the same time.
 * <p>
 * Therefore, we need this cross-engine validator to check if there
 * is any job queued/ongoing/fenced on one DDL engine when trying
 * to start a new job on another DDL engine.
 */
public class CrossEngineValidator {

    private static final String QUEUED = "queued";
    private static final String ONGOING = "ongoing";
    private static final String FENCED = "fenced";

    public static boolean isDDLSupported(ExecutionContext executionContext) {
        return executionContext.getDdlContext() != null;
    }

    public static boolean isJobInterrupted(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        return ddlContext != null ? ddlContext.isInterrupted() : true;
    }

    public static GenericPhyObjectRecorder getPhyObjectRecorder(RelNode physicalPlan,
                                                                ExecutionContext executionContext) {
        if (physicalPlan != null && physicalPlan instanceof PhyDdlTableOperation) {
            switch (((PhyDdlTableOperation) physicalPlan).getKind()) {
            case CREATE_TABLE:
                return new CreatePhyObjectRecorder(physicalPlan, executionContext);
            case DROP_TABLE:
                return new DropPhyObjectRecorder(physicalPlan, executionContext);
            case RENAME_TABLE:
                return new RenameTablePhyObjectRecorder(physicalPlan, executionContext);
            case CREATE_INDEX:
            case DROP_INDEX:
            case ALTER_TABLE:
            default:
                return new AlterTablePhyObjectRecorder(physicalPlan, executionContext);
            }
        } else {
            return new AlterTablePhyObjectRecorder(physicalPlan, executionContext);
        }
    }

}

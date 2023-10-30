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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.util.Set;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.COLON;
import static com.alibaba.polardbx.common.ddl.newengine.DdlState.isRollBackRunning;

public class AlterTablePhyObjectRecorder extends GenericPhyObjectRecorder {

    private static final String BEFORE_PHY_DDL = "false";
    private static final String AFTER_PHY_DDL = "true";

    private String hashcodeBeforePhyDdl = null;

    public AlterTablePhyObjectRecorder(RelNode physicalPlan, ExecutionContext executionContext) {
        super(physicalPlan, executionContext);
    }

    @Override
    protected boolean checkIfPhyObjectDone() {
        DdlHelper.waitUntilPhyDDLDone(schemaName, groupName, phyTableName, ddlContext.getTraceId());

        String keyPrefix = genPhyObjectInfoKey();
        Set<String> phyObjectsDone = phyDdlExecutionRecord.getPhyObjectsDone();

        String keyBefore = keyPrefix + BEFORE_PHY_DDL;
        String phyObjectDoneBefore =
            phyObjectsDone.stream().filter(obj -> 0 == obj.indexOf(keyBefore)).findFirst().orElse(null);

        String keyAfter = keyPrefix + AFTER_PHY_DDL;
        String phyObjectDoneAfter =
            phyObjectsDone.stream().filter(obj -> 0 == obj.indexOf(keyAfter)).findFirst().orElse(null);

        boolean isPhyObjectDone;
        if (TStringUtil.isBlank(phyObjectDoneBefore) && TStringUtil.isBlank(phyObjectDoneAfter)) {
            String phyObjectInfoForDebug;

            if (isRollBackRunning(ddlContext.getState())) {
                isPhyObjectDone = true;
                phyObjectInfoForDebug = keyPrefix;
            } else {
                isPhyObjectDone = false;

                String phyObjectInfo = genPhyObjectInfo(false);
                phyObjectInfoForDebug = phyObjectInfo;

                String[] currentParts = phyObjectInfo.split(COLON);
                hashcodeBeforePhyDdl = currentParts[3];

                recordObjectNormal(phyObjectInfo, false);
            }

            printDebugInfo("AlterTablePhyObjectRecorder.checkIfPhyObjectDone() - " + isPhyObjectDone,
                phyDdlExecutionRecord, phyObjectInfoForDebug);
        } else {
            isPhyObjectDone = checkAndCompensate(phyObjectDoneBefore, phyObjectDoneAfter);
        }

        return isPhyObjectDone;
    }

    private boolean checkAndCompensate(String phyObjectDoneBefore, String phyObjectDoneAfter) {
        boolean isPhyObjectDone;
        String phyObjectInfoForDebug;

        if (TStringUtil.isBlank(phyObjectDoneAfter)) {
            // Only the hashcode record before physical DDL exists, so we
            // should check if the physical DDL has been actually done.
            String phyObjectInfo = genPhyObjectInfo(true);
            String[] currentParts = phyObjectInfo.split(COLON);
            String currentHashcode = currentParts[3];

            String[] partsBefore = phyObjectDoneBefore.split(COLON);
            String hashcodeBefore = partsBefore[3];

            phyObjectInfoForDebug = phyObjectDoneBefore;

            if (isRollBackRunning(ddlContext.getState())) {
                // Rolling back
                isPhyObjectDone = TStringUtil.equalsIgnoreCase(currentHashcode, hashcodeBefore);
                if (isPhyObjectDone) {
                    recordObjectRollback(false);
                } else {
                    hashcodeBeforePhyDdl = currentHashcode;
                    // For subsequent rollback to decrease.
                    phyDdlExecutionRecord.increasePhyObjsDone();
                }
            } else {
                // Recovering
                isPhyObjectDone = !TStringUtil.equalsIgnoreCase(currentHashcode, hashcodeBefore);
                if (isPhyObjectDone) {
                    phyObjectInfoForDebug = phyObjectInfo;
                    recordObjectNormal(phyObjectInfo, true);
                } else {
                    hashcodeBeforePhyDdl = hashcodeBefore;
                }
            }
        } else {
            // We think that the physical DDL has been done as long as
            // the hashcode record after physical DDL exists.
            isPhyObjectDone = !isRollBackRunning(ddlContext.getState());
            if (!isPhyObjectDone) {
                String[] partsAfter = phyObjectDoneAfter.split(COLON);
                hashcodeBeforePhyDdl = partsAfter[3];
            }
            phyObjectInfoForDebug = phyObjectDoneAfter;
        }

        printDebugInfo("AlterTablePhyObjectRecorder.checkIfPhyObjectDone():check - " + isPhyObjectDone,
            phyDdlExecutionRecord, phyObjectInfoForDebug);

        return isPhyObjectDone;
    }

    @Override
    protected boolean checkIfPhyObjectDoneByHashcode() {
        int delay = executionContext.getParamManager().getInt(ConnectionParams.GET_PHY_TABLE_INFO_DELAY);
        String hashcodeAfterDdl = DdlHelper.genHashCodeForPhyTableDDL(schemaName, groupName, phyTableName, delay);
        return hashcodeBeforePhyDdl != null && !TStringUtil.equalsIgnoreCase(hashcodeBeforePhyDdl, hashcodeAfterDdl);
    }

    @Override
    protected void recordObjectNormal() {
        recordObjectNormal(true);
    }

    protected void recordObjectNormal(boolean afterPhyDdl) {
        String phyObjectInfo = genPhyObjectInfo(afterPhyDdl);
        recordObjectNormal(phyObjectInfo, afterPhyDdl);
    }

    @Override
    protected void recordObjectRollback() {
        resetPhyObjectsDone(genPhyObjectInfoKey(), true);
    }

    protected void recordObjectRollback(boolean afterPhyDdl) {
        resetPhyObjectsDone(genPhyObjectInfoKey(), afterPhyDdl);
    }

    @Override
    protected void removePhyObjectDone(String phyObjectInfo, boolean afterPhyDdl) {
        phyDdlExecutionRecord.getPhyObjectsDone().removeIf(obj -> 0 == obj.indexOf(phyObjectInfo));
        if (afterPhyDdl) {
            phyDdlExecutionRecord.decreasePhyObjsDone();
        }
    }

    protected String genPhyObjectInfoKey() {
        return genPhyObjectInfo() + COLON;
    }

    protected String genPhyObjectInfo(boolean afterPhyDdl) {
        int delay = executionContext.getParamManager().getInt(ConnectionParams.GET_PHY_TABLE_INFO_DELAY);
        return DdlHelper.genPhyTableInfoWithHashcode(schemaName, groupName, phyTableName, afterPhyDdl, delay);
    }

}

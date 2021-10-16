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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.util.Set;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.COLON;

public class AlterTablePhyObjectRecorder extends GenericPhyObjectRecorder {

    public AlterTablePhyObjectRecorder(RelNode physicalPlan, ExecutionContext executionContext) {
        super(physicalPlan, executionContext);
    }

    @Override
    protected boolean checkIfPhyObjectDone() {
        Set<String> phyObjectsDone = phyDdlExecutionRecord.getPhyObjectsDone();

        DdlHelper.waitUntilPhyDdlDone(schemaName, physicalDdlPlan.getDbIndex(), executionContext.getTraceId());

        // Currently physical object info
        String phyObjectInfo = genPhyObjectInfo(false);
        String[] objectParts = phyObjectInfo.split(COLON);
        assert 4 == objectParts.length;

        // Physical object already done
        String keyInfo = objectParts[0] + COLON + objectParts[1] + COLON;
        String phyObjectDone =
            phyObjectsDone.stream().filter(object -> 0 == object.indexOf(keyInfo)).findFirst().orElse(null);

        if (phyObjectDone != null) {
            String[] doneParts = phyObjectDone.split(COLON);
            assert 4 == doneParts.length;
            return compareHashCodeAndProbe(objectParts, doneParts);
        } else if (ddlContext.getState() == DdlState.ROLLBACK_RUNNING) {
            return true;
        } else {
            recordObjectNormal(phyObjectInfo, false);
            return false;
        }
    }

    private boolean compareHashCodeAndProbe(String[] objectParts, String[] doneParts) {
        String groupName = objectParts[0];
        String phyTableName = objectParts[1];

        String currentHashCode = objectParts[2];
        String recordedHashCode = doneParts[2];
        String recordedProbe = doneParts[3];

        if (TStringUtil.equalsIgnoreCase(currentHashCode, recordedHashCode)
            && TStringUtil.equalsIgnoreCase(recordedProbe, Boolean.TRUE.toString())) {
            // If current hash code is the same as recorded after
            // the physical DDL was done (TRUE), then it has been
            // done and recorded as well, so skip it.
            return ddlContext.getState() != DdlState.ROLLBACK_RUNNING;
        } else if (!TStringUtil.equalsIgnoreCase(currentHashCode, recordedHashCode)
            && TStringUtil.equalsIgnoreCase(recordedProbe, Boolean.FALSE.toString())) {
            // If current hash code is different from recorded before
            // the physical DDL was done (FALSE), then means that the
            // physical DDL is already been done (asynchronously) even
            // if not recorded, so we can record and skip it.
            if (ddlContext.getState() == DdlState.ROLLBACK_RUNNING) {
                return false;
            } else {
                String phyObjectInfo = genPhyObjectInfo(true);
                recordObjectNormal(phyObjectInfo, true);
                return true;
            }
        } else if (!TStringUtil.equalsIgnoreCase(currentHashCode, recordedHashCode)
            && TStringUtil.equalsIgnoreCase(recordedProbe, Boolean.TRUE.toString())) {
            // If current hash code is different from recorded after
            // the physical DDL was done (TRUE), then means that the
            // physical table has been changed again unexpectedly,
            // so we have to throw an exception to report it to user.
            StringBuilder buf = new StringBuilder();
            buf.append("the physical object '" + groupName + "." + phyTableName);
            buf.append(" has been changed, please use 'CHECK TABLE ").append(ddlContext.getObjectName());
            buf.append("' to check the consistency of the logical table. ");
            buf.append("If the check result is OK or just contains a warning to prompt ");
            buf.append("a PENDING job, then you can use 'REMOVE DDL ").append(ddlContext.getJobId());
            buf.append("' to safely remove the job, otherwise you may need to ");
            buf.append("change the inconsistent physical table manually or ask for support.");
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, buf.toString());
        } else {
            // If current hash code is the same as recorded before
            // the physical DDL was done (FALSE), then let's do it.
            // However, we should decrease the number if we load
            // and calculate all records previously.
            if (ddlContext.getState() == DdlState.ROLLBACK_RUNNING) {
                String phyObjectInfo = genPhyObjectInfo(false);
                recordObjectRollback(phyObjectInfo);
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    protected String genPhyObjectInfo(boolean afterPhyDdl) {
        return DdlHelper.genPhyTableInfo(schemaName, physicalDdlPlan, true, afterPhyDdl, ddlContext);
    }

    @Override
    protected void removePhyObjectDone(String phyObjectInfo, Set<String> phyObjectsDone) {
        final String[] objectParts = phyObjectInfo.split(COLON);
        assert 4 == objectParts.length;
        // Remove target group & table.
        phyObjectsDone.removeIf(object -> 0 == object.indexOf(objectParts[0] + COLON + objectParts[1] + COLON));
    }

    @Override
    protected void recordObjectNormal(String phyObjectInfo, boolean withProgress) {
        if (withProgress) {
            synchronized (AlterTablePhyObjectRecorder.class) {
                DdlJobManagerUtils.reloadPhyTablesDone(phyDdlExecutionRecord);
                Set<String> phyObjectsDone = phyDdlExecutionRecord.getPhyObjectsDone();

                String newPhyObjectsDone =
                    DdlHelper.overwritePhyTablesDone(phyObjectInfo, phyObjectsDone, executionContext);

                if (TStringUtil.isNotEmpty(newPhyObjectsDone)) {
                    phyDdlExecutionRecord.increasePhyObjsDone();
                    DdlJobManagerUtils.resetPhyTablesDone(phyDdlExecutionRecord, newPhyObjectsDone, true);
                }
            }
        } else {
            DdlJobManagerUtils.appendPhyTableDone(phyDdlExecutionRecord, phyObjectInfo, false);
        }
    }

}

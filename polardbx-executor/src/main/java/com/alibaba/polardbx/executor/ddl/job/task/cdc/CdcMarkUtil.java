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

package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_MARK_SQL_MODE;

/**
 * created by ziyang.lb
 **/
public class CdcMarkUtil {

    public static Map<String, Object> buildExtendParameter(ExecutionContext executionContext) {
        Map<String, Object> parameter = executionContext.getExtraCmds();
        final Map<String, Object> extraVariables = executionContext.getExtraServerVariables();
        final Map<String, Object> serverVariables = executionContext.getServerVariables();
        if (null != extraVariables && extraVariables.containsKey("polardbx_server_id")) {
            Object serverId = extraVariables.get("polardbx_server_id");
            parameter.put("polardbx_server_id", serverId);
        }
        if (executionContext.getDdlContext() != null && executionContext.getDdlContext().getSqlMode() != null) {
            parameter.put(CDC_MARK_SQL_MODE, executionContext.getDdlContext().getSqlMode());
        }
        boolean notIgnoreGSI = false;
        if (executionContext.getDdlContext() != null) {
            notIgnoreGSI = !executionContext.getDdlContext().isIgnoreCdcGsiMark();
        }
        parameter.put(ICdcManager.NOT_IGNORE_GSI_JOB_TYPE_FLAG, notIgnoreGSI);
        final String uniqueChecksFlag = "unique_checks";
        if (null != serverVariables && serverVariables.containsKey(uniqueChecksFlag)) {
            parameter.put("unique_checks", serverVariables.get(uniqueChecksFlag));
        }

        if (null != serverVariables && serverVariables.containsKey(ConnectionParams.FOREIGN_KEY_CHECKS.getName())) {
            parameter.put(ConnectionParams.FOREIGN_KEY_CHECKS.getName(),
                serverVariables.get(ConnectionParams.FOREIGN_KEY_CHECKS.getName()));
        }

        parameter.put(ICdcManager.CDC_ORIGINAL_DDL, "");
        if (isUseOriginalDDL(executionContext)) {
            parameter.put(ICdcManager.CDC_ORIGINAL_DDL, executionContext.getDdlContext().getDdlStmt());
        } else if (isUseFkOriginalDDL(executionContext)) {
            parameter.put(ICdcManager.CDC_ORIGINAL_DDL, executionContext.getDdlContext().getForeignKeyOriginalSql());
        }
        return parameter;
    }

    private static boolean isUseOriginalDDL(ExecutionContext executionContext) {
        Map<String, Object> parameter = executionContext.getExtraCmds();
        String useOriginalDDL = (String) parameter.get(ICdcManager.USE_ORGINAL_DDL);

        if (executionContext.getDdlContext() == null ||
            StringUtils.isEmpty(
                executionContext.getDdlContext().getDdlStmt())) {
            return false;
        }

        return StringUtils.equalsIgnoreCase("true", useOriginalDDL);
    }

    private static boolean isUseFkOriginalDDL(ExecutionContext executionContext) {
        Map<String, Object> parameter = executionContext.getExtraCmds();
        String foreignKeysDdl = (String) parameter.get(ICdcManager.FOREIGN_KEYS_DDL);

        if (executionContext.getDdlContext() == null ||
            StringUtils.isEmpty(
                executionContext.getDdlContext().getDdlStmt())) {
            return false;
        }

        return StringUtils.equalsIgnoreCase("true", foreignKeysDdl);
    }
}

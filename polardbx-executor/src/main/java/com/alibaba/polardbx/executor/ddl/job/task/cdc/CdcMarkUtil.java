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
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_MARK_SQL_MODE;
import static com.alibaba.polardbx.common.cdc.ICdcManager.DDL_ID;
import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;
import static com.alibaba.polardbx.common.cdc.ICdcManager.POLARDBX_SERVER_ID;

/**
 * created by ziyang.lb
 **/
public class CdcMarkUtil {

    public static Map<String, Object> buildExtendParameter(@NotNull ExecutionContext executionContext) {
        String originalDdl = Optional.ofNullable(executionContext.getDdlContext())
            .map(c -> {
                if (StringUtils.isNotBlank(c.getCdcRewriteDdlStmt())) {
                    return c.getCdcRewriteDdlStmt();
                } else {
                    return c.getDdlStmt();
                }
            }).orElse("");
        return buildExtendParameter(executionContext, originalDdl);
    }

    public static Map<String, Object> buildExtendParameter(ExecutionContext executionContext, String originalDdl) {
        Map<String, Object> parameter = executionContext.getExtraCmds();
        final Map<String, Object> extraVariables = executionContext.getExtraServerVariables();
        final Map<String, Object> serverVariables = executionContext.getServerVariables();
        if (null != extraVariables && extraVariables.containsKey(POLARDBX_SERVER_ID)) {
            Object serverId = extraVariables.get(POLARDBX_SERVER_ID);
            parameter.put(POLARDBX_SERVER_ID, serverId);
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
        if (isUseFkOriginalDDL(executionContext)) {
            parameter.put(ICdcManager.CDC_ORIGINAL_DDL, executionContext.getDdlContext().getForeignKeyOriginalSql());
        } else if (isUseOriginalDDL(executionContext)) {
            // Has to remove USE_DDL_VERSION_ID flag from ExecutionContext, in case corrupt following ddl statements
            final Long ddlVersionId = getAndRemoveDdlVersionId(executionContext);
            if (isVersionIdInitialized(ddlVersionId)) {
                originalDdl = buildVersionIdHint(ddlVersionId) + originalDdl;
                parameter.put(DDL_ID, ddlVersionId);
            }

            parameter.put(ICdcManager.CDC_ORIGINAL_DDL, originalDdl);
        }
        return parameter;
    }

    private static Long getAndRemoveDdlVersionId(ExecutionContext executionContext) {
        if (executionContext.getExtraCmds().containsKey(ICdcManager.USE_DDL_VERSION_ID)) {
            final String ddlVersionId = executionContext
                .getExtraCmds()
                .remove(ICdcManager.USE_DDL_VERSION_ID)
                .toString();
            return DynamicConfig.parseValue(ddlVersionId, Long.class, DEFAULT_DDL_VERSION_ID);
        }
        return DEFAULT_DDL_VERSION_ID;
    }

    private static boolean isUseOriginalDDL(ExecutionContext executionContext) {
        Map<String, Object> parameter = executionContext.getExtraCmds();
        String useOriginalDDL = (String) parameter.get(ICdcManager.USE_ORIGINAL_DDL);

        if (executionContext.getDdlContext() == null ||
            StringUtils.isEmpty(executionContext.getDdlContext().getDdlStmt())) {
            return false;
        }

        return StringUtils.equalsIgnoreCase("true", useOriginalDDL);
    }

    public static boolean isUseFkOriginalDDL(ExecutionContext executionContext) {
        Map<String, Object> parameter = executionContext.getExtraCmds();
        String foreignKeysDdl = (String) parameter.get(ICdcManager.FOREIGN_KEYS_DDL);

        if (executionContext.getDdlContext() == null ||
            StringUtils.isEmpty(
                executionContext.getDdlContext().getDdlStmt())) {
            return false;
        }

        return StringUtils.equalsIgnoreCase("true", foreignKeysDdl);
    }

    public static String buildVersionIdHint(Long versionId) {
        return String.format("/*%s=%s*/", DDL_ID, versionId);
    }

    public static void useOriginalDDL(ExecutionContext executionContext) {
        executionContext.getExtraCmds().put(ICdcManager.USE_ORIGINAL_DDL, "true");
    }

    public static void useDdlVersionId(ExecutionContext executionContext, Long versionId) {
        if (null != versionId) {
            executionContext.getExtraCmds().put(ICdcManager.USE_DDL_VERSION_ID, versionId);
        }
    }

    public static boolean isVersionIdInitialized(Long versionId) {
        return !Objects.equals(versionId, DEFAULT_DDL_VERSION_ID);
    }
}

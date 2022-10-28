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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Map;

/**
 * created by ziyang.lb
 **/
public class CdcMarkUtil {

    public static Map<String, Object> buildExtendParameter(ExecutionContext executionContext) {
        Map<String, Object> parameter = executionContext.getExtraCmds();
        final Map<String, Object> extraVariables = executionContext.getExtraServerVariables();
        if (null != extraVariables && extraVariables.containsKey("polardbx_server_id")) {
            Object serverId = extraVariables.get("polardbx_server_id");
            parameter.put("polardbx_server_id", serverId);
        }
        return parameter;
    }
}

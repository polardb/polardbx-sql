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

import com.alibaba.polardbx.common.utils.HttpClientHelper;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yudong
 * @since 2023/6/19 11:40
 **/
@Slf4j
public class CdcVersionUtil {
    public static String getVersion() {
        String result = null;
        try {
            String daemonEndpoint = CdcTargetUtil.getDaemonMasterTarget();
            result = HttpClientHelper.doGet("http://" + daemonEndpoint + "/system/getVersion");
        } catch (Exception e) {
            log.error("get cdc version error", e);
        }
        return result;
    }
}

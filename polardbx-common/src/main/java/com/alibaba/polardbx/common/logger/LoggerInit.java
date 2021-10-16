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

package com.alibaba.polardbx.common.logger;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.Version;

public class LoggerInit {

    public static final Logger TDDL_DYNAMIC_CONFIG = LoggerFactory.getLogger("TDDL_DYNAMIC_CONFIG");

    public static final Logger TDDL_SEQUENCE_LOG = LoggerFactory.getLogger("TDDL_SEQUENCE_LOG");

    static {
        initTddlLog();
    }

    private static void initTddlLog() {
        TDDL_DYNAMIC_CONFIG.info(Version.getBuildVersion());
    }
}
